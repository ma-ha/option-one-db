const cfgHlp = require( '../helper/config' )
const log   = require( '../helper/logger' ).log
const fs    = require( 'fs' )
const { mkdir, writeFile, readFile, rename, rm, rmdir, stat, readdir, open, opendir, cp  } = require( 'node:fs/promises' )
// const path = require('path')

module.exports = {
  init, 
  collPath,
  loadDbTree,

  creDb,
  updDb,

  getDbVersion,
  updDbVersion,
  delDb,

  creColl,
  renameColl,
  delColl,
  delCollIdx,

  creDoc,
  replaceDocByIdPrep,
  replaceDocByIdCommit,
  replaceDocByIdRollback,

  loadDocById,

  getAllDocIds,
  loadAllDoc,
  getJsonRecursive,

  creBackup,
  restoreBackup,
  purgeBackup,

  deleteDocById,
  fileExists,
  dirExists,
  ensureDirExists,
  checkValidName

}

// ============================================================================

let cfg = {
  DATA_DIR: './db/',
  BACKUP_DIR: './backup/'
}

let ownNodeName = null
let dtaDir      = null
let backupDir   = null

// let dirSched  = null

async function init( nodeName, configParams ) {
  cfgHlp.setConfig( cfg, configParams )
  //log.info( 'dbfile.init', cfg)
  try { 
    ownNodeName = nodeName

    dtaDir = cfg.DATA_DIR

    // dirSched = setInterval( printDir, 200 )
    // setTimeout( () => { clearInterval( dirSched ) }, 20000 )

    if ( ! dtaDir.endsWith('/') ) { dtaDir += '/'  }
    dtaDir += ownNodeName.replaceAll(':','').replaceAll('/','') + '/'
    await ensureDirExists( dtaDir )

    if ( ! cfg.BACKUP_DIR.endsWith('/') ) { cfg.BACKUP_DIR += '/'  }
    backupDir = cfg.BACKUP_DIR + ownNodeName.replaceAll(':','').replaceAll('/','') + '/'
    await ensureDirExists( backupDir )

    // let s = await du( dtaDir )
    // log.info( '>>>>>>>>>>>>>>>>>>>>>>>>>>>',  Math.round( s / ( 1024 * 1024 ) ) + ' MB' )
  } catch ( exc ) {
    log.fatal( 'init', exc.message )
    throw Error( exc.message )
  }
}

async function printDir() {
  let dbDirArr = fs.readdirSync( dtaDir, { withFileTypes: true } )
  log.info( 'DIR', JSON.stringify( dbDirArr ) )
}

function collPath( dbName, collName ) {
  return dtaDir + dbName + '/' + collName+ '/'
}

// ============================================================================
// re-init from data dir

async function loadDbTree( ) {
  try { 
    log.debug( 'loadDbTree...' )
    let result = {}
    let dbDirArr = fs.readdirSync( dtaDir, { withFileTypes: true } )
    for ( let x of dbDirArr ) {
      if ( x.isDirectory() ) {
        result[ x.name ] = await loadDatabase( x.name )
      }
    }
    return result
  } catch ( exc ) { return errorMsg( 'loadDbTree', exc ) }
}

async function loadDatabase( dbName ) {
  try { 
    log.debug( 'loadDatabase', dbName )
    checkValidName( dbName )
    const dbMetaStr = await readFile(  dtaDir + dbName + '/db.json' )
    let db = JSON.parse( dbMetaStr )
    let aDB = { 
      creDate    : db.creDate,
      version    : ( db.version ? db.version : 0 ),
      collection : await loadCollectionMap( dbName ),
      collectionDeleted : await loadCollDeletedMeta( dbName ) 
    }
    return aDB
  } catch ( exc ) { return errorMsg( 'loadDatabase', exc ) }
}

async function loadCollectionMap( dbName ) {
  try {
    checkValidName( dbName )
    log.debug( 'loadCollections', dbName )
    let collMap = {}
    let dbDirArr = fs.readdirSync( dtaDir + dbName , { withFileTypes: true } )
    for ( let x of dbDirArr ) {
      if ( x.isDirectory() ) {
        collMap[ x.name ] =  await loadCollection( dbName, x.name )
      }
    }
    return collMap
  } catch ( exc ) { return errorMsg( 'loadCollectionMap', exc ) }
}

async function loadCollection( dbName , collName ) {
  try {
    checkValidName( dbName, collName )
    log.debug( 'loadCollection', dbName, collName )
    const collMetaStr = await readFile(  dtaDir + dbName +'/'+ collName+ '/collection.json' )
    const collMeta = JSON.parse( collMetaStr )
    let coll = {
      primaryKey   : collMeta.pk,
      masterData   : collMeta.masterData,
      index        : await loadCollIdxMap( dbName, collName ),
      indexDeleted : (collMeta.indexDeleted ? collMeta.indexDeleted : {}),// await loadCollIdxDeleted( dbName, collName ),
      creDate      : collMeta.creDate,
      cacheMax     : collMeta.cacheMax,
      doc          : {}
    }
    return coll
  } catch ( exc ) { 
    // TODO request write collection
//     [2026-02-11T16:45:36.490Z]  INFO: option-one-db/1 on option-one-db-2: << OK <<< SyncNodes <<< (option-one-db-1:9000/option-one-db) option-one-db-1:9000/option-one-db OK  option-one-db-2:9000/option-one-db OK  option-one-db-0:9000/option-one-db OK  {"0":"option-one-db-0:9000/option-one-db","1":"option-one-db-2:9000/option-one-db","2":"option-one-db-1:9000/option-one-db"}
// [2026-02-11T16:45:39.502Z] FATAL: option-one-db/1 on option-one-db-2: loadCollection ENOENT: no such file or directory, open '/db/option-one-db-29000option-one-db/admin/log/collection.json'

    return errorMsg( 'loadCollection', exc ) 
  }
}

async function loadCollIdxMap( dbName, collName ) {
  let idx = {}
  log.debug( 'loadCollIdxMap', dbName, collName )
  const idxFile = dtaDir + dbName  +'/'+ collName +'/idx/idx.json'
  try {
    const idxMetaStr = await readFile( idxFile )
    idx = JSON.parse( idxMetaStr )
  } catch ( exc ) { 
    log.warn( 'loadCollIdxMap', exc.message ) 
    log.warn( 'loadCollIdxMap recreate empty index definition', idxFile ) 
    await writeFile( idxFile, '{}' )
  }
  return idx
}

// async function loadCollIdxDeleted( dbName, collName ) {
//   try {
//     let delFile = dtaDir + dbName  +'/'+ collName +'/idx/idx-del.json'
//     log.debug( 'loadCollIdxDeleted', delFile )
//     log.debug( 'loadCollIdxDeleted', await fileExists( delFile ) )
//     if ( await fileExists( delFile ) ) {
//       const meta = JSON.parse( await readFile( delFile ) )
//       return meta
//     } 
//   } catch ( exc ) { log.fatal( 'loadCollIdxDeleted', exc.message ) }
//   return {}
// }

async function loadCollDeletedMeta( dbName ) {
  try {
    checkValidName( dbName )
    let delFile = dtaDir + dbName  + '/collection-del.json'
    log.debug( 'loadCollDeletedMeta', delFile )
    // log.debug( 'loadCollDeletedMeta', await fileExists( delFile ) )
    if ( await fileExists( delFile ) ) {
      const meta = JSON.parse( await readFile( delFile ) )
      log.info( 'loadCollDeletedMeta', meta )
      return meta
    } 
  } catch ( exc ) { log.fatal( 'loadCollDeletedMeta', exc.message ) }
  return {}
}
// ============================================================================

async function creDb( dbName ) {
  try {
    checkValidName( dbName )
    let dbDir = dtaDir + dbName
    log.info( 'creDb', dbDir )
    if ( ! await dirExists( dbDir ) ) {
      await mkdir( dbDir )
      let meta = { 
        name: dbName, 
        creDate: Date.now(),
        updDate: Date.now(),
        version: 1
      }
      await writeFile( dbDir + '/db.json', JSON.stringify( meta, null, ' ' ) )
    } 
    return { _ok: true }
  } catch ( exc ) {
    log.fatal( 'creDb', exc.message )
    return { _error: exc.message }
  }
} 

async function getDbVersion( dbName ) {
  try {
    checkValidName( dbName )
    let dbDir = dtaDir + dbName
    const dbMetaStr = await readFile(  dtaDir + dbName + '/db.json' )
    let db = JSON.parse( dbMetaStr )
    return { 
      _ok: true, 
      version: ( db.version ? db.version : 0 ) 
    }
  } catch ( exc ) {
    log.fatal( 'creDb', exc.message )
    return { _error: exc.message }
  }
} 

async function updDbVersion( dbName, version ) {
  try {
    checkValidName( dbName )
    let dbDir = dtaDir + dbName
    const dbMetaStr = await readFile(  dtaDir + dbName + '/db.json' )
    let db = JSON.parse( dbMetaStr )
    let meta = { 
      name: dbName, 
      creDate: db.creDate,
      updDate: Date.now(),
      version: version
    }
    await writeFile( dbDir + '/db.json', JSON.stringify( meta, null, ' ' ) )
    return { _ok: true }
  } catch ( exc ) {
    log.fatal( 'creDb', exc.message )
    return { _error: exc.message }
  }
} 

async function updDb( dbName, details ) {
  try {
    checkValidName( dbName )
    let dbDir = dtaDir + dbName
    await writeFile( dbDir + '/db.json', JSON.stringify( details, null, ' ' ) )
    return { _ok: true }
  } catch ( exc ) {
    log.fatal( 'creDb', exc.message )
    return { _error: exc.message }
  }
}

async function delDb( dbName ) {
  try {
    checkValidName( dbName )
    let dbDir = dtaDir + dbName 
    log.info( 'delDb', dbName, 'rmdir', dbDir )
    await rm( dbDir, { recursive : true } )
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'delDb', exc ) }
} 


async function creColl(  dbName, collName, collSpec ) {
  try {
    checkValidName( dbName, collName )
    let colDir = dtaDir + dbName + '/' + collName
    log.debug( 'creCol', colDir )
    if ( ! await dirExists( colDir ) ) {
      await mkdir( colDir )
      await mkdir( colDir +'/doc' )
      await mkdir( colDir +'/idx' )
      let meta = { 
        db         : dbName, 
        name       : collName,
        pk         : collSpec.primaryKey,
        noPK       : collSpec.noPK,
        masterData : collSpec.masterData,
        creDate    : Date.now(),
        cacheMax   : ( collSpec.cacheMax ? collSpec.cacheMax : 100 )
      }
      await writeFile( colDir + '/collection.json', JSON.stringify( meta, null, ' ' ) )
    }
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'creColl', exc ) }
}

async function renameColl( jobId, dbName, oldCollName, newCollName ) {
  try {
    log.info( jobId, 'renameColl starting...' )
    let oldDir = dtaDir + dbName + '/' + oldCollName + '/'
    let newDir = dtaDir + dbName + '/' + newCollName + '/'
    await ensureDirExists( newDir ) 
    log.info( jobId, 'renameColl read', oldDir, 'collection.json')
    const collDef = JSON.parse( await readFile( oldDir + 'collection.json' ) )
    collDef.name = newCollName
    log.info( jobId, 'renameColl copy', oldDir, newDir )
    await cp( oldDir, newDir, { recursive: true } )
    log.info( jobId, 'renameColl write', newDir, 'collection.json' )
    await writeFile( newDir + 'collection.json', JSON.stringify( collDef, null, '  ' ) )
    log.info( jobId, 'renameColl rm', newDir )
    await rm( oldDir, { recursive : true } )
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'delColl', exc ) }
}


async function delColl( dbName, collName ) {
  try {
    checkValidName( dbName, collName )
    let colDir = dtaDir + dbName + '/' + collName
    log.debug( 'delColl',  colDir )
    await rm( colDir, { recursive : true } )
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'delColl', exc ) }
}


async function delCollIdx( dbName, collName, idxField ) {
  try {
    checkValidName( dbName, collName )
    let idxDir = dtaDir + dbName + '/' + collName  + '/idx/' + idxField
    log.debug( 'delCollIdx',  idxDir )
    await rm( idxDir, { recursive : true } )
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'delCollIdx', exc ) }
}


//=============================================================================

async function creDoc( dbName, collName, doc, options ) {
  try {
    checkValidName( dbName, collName, doc._id )
    let id = doc._id
    let folder = await dir( dbName, collName, id )
    if ( collName != 'audit-log' && collName != 'log' ) 
      log.info( doc._txnId, 'creDoc', folder, doc._id )
    await writeFile( folder +'/'+ id +'.json', JSON.stringify( doc, null, ' ' ) )
    return { _ok: true, _id: doc._id }
  } catch ( exc ) { return errorMsg( 'creDoc', exc ) }
}


//=============================================================================

async function replaceDocByIdPrep( dbName, collName, replacement, options ) {
  try {    
    let id = replacement._id
    checkValidName( dbName, collName, id )
    let docFile = ( await dir( dbName, collName, id )) +'/'+ id 
    // step 1: checkfor a parallel transaction 
    if ( await fileExists( docFile +'_.json' ) ) { 
      let s = await stat( docFile +'_.json'  )
      if ( s.mtimeMs  &&  Date.now() - s.mtimeMs > 5000 ) { // old failed transaction
        log.warn( 'replaceDocByIdPrep', 'rm old transaction relict', docFile, s )
        await rm( docFile +'_.json' )
      } else {
        log.warn( 'replaceDocByIdPrep transaction incomplete', dbName, collName, id )
        return { _error: 'transaction failed' }  
      }
    }
    // step 2: if exist: rename existing
    if ( await fileExists( docFile +'.json' ) ) { 
      await rename( docFile +'.json', docFile +'_.json')
    }
    // step 3: write new file
    await writeFile( docFile +'.json', JSON.stringify( replacement, null, ' ' ) )

    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'replaceDocByIdPrep', exc ) }
}

async function replaceDocByIdCommit( dbName, collName, docId, options ) {
  try {    
    checkValidName( dbName, collName, docId )
    let docFile = ( await dir( dbName, collName, docId )) +'/'+ docId 
    // step 1: checkfor a parallel transaction 
    if ( await fileExists( docFile +'_.json' ) ) { 
      await rm( docFile +'_.json' )
    } else {
      log.warn( 'replaceDocByIdCommit', 'transaction already gone', docFile )
    }
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'replaceDocByIdCommit', exc ) }
}

async function replaceDocByIdRollback( dbName, collName, docId, options ) {
  try {    
    checkValidName( dbName, collName, docId )
    let docFile = ( await dir( dbName, collName, docId )) +'/'+ docId 
    if ( await fileExists( docFile +'_.json' ) ) { // there was a transaction prepared
      if ( await fileExists( docFile +'.json' ) ) { // a replacement is already there
        await rm( docFile +'.json' ) // remove replacement
      }
      await rename( docFile +'_.json', docFile +'.json') // recover original doc
    }
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'replaceDocByIdRollback', exc ) }
}

//=============================================================================

async function deleteDocById( dbName, collName, docId, options ) {
  try {    
    checkValidName( dbName, collName, docId )
    // let docFile = dtaDir + dbName +'/'+ collName +'/doc/'+ docId[0] +'/'+ docId[1]  +'/'+ docId 
    let docFile = ( await dir( dbName, collName, docId ))  +'/'+ docId 
    // let docFile = ( await dir( dbName, collName, docId )) +'/'+ docId 
    if ( await fileExists( docFile +'_.json' ) ) { // there was a transaction prepared
      await rm( docFile +'_.json' ) // TODO perhaps be more polite ?
    }
    await rm( docFile +'.json' )
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'deleteDocById', exc ) }
}

//=============================================================================

// TODO fix  FATAL: loadDocById admin/db-metrics/[object Object] docId is not alphanumeric: [object Object]
// loadDocById  admin/db-metrics/[object Object] Error: docId is not alphanumeric: [object Object]
// at checkValidName (/option-one/app/db-engine/db-file.js:739:15)
// at Object.loadDocById (/option-one/app/db-engine/db-file.js:454:5)
// at Object.getDocById (/option-one/app/db-engine/db-persistence.js:314:29)
// at updateOneDoc (/option-one/app/db-engine/db.js:172:35)
// at Timeout.persistMetrics [as _onTimeout] (/option-one/app/db-engine/db.js:688:13)

async function loadDocById( dbName, collName, docId, options ) {
  try {
    log.debug( 'loadDocById', dbName, collName, docId )
    checkValidName(  dbName, collName, docId )
    // let docFile = dtaDir + dbName +'/'+ collName +'/doc/'+ id[0] +'/'+ id[1] +'/'+ docId 
    let docFile = ( await dir( dbName, collName, docId ))  +'/'+ docId 
    let data = null

    if ( await fileExists( docFile +'.json' ) ) {
      data = await readFile(  docFile +'.json' )
    } else if ( await fileExists( docFile +'_.json' ) ) { // in middle of transaction ?
      data = await readFile(  docFile +'_.json' )
    }
    
    if ( data ) {
      let docJson = JSON.parse( data )
      // if ( docJson._txnId ) { 
      //   delete docJson._txnId 
      // }
      log.debug( 'loadDocById', dbName, collName, docJson )
      return {
        doc  : docJson,
        node : ownNodeName,
        _ok  : true
      }
    } else {
      log.debug( 'loadDocById NOT FOUND', dbName, collName, docId )
      return { _error: 'Not found' }
    }
  } catch ( exc ) { 
    log.error( 'loadDocById ', dbName+'/'+ collName+'/'+docId, exc)
    return errorMsg( 'loadDocById '+dbName+'/'+ collName+'/'+docId, exc ) 
  }
}


async function getAllDocIds( txnId, dbName, collName, options = {} ) {
  try {
    log.debug( txnId, 'getAllDocIds', dbName, collName, options  )
    checkValidName( dbName, collName )
    let docDir = dtaDir + dbName +'/'+ collName +'/doc'
    // let docIds = readIdsRecursive( docDir, options )
    let docIds = await readIdsRecursiveAsync( docDir, options )
    // log.info( '>>???????>>>>>', docIds )
    if ( options.MAX_ID_SCAN  &&  typeof options.MAX_ID_SCAN === 'number' && docIds.length > options.MAX_ID_SCAN ) {
      docIds = docIds.slice( 0, options.MAX_ID_SCAN )
    } else if ( typeof options.start === 'number'  && typeof options.count === 'number') {
      docIds = docIds.slice( options.start, options.start + options.count  )
    }
    log.debug( txnId, 'getAllDocIds', dbName, collName, docIds  )
    return docIds
  } catch ( exc ) { return errorMsg( 'loadDocById', exc ) }
}

async function readIdsRecursiveAsync( path, options = {} ) {
  let docIdArr = []
  try {
    let xArr = await readdir( path, { withFileTypes: true } )
    for ( let x of xArr ) {
      if ( x.isDirectory() ) {
        if ( options.optimize == 'only master nodes'  &&  options.ownToken ) {
          // log.info( 'readIdsRecursive', path, x.name , options.ownToken[ x.name ]  )
          if ( ! options.ownToken[ x.name ] ) { continue } // other node should take this dir
        }
        // log.info( 'dddddd', path, x.name )
        docIdArr = docIdArr.concat( await readIdsRecursiveAsync( path +'/'+ x.name  ) ) // no options required for next level
      } else {
        if ( ! x.name.endsWith( 'idx.json' ) ) {
          // log.info( 'ffffff', x.name.substring( 0, 64)  )
          docIdArr.push( x.name.replace( '.json', '' ) ) // _id has length 32
        }
      }
    }
  } catch ( exc ) {
    log.fatal( 'readIdsRecursive', exc )
    throw Error( exc.message )
  }
  return docIdArr
}

// function readIdsRecursive( path, options = {} ) {
//   let docIdArr = []
//   try {
//     let xArr = fs.readdirSync( path, { withFileTypes: true } )
//     for ( let x of xArr ) {
//       if ( x.isDirectory() ) {
//         if ( options.optimize == 'only master nodes'  &&  options.ownToken ) {
//           // log.info( 'readIdsRecursive', path, x.name , options.ownToken[ x.name ]  )
//           if ( ! options.ownToken[ x.name ] ) { continue } // other node should take this dir
//         }
//         // log.info( 'dddddd', path, x.name )
//         docIdArr = docIdArr.concat( readIdsRecursive( path +'/'+ x.name  ) ) // no options required for next level
//       } else {
//         if ( ! x.name.endsWith( 'idx.json' ) ) {
//           // log.info( 'ffffff', x.name.substring( 0, 64)  )
//           docIdArr.push( x.name.replace( '.json', '' ) ) // _id has length 32
//         }
//       }
//     }
//     //  }
//   } catch ( exc ) {
//     log.fatal( 'readIdsRecursive', exc )
//     throw Error( exc.message )
//   }
//   return docIdArr
// }


async function loadAllDoc( dbName, collName, query, options ) {
  log.debug( 'loadAllDoc', dbName, collName, query, options  )
  try {
    checkValidName( dbName, collName )
    let docDir = dtaDir + dbName +'/'+ collName +'/doc/'
    let filenames = await getJsonRecursive( docDir )
    let result = {
      doc : []
    }
    log.debug( 'file/loadAllDoc', docDir, filenames )
    let cnt = 0
    for ( let docFile of filenames ) {
      if ( docFile.endsWith( 'idx.json' ) ) { continue }
      const data = await readFile( docFile )
      let docJson = JSON.parse( data )
      if ( matchesQuery( docJson, query ) ) {
        result.doc.push( docJson )
      }
      // if ( docJson._txnId ) { 
      //   delete docJson._txnId 
      // }
      cnt ++
      if ( options && options.limit && cnt > options.imit ) { continue }
    }
    result._ok = true
    log.debug( 'file/loadAllDoc cnt', result.doc.length )
    return result
  } catch ( exc ) {  return errorMsg( 'loadAllDoc', exc ) }
}

function matchesQuery( doc, query )  {
  // log.info( 'matchesQuery', doc, query  )
  try {
    if ( ! query || query == {} ) { return true }
    for ( let qKey in query ) {
      let qFld = query[ qKey ]

      if ( typeof  qFld === 'string' || typeof  qFld === 'number' || typeof  qFld === 'boolean' ) {

        // TODO sub docs
        let docVal = getSubDoc( doc, qKey )
        if (docVal && docVal == qFld ) {
          return true
        } else {
          return false
        }

      } else if ( typeof  qFld === 'object' ) {
        // TODO
        return false
      }

    }
  } catch ( exc ) { log.warn( 'matchesQuery'), exc  }
  return true
}


function getSubDoc( doc, key ) {
  while ( key.indexOf('.') > 0 ) {
    let firstFld = key.substring( 0,  key.indexOf('.') )
    if ( doc[ firstFld ] != undefined ) {
      return getSubDoc(  doc[ firstFld ], key.substring( key.indexOf('.') + 1) )
    } else { return null }
  } 
  return doc[ key ]
}

//=============================================================================
// File Backup

// Cannot overwrite non-directory with directory: 
// cp returned undefined (
//   cannot overwrite non-directory 
//   ./db/mh-UX305CA9000db/mocha-test-db/perf-test-noidx/collection.json 
//   with directory 
//   ./backup/mh-UX305CA9000db/20251227T0915/mocha-test-db/perf-test-noidx/
//   ) ./backup/mh-UX305CA9000db/20251227T0915/mocha-test-db/perf-test-noidx/ 
//   Error: Cannot overwrite non-directory with directory: 
//   cp returned undefined (cannot overwrite non-directory ./db/mh-UX305CA9000db/mocha-test-db/perf-test-noidx/collection.json with directory ./backup/mh-UX305CA9000db/20251227T0915/mocha-test-db/perf-test-noidx/) ./backup/mh-UX305CA9000db/20251227T0915/mocha-test-db/perf-test-noidx/

async function creBackup( dateStr, dbName, collName ) {
  try {
    let dir = backupDir + dateStr.replaceAll(':','').replaceAll('-','') +'/'+ dbName +'/'+ collName +'/'
    log.info( 'BACKUP starting to', dir )
    await ensureDirExists( dir ) 

    let collDir = dtaDir + dbName +'/'+ collName +'/'
    await cp( collDir, dir, { recursive: true } )

    let s = await du( dir )
    log.info( s )
    return {
      _ok    : true,
      size   : Math.round( s / 10240 ) / 100
    }
  } catch ( exc ) {
    log.fatal( 'creBackup', exc.message)
  }
  return {
    size   : '0 MB'
  }
}

async function restoreBackup( dateStr, dbName, collName, restoreIndex, deactivateExpire ) {
  try {
    log.info( 'restoreBackup ...', dbName, collName, dateStr, restoreIndex, deactivateExpire )
    let dtStr = dateStr.replaceAll(':','').replaceAll('-','')
    let bckDir = backupDir + dtStr +'/'+ dbName +'/'+ collName +'/'
    let collDir = dtaDir + dbName +'/'+ collName +'-'+ dtStr +'/'
    await ensureDirExists( collDir + 'doc/' ) 
    await ensureDirExists( collDir + 'idx/' ) 
    log.info( 'restoreBackup copy docs ...' )
    await cp( bckDir + 'doc/', collDir + 'doc/', { recursive: true } )
    const collDef = JSON.parse( await readFile( bckDir + 'collection.json' ) )
    collDef.name = collName +'-'+ dtStr
    await writeFile( collDir + 'collection.json', JSON.stringify( collDef, null, '  ' ) )

    if ( restoreIndex ) {
      log.info( 'restoreBackup restoreIndex ...' )

      const collIdx = JSON.parse( await readFile( bckDir + 'idx/idx.json' ) )
      let restoreIdx = {}
      for ( let idx in collIdx ) {
        if ( deactivateExpire && ( collIdx[ idx ].expiresAfterSeconds || collIdx[ idx ].expiresAt ) ){
          log.info( 'restoreBackup skip expire index', idx )           
        } else {
          log.info( 'restoreBackup restore index', idx )
          restoreIdx[ idx ] = collIdx[ idx ]
          await cp( bckDir +'idx/'+ idx, collDir +'idx/'+ idx, { recursive: true } )
        }
      }
      await writeFile( collDir + 'idx/idx.json', JSON.stringify( restoreIdx, null, '  ' ) )

    } else {
      await writeFile( collDir + 'idx/idx.json', '{}' )
    }


    log.info( 'RESTORE starting from', bckDir, '>', collDir )
    return { _ok : true }
  } catch ( exc ) {
    log.fatal( 'RESTORE', exc.message)
  }
  return {}
}

async function du( dir ) {
  let size = 0
  try {
    let xArr = fs.readdirSync( dir, { withFileTypes: true } )
    for ( let x of xArr ) {
      if ( x.isDirectory() ) {
        let fileInf = await stat( dir +'/'+ x.name )
        size += fileInf.size
        size += await du( dir +'/'+ x.name )
        // log.info( dir +'/'+ x.name , fileInf.size )
      } else {
        let fileInf = await stat( dir +'/'+ x.name )
        // log.info( dir +'/'+ x.name , fileInf.size )
        size += fileInf.size
      }
    }
  } catch ( exc ) { log.warn( exc.message ) }
  return size
}

async function purgeBackup( dateStr, dbName, collArr ) {
  log.info( 'purgeBackupDir ...', dateStr, dbName, collArr )
  let dtStr = dateStr.replaceAll(':','').replaceAll('-','')
  let bckDir = backupDir + dtStr +'/'+ dbName  
  log.info( 'purgeBackupDir ...', bckDir )
  await rm( bckDir, { recursive : true } )
  // TODO check empty folder
  log.info( 'purgeBackupDir deleted',bckDir )
}

//=============================================================================
const ALPHANUMERIC = /^[a-z0-9_-]+$/i; 

function checkValidName( dbName, collName, docId ) {
  if ( ! ALPHANUMERIC.test( dbName ) ) {
    throw Error( 'dbName is not alphanumeric: '+dbName )
  }
  if ( collName != null ) {
    if ( ! ALPHANUMERIC.test( collName ) ) {
      throw Error( 'collName is not alphanumeric: '+collName )
    }
    if ( docId != null ) {
      if ( ! ALPHANUMERIC.test( docId ) ) {
        throw Error( 'docId is not alphanumeric: '+docId )
      }
    }
  }
}

async function dir( dbName, collName, docId ) {
  try {
    let folder = dtaDir + dbName +'/'+ collName +'/doc/'+ docId[0] +'/'+ docId[1] 
    await ensureDirExists( folder )
    return folder      
  } catch ( exc ) {
    log.fatal( 'dir', dtaDir, dbName, collName, docId, exc.message)
    throw Error( exc.message )
  }
}

// function flatten(lists) {
//   return lists.reduce((a, b) => a.concat(b), []);
// }


function getJsonRecursive( path ) {
  let jsonFiles = []
  try {
    let xArr = fs.readdirSync( path, { withFileTypes: true } )
    for ( let x of xArr ) {
      if ( x.isDirectory() ) {
        jsonFiles = jsonFiles.concat( getJsonRecursive( path +'/'+ x.name ) )
      } else {
        jsonFiles.push( path +'/'+ x.name )
      }
    }
  } catch ( exc ) {
    log.fatal( 'getJsonRecursive', path, exc.message)
    // throw Error( exc.message )
  }
  return jsonFiles
}

// function getDirectoriesRecursive(srcpath) {
//   return [srcpath, ...flatten(getDirectories(srcpath).map(getDirectoriesRecursive))];
// }


function errorMsg( fnName, exc ) {
  log.fatal( fnName, exc.message )
  return { _error : ''+exc.message }
}


//=============================================================================
let dirsOK = []

async function dirExists( path ) {
  if ( dirsOK.includes( path ) ) {
    // log.warn( 'dirExists cache', path )
    return true
  }
  try {
    let dirHandle = await opendir( path )
    dirHandle.close()
    dirsOK.push( path )
  } catch (error) {
    // log.warn( '! dirExists', path )
    return false
  } 
  // log.warn( 'dirExists', path )
  return true
}


async function ensureDirExists( folder ) {
  try {
    if ( ! await dirExists( folder ) ) {
      await mkdir( folder, { recursive: true } ) 
    }
  } catch ( exc ) {
    log.fatal( 'ensureDirExists', exc )
  }
}

async function fileExists( filePath ) {
  try {
    const dir = await open( filePath )
    dir.close()
    // log.warn( 'fileExists', filePath )
    return true
  } catch (error) {
    // log.warn( '! fileExists', filePath )
    return false
  }
}
