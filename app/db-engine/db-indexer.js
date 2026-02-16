const cfgHlp = require( '../helper/config' )
const log   = require( '../helper/logger' ).log
const fs    = require( 'fs' )
const { mkdir, writeFile, readFile,  rmdir } = require( 'node:fs/promises' )
const dbFile  = require( './db-file' )

module.exports = {
  init, 
  updateIndex,
  writeIndexDef,
  addDocToIndex,
  updateDocIndex,
  deleteDoc,
  reIndex,
  expireDocs,
  delIndex,
  syncIndexCacheToFile,
  terminate
}

// ============================================================================

let cfg = {
  DATA_DIR: './db/'
}

let indexSyncInterval = null

function init( configParams ) {
  // log.info( 'persistence.init', configParams )
  cfgHlp.setConfig( cfg, configParams )
  indexSyncInterval = setInterval( syncIndexCacheToFile, 10000 )
}

async function terminate() {
  log.info( 'Indexer terminate...' )
  if ( indexSyncInterval ) {
    clearInterval( indexSyncInterval)
  }
}

//=============================================================================

async function addDocToIndex( jobId, dbName, collName, doc, index ) {
  try {
    log.debug( jobId, 'addDocToIndex', dbName, collName, doc._id )
    let idxVals = getIndexedProperties( doc, index )
    log.debug( jobId, 'addDocToIndex idxVals', idxVals )
    if ( idxVals.needStore ) {
      for ( let idxField in idxVals.key ) {
        await addIdxField( jobId, dbName, collName, index, idxField, doc )
      }
    }
  } catch ( exc ) {
    log.error( jobId, 'addDocToIndex',  dbName, collName, doc, index, exc )
  }
}



async function updateDocIndex( jobId, dbName, collName, changedIdxField, doc ) {
  try {
    log.info( jobId, 'updateDocIndex', dbName, collName, changedIdxField, doc._id )
    let index = await loadIndexDef( jobId, dbName, collName )
    for ( let idxField of changedIdxField ) {
      // step 1: remove old index id
      let idx = await readIndexFile( jobId, dbName, collName, idxField, doc._token )
      removeDocIdFromIdx( jobId, idx, doc._id )
      await writeIndexFile( jobId, dbName, collName, idxField,  doc._token, idx )
      // step 2: re-add id to index
      await addIdxField( jobId, dbName, collName, index, idxField, doc )
    }
  } catch ( exc ) {
    log.error( jobId, 'updateDocIndex', dbName, collName, doc, exc )
  }
}


async function deleteDoc( jobId, dbName, collName, doc ) {
  try {
    log.info( jobId, 'deleteDoc', dbName, collName, doc._id )
    let collIdx = await loadIndexDef( jobId, dbName, collName )
    for ( let idxField in collIdx ) {
      let idx = await readIndexFile( jobId, dbName, collName, idxField, doc._token )
      let needWrite = removeDocIdFromIdx( jobId, idx, doc._id )
      if ( needWrite ) {
        await writeIndexFile( jobId, dbName, collName, idxField, doc._token, idx )
      }
    }
  } catch ( exc ) {
    log.error( jobId, 'deleteDoc', dbName, collName, doc, exc )
  }
}

function removeDocIdFromIdx( jobId, idx, rmDocId ) {
  log.debug( jobId, 'removeDocIdFromIdx', idx, rmDocId )
  let needWrite = false
  for ( let val in idx ) {
    let docArr = idx[ val ]
    log.debug( jobId, 'deleteDoc val', docArr.includes( rmDocId ) )
    if ( idx[ val ].includes( rmDocId )) {
      needWrite = true
      log.debug( jobId, 'removeDocIdFromIdx NEED CLEANUP', rmDocId )
      let newDocArr = []
      for ( let docId of docArr ) {
        if ( docId != rmDocId ) { newDocArr.push( docId ) }
      }
      if ( newDocArr.length == 0 ) {
        delete idx[ val ]
      } else {
        idx[ val ] = newDocArr
      }
    }
  }
  return needWrite
}


async function addIdxField( jobId, dbName, collName, index, idxField, doc ) {
  log.debug( jobId, 'addIdxField', dbName, collName, index, idxField, doc)
  let val = getVal( doc, idxField )

  if ( index[ idxField ].expiresAt ) {
    log.debug( jobId, 'addDocToIndex expiresAt', idxField, val, typeof val  )
    try {
      if ( val != null && val != undefined ) {
        if ( typeof val == 'number' ) {
          await addToIdxFile( jobId, dbName, collName, idxField, doc._token, doc._id, getTimeHrStr( val ) )
        } else if  ( val instanceof Date ) {
          await addToIdxFile( jobId, dbName, collName, idxField, doc._token, doc._id, getTimeHrStr( val.getTime() ) )
        } else if  ( typeof val == 'string'  ) {
          let dt = new Date( val )
          await addToIdxFile( jobId, dbName, collName, idxField, doc._token, doc._id, getTimeHrStr( dt.getTime() ) )
        } else {
          log.warn( jobId, 'addDocToIndex expiresAt unexpected value',  doc._id, idxField, val )
        }
      }
    } catch ( exc ) { log.warn( jobId, 'addDocToIndex expiresAfter',  doc._id, idxField, val, exc.message ) }

  } else if ( index[ idxField ].expiresAfterSeconds ) {
    log.debug( jobId, 'addDocToIndex expiresAfterSeconds', idxField, val )
    if ( val != null && val != undefined ) {
      if ( typeof val == 'number' ) {
        let expireDt = val + index[ idxField ].expiresAfterSeconds * 1000
        await addToIdxFile( jobId, dbName, collName, idxField, doc._token, doc._id, getTimeHrStr( expireDt ) )
      } else { 
        log.warn( jobId, 'addDocToIndex expiresAfterSeconds expected number',  doc._id, idxField, val )
      }
    }
  
  } else { // "simple" index field
    let valStr = getValAsIdxStr( doc, idxField, index[idxField] )
    await addToIdxFile( jobId, dbName, collName, idxField, doc._token, doc._id, valStr )
  }
}


async function addToIdxFile( jobId, dbName, collName, idxField, token, docId, valStr ) {
  log.debug( jobId, 'addToIdxFile', dbName, collName, idxField, token, docId, valStr  )
  let idx = await readIndexFile( jobId, dbName, collName, idxField, token )
  if ( idx[ valStr ] ) {
    if ( ! idx[ valStr ].includes( docId ) ) {
      idx[ valStr ].push( docId )
      await writeIndexFile( jobId, dbName, collName, idxField, token, idx )
    }
  } else {
    idx[ valStr ] = [ docId ]
    await writeIndexFile( jobId, dbName, collName, idxField, token, idx )
  }  
}


function getValAsIdxStr( doc, idxKey, indexDef ) {
  let val = getVal( doc, idxKey )

  let maxLen = 100
  if ( indexDef.msbLen && Number.isInteger( indexDef.msbLen ) ) {
    maxLen =indexDef.msbLen
  }

  if ( val == null ) {
    val = '_null_'
  } else {
    switch ( typeof val ) {
      case 'number':
        val =  ''+val
        break
      case 'string':
        val =  val.substring( 0, maxLen )
        break
      case 'object':
        val =  JSON.stringify( val ).substring( 0, maxLen )
        break
      case 'boolean':
        val =  '_'+val
        break
      case 'undefined':
        val =  '_undefined_'
        break
      case 'bigint':
        val =  val.toString(16)
        break
    }
  }
  log.debug( 'getValAsIdxStr', val )
  return val
}


function getIndexedProperties( doc, index  ) {
  log.debug(  'i', doc,  index )
  let idxVals = { needStore: false, key: {} }
  for ( let idxKey in index ) {
    let val = getVal( doc, idxKey )
    log.debug( 'ii', idxKey, val )
    if ( val ) {
      idxVals.needStore = true
      idxVals.key[ idxKey ] = val

      // if ( db[ dbName ].collection[ collection ].index[ idxKey ].expireAfterSeconds ) {
      //   // TODO expireAfterSeconds
      // }
    }
  }
  return idxVals
}

// ============================================================================

async function updateIndex( jobId, dbName, collName, updIdx ) {
  log.info( jobId, 'updateIndex', dbName, collName, updIdx )
  try {
    if ( ! updIdx ) {
      log.warn( jobId, 'updateIndex ignore', dbName, collName, updIdx )
      return  { _ok: false, _error: 'updIdx undefined' }
    }
    let oldIdx = await loadIndexDef( jobId, dbName, collName )
    // check for deleted indexes:
    for ( let idxField in oldIdx ) {
      if ( ! updIdx[ idxField ] ) {
        await dbFile.delCollIdx( dbName, collName, idxField ) 
      }
    }
    await writeIndexDef( jobId, dbName, collName, updIdx )
    return { _ok: true }
  } catch ( exc ) { 
    return errorMsg( 'updateIndex', exc )
  }
}


async function delIndex( jobId, dbName, collName, idxName ) {
  try {
    log.warn( jobId, 'delIndex', dbName, collName, idxName )
    let collIdx = await loadIndexDef( jobId, dbName, collName )
    delete  collIdx[ idxName ]
    await writeIndexDef( jobId, dbName, collName, collIdx )

    let idxDir = dbFile.collPath( dbName, collName ) + '/idx/' + idxName
    log.info( jobId, 'delIndex', idxDir )
    if ( ! await dbFile.dirExists( idxDir ) ) {
      await rmdir( idxDir )
    }
  
    return { _ok: true }
  } catch ( exc ) { return errorMsg( 'delIndex', exc ) }
}

// ============================================================================

async function reIndex( jobId, dbName, collName, idx ) {
  log.info( jobId, 'reIndex', dbName, collName )
  let colDir = dbFile.collPath( dbName, collName )
  let docDir = colDir +'doc/'

  let filenames = await dbFile.getJsonRecursive( docDir )

  let indexFile = {} // build in memory first
  let maxLen = {}
  for ( let idxField in idx ) {
    let idxSpec = idx[ idxField ]
    if ( idxSpec?.msbLen && Number.isInteger( idxSpec.msbLen ) ) {
      maxLen[ idxField ] = idxSpec.msbLen
    } else {
      maxLen[ idxField ] = 100
    }
    let idxDir = colDir +'idx/' + idxField
    await dbFile.ensureDirExists( idxDir )
  }

  for ( let docFile of filenames ) try {
    if ( docFile.endsWith( 'idx.json' ) ) { continue }
    const data = await readFile( docFile )
    let doc = JSON.parse( data )
    if ( ! doc._token ) { continue }

    for ( let idxField in idx ) {
      let idxFileName =  idxField +'/'+ idxField +'_'+ doc._token +'.json'
      if ( ! indexFile[ idxFileName ] ) { indexFile[ idxFileName ] = {} }

      let val = getVal( doc, idxField )
      // log.info( jobId, 'reIndex field', idxField, val  )

      if ( idx[ idxField ].expiresAt ) {
        // log.info( jobId, 'reIndex expiresAt', idxField, val, typeof val  )
        try {
          if ( val != null && val != undefined ) {
            if ( typeof val == 'number' ) {
              // log.info( jobId, 'expiresAt', getTimeHrStr( val ), doc._id  )
              addDocId( indexFile[ idxFileName ], getTimeHrStr( val ), doc._id )
            } else if  ( val instanceof Date ) {
              addDocId( indexFile[ idxFileName ], getTimeHrStr( val.getTime() ), doc._id )
            } else if  ( typeof val == 'string'  ) {
              let dt = new Date( val )
              addDocId( indexFile[ idxFileName ], getTimeHrStr( dt.getTime() ), doc._id )
            } else {
              log.warn( jobId, 'reIndex expiresAfter unexpected value',  doc._id, idxField, val )
            }
          }
        } catch ( exc ) { log.warn( jobId, 'reIndex expiresAfter',  doc._id, idxField, val, exc.message ) }

      } else if ( idx[ idxField ].expiresAfterSeconds ) {
        // log.info( jobId, 'reIndex expiresAfterSeconds', idxField, val  )
        if ( val != null && val != undefined ) {
          if ( typeof val == 'number' ) {
            let expireDt = val +  idx[ idxField ].expiresAfterSeconds * 1000
            log.debug( jobId, 'expiresAfterSeconds',  getTimeHrStr( expireDt ), doc._id  )
            addDocId( indexFile[ idxFileName ], getTimeHrStr( expireDt ), doc._id )
          } else {
            log.warn( jobId, 'reIndex expiresAfterSeconds expected number',  doc._id, idxField, val )
          }
        } 
      } else if ( val == null ) {
        addDocId(  indexFile[ idxFileName ], '_null_', doc._id )
      } else {
        switch ( typeof val ) {
          case 'number':
            addDocId(  indexFile[ idxFileName ], ''+val, doc._id )
            break
          case 'string':
            addDocId(  indexFile[ idxFileName ], val.substring( 0, maxLen[ idxField ] ), doc._id )
            break
          case 'object':
            addDocId(  indexFile[ idxFileName ], JSON.stringify( val ).substring( 0, maxLen[ idxField ]) , doc._id )
            break
          case 'boolean':
            addDocId(  indexFile[ idxFileName ], '_'+val, doc._id )
            break
          case 'undefined':
            addDocId(  indexFile[ idxFileName ], '_undefined_', doc._id )
            break
          case 'bigint':
            addDocId(  indexFile[ idxFileName ], val.toString(16), doc._id )
            break
        }
      }
    }
  } catch ( exc ) { log.warn( jobId, 'EXC reIndex', dbName, collName, idx, exc  ) }

  for ( let indexFileName in indexFile ) try {
    let idxFile = colDir +'idx/'+ indexFileName
    writeIndexFileNm( idxFile,  indexFile[ indexFileName ] )
    // log.debug( jobId, 'reIndex write file:', idxFile )
    // await writeFile( idxFile, JSON.stringify( indexFile[indexFileName], null, ' ' ) )
  } catch ( exc ) { log.warn( jobId, 'EXC reIndex write', dbName, collName, indexFile, exc  ) }

  return { _ok: true, result: 'Re-indexed '+filenames.length+' documents.' }
}

// ----------------------------------------------------------------------------

function getTimeHrStr( time ) {
  let timeHr = Math.round( time / 100000 ) * 100000
  return '' + timeHr
}


function addDocId( indexFile, key, id ) {
  if ( ! indexFile[ key ] ) { 
    indexFile[ key ] = [ id ] // create array for doc IDs
  } else { 
    indexFile[ key ].push( id )
  }
}


function getVal( doc, field ) {
  log.debug( '>>>>>>>>>>>>>  getVal', doc, field )
  let split = field.indexOf('.')
  if ( split > 0 ) {
    let subField = field.substring( 0, split )
    if ( doc.hasOwnProperty( subField ) ) {
      return doc[ subField ]
    } else {
      let nextField = field.substring( split + 1 )
      return getVal( doc[ subField ], nextField )
    }
  } else {
    return doc[ field ]
  }
}


// ============================================================================
 
async function expireDocs( jobId, dbName, collName, idxField ) {
  let expiredDocs = []
  let now = Date.now()
  try {
    let idxFolderName = dbFile.collPath( dbName, collName ) +'idx/'+ idxField 
    log.debug( jobId, 'expireDocs', idxFolderName )
    let xArr = fs.readdirSync( idxFolderName, { withFileTypes: true } )
    for ( let x of xArr ) {
      if ( ! x.isDirectory() ) {
        const data = await readFile( idxFolderName +'/'+ x.name )
        let writeRequired = false
        let indexedDocs = JSON.parse( data )
        for ( let expireStr in indexedDocs ) try {
          if ( Number.parseInt( expireStr ) < now ) {
            log.info(  jobId, '..... Expire', expireStr, now, indexedDocs[ expireStr ] )
            for ( let docId of indexedDocs[ expireStr ] ) {
              expiredDocs.push( docId )
            }
            delete indexedDocs[ expireStr ]
            writeRequired = true
          }
        } catch ( exc ) { log.error( 'expireDocs parse', jobId, dbName, collName, idxField, expireStr, exc ) }
        if ( writeRequired ) {
          log.debug( jobId ,'expireDocs update index', idxFolderName +'/'+ x.name )
          await writeFile( idxFolderName +'/'+ x.name, JSON.stringify( indexedDocs, null, ' ' ) )
        }
      }
    }
  } catch ( exc ) { log.error( 'expireDocs',jobId, dbName, collName, idxField, exc )}
  return expiredDocs
}

// ============================================================================
// helper

let indexDefs = {}

async function loadIndexDef( jobId, dbName, collName ) {
  let idxDef = {}
  try {
    let collIdxFile = dbFile.collPath( dbName, collName ) + '/idx/idx.json'
    if ( indexDefs[ collIdxFile ] ) {
      log.debug( jobId, 'loadIndexDef from cache', collIdxFile )
      return  indexDefs[ collIdxFile ]
    }
    log.info( jobId, 'loadIndexDef', collIdxFile )
    if ( await dbFile.fileExists( collIdxFile ) ) {
      idxDef = JSON.parse( await readFile( collIdxFile ) )
      indexDefs[ collIdxFile ] = idxDef
    }
  } catch ( exc ) { log.error( jobId, 'loadIndexDef', dbName, collName, exc ) }
  return idxDef
}

async function writeIndexDef( jobId, dbName, collName, collIdx ) {
  let collIdxFile = dbFile.collPath( dbName, collName ) + 'idx/idx.json'
  indexDefs[ collIdxFile ] = collIdx 
  log.info( jobId, 'writeIndexDef', collIdxFile )
  await writeFile( collIdxFile, JSON.stringify( collIdx, null, ' ' ) )
}

function errorMsg( fnName, exc ) {
  log.fatal( fnName, exc )
  return { _error : ''+exc.message }
}

// ----------------------------------------------------------------------------

const IDX_CACHE = {}

async function readIndexFile( jobId, dbName, collName, idxField, docToken ) {
  let idx = {}
  try {
    let idxFileName = dbFile.collPath( dbName, collName ) +'idx/'+ idxField +'/'+ idxField +'_'+ docToken +'.json'
    if ( IDX_CACHE[ idxFileName ] ) {
      return IDX_CACHE[ idxFileName ].idx
    }
    log.debug( jobId, 'readIndexFile', idxFileName )
    if ( await dbFile.fileExists( idxFileName ) ) {
      idx = JSON.parse( await readFile( idxFileName ) ) // TODO auto/repair
    }
    IDX_CACHE[ idxFileName ] = { idx: idx, needWrite: false }
  } catch ( exc ) { log.warn( jobId, 'readIndexFile', dbName, collName, idxField, docToken, exc.message ) }
  return idx
}


function writeIndexFileNm( idxFileName, idx ) {
  IDX_CACHE[ idxFileName ] = { idx: idx, needWrite: true }
}

async function writeIndexFile( jobId, dbName, collName, idxField, docToken, idx ) {
  try {
    let idxFileName = dbFile.collPath( dbName, collName ) +'idx/'+ idxField +'/'+ idxField +'_'+ docToken +'.json'
    IDX_CACHE[ idxFileName ] = { idx: idx, needWrite: true }
  } catch ( exc ) { log.warn( jobId, 'writeIndexFile', exc.message ) }

  // try {
  //   await dbFile.ensureDirExists( dbFile.collPath( dbName, collName ) +'idx/'+ idxField )
  //   let idxFileName = dbFile.collPath( dbName, collName ) +'idx/'+ idxField +'/'+ idxField +'_'+ docToken +'.json'
  //   // log.info( jobId, 'writeIndexFile', idxFileName )    
  //   await writeFile( idxFileName, JSON.stringify(idx, null, ' ' ) )
  // } catch ( exc ) { log.warn( jobId, 'writeIndexFile', exc.message ) }
}

async function syncIndexCacheToFile() {
  for ( let idxFileName in IDX_CACHE ) {
    if ( IDX_CACHE[ idxFileName ].needWrite ) {
      try {
        log.debug( 'syncIndexCacheToFile', idxFileName )    
        let idxPath = idxFileName.substring( 0, idxFileName.lastIndexOf('/') )
        await dbFile.ensureDirExists( idxPath )
        await writeFile( idxFileName, JSON.stringify( IDX_CACHE[ idxFileName ].idx, null, ' ' ) )
        IDX_CACHE[ idxFileName ].needWrite = false
      } catch ( exc ) { log.warn( 'syncIndexCacheToFile', exc.message ) }
    }
  }
  // TODO: watch index mem size
}