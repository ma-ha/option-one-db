const log     = require( '../helper/logger' ).log
const cfgHlp  = require( '../helper/config' )
const dbFile  = require( './db-file' )
const cache   = require( './db-cache' )
const indexer = require( './db-indexer' )
const idxQry  = require( './db-index-qry' )

module.exports = {
  init,
  terminate,
  dbOk,
  getNodeName,

  creDB,
  creDBjob,
  getDB,
  updDB,
  listDBs,
  listAllDBs,
  getDbVersion,
  updDbVersion,
  delDB,
  delDbJob,
  
  creColl,
  getColl,
  getCollPK,
  getCollSpec,
  renameColl,
  delColl,
  
  updateIdx,
  reIdx,
  listCollIdx,
  geCollIdx,
  delCollIdx,

  getDbTree,
  updateDbTree,

  insertDocPrep,
  // insertDocCommit,
  // insertDocRollback,

  updateDocPrep,
  updateDocCommit,
  updateDocIndex,
  // updateDocRollback,

  deleteDoc,

  getDocById,

  getAllDocIds,
  findDocCandidates,
  getAllDoc,

  // getUser,
  // getUserById,
  listUserRights,
  // changeUserRights

  // newBackupSchedule,
  // cancelBackupSchedule,
  // runBackupOnNode,
  // restoreBackup
}
// ============================================================================
let nodeName = null

let docExpireInterval = null
let reloadDbTreeInterval = null


let cfg = {
  MAX_ID_SCAN: 10000,
  CHECK_EXPIRE_INTERVAL: 10*60*1000
}

let  db = {}           // cache of meta data and data

let  dbDeleted = {}    // need for syncs

async function init( ownNodeName, configParams ) {
  nodeName = ownNodeName
  cfgHlp.setConfig( cfg, configParams )
  await dbFile.init( nodeName, configParams )

  db = await dbFile.loadDbTree( )
  indexer.init( configParams )
  idxQry.init( configParams )

  // log.debug( 'DB', JSON.stringify( db, null, '  ' ) )
  docExpire()
  docExpireInterval = setInterval( docExpire, cfg.CHECK_EXPIRE_INTERVAL ) // check all 10 min
  reloadDbTreeInterval = setInterval( reloadDbTree, 60*1000 ) // every minute ... important for backup restores
}

let writeOpsOngoing = 0

async function terminate() {
  log.info( 'Terminate persistence engine...', writeOpsOngoing )
  try {
    clearInterval( docExpireInterval )
    clearInterval( reloadDbTreeInterval )
    while ( writeOpsOngoing > 0 ) {
      log.info( 'Terminate persistence engine...', writeOpsOngoing )
      sleep( 300 )
    }
    await indexer.terminate()
    await indexer.syncIndexCacheToFile()
  } catch ( exc ) { log.info( 'Terminate persistence engine', exc ) }
}

const sleep = ms => new Promise( r => setTimeout( r, ms ) )

async function reloadDbTree() {
  let dbUpdate = await dbFile.loadDbTree()
  db = dbUpdate
}

async function docExpire() {
  let jobId = 'EXP.' + Date.now()
  writeOpsOngoing ++
  for ( let dbName in db ) {
    for ( let collName in db[ dbName ].collection ) {
      let coll = db[ dbName ].collection[ collName ]
      for ( let idxField in coll.index ) {
        let idx = coll.index[ idxField ]
        if ( idx.expiresAfterSeconds ) {
          log.debug( jobId, 'Check expiresAfterSeconds', dbName, collName )
          let expireDocs = await indexer.expireDocs( jobId, dbName, collName, idxField )
          for ( let docId of expireDocs ) {
            log.info( jobId, '>>> Doc expired', dbName, collName, docId )
            await deleteDoc( jobId, dbName, collName, docId )
          }
        } else if ( idx.expiresAt ) {
          log.debug( jobId, 'Check expiresAt', dbName, collName )
          let expireDocs = await indexer.expireDocs( jobId, dbName, collName, idxField )
          for ( let docId of expireDocs ) {
            log.info( jobId, '>>>> Doc expired', dbName, collName, docId )
            await deleteDoc( jobId, dbName, collName, docId )
          }
        }
      }
    }
  }
  writeOpsOngoing --
}

function getNodeName() {
  return nodeName
}

async function dbOk( dbName, collection, user, role ) {
  if ( ! db[ dbName ] ) { return { _error: 'DB not found' } }
  if ( ! db[ dbName ][ collection ] ) { return { _error: 'Collection not found' } }

  // TODO implement authorization
  return true
}

async function getDoc( dbName, collection, id, options ) {
  log.debug( 'getDoc', dbName, collection, id )
  // from cache
  let docFrmCache = cache.getDocFrmCache( dbName, collection, id ) 
  if ( docFrmCache ) { 
    return {
      doc: docFrmCache,
      _ok: true
    }
  }
  // from storage
  let doc = await dbFile.loadDocById( dbName, collection, id, options )
  if ( doc._error ) { return { _ok: false } }
  // add to cache
  cache.addToCache( dbName, collection, id, doc.doc )
  return doc
}

// ============================================================================
async function insertDocPrep( txnId, dbName, collection, doc, options ) {
  log.debug( txnId, 'insertDocPrep', dbName, collection, doc._id)
  let docId = doc._id
  let tstDoc = await getDoc( dbName, collection, docId, options )
  if ( tstDoc._ok ) { 
    if ( options?.noReplace ) {
      log.debug( txnId,  'insertDocPrep EXISTS', dbName, collection, doc._id)
      return { _ok: false, _error: 'exists' }       
    }
    if ( doc._upd ) {
      log.debug( txnId,  'insertDocPrep EXISTS', dbName, collection, doc._id)
      return { _ok: false, _error: 'exists' } 
    } else if ( tstDoc._upd >= doc._upd ) {
      log.debug(txnId, 'insertDocPrep EXISTS+NEWER', dbName, collection, doc._id)
      return { _ok: false, _error: 'exists' } 
    } // else overwrite newer version // TODO OK?
  }
  writeOpsOngoing ++
  cache.addToCache( dbName, collection, docId, doc )
  let result = await dbFile.creDoc( dbName, collection, doc, options )

  if ( collHasIndex( dbName, collection ) ) {
    await indexer.addDocToIndex( txnId, dbName, collection, doc, db[ dbName ].collection[ collection].index )
  }
  writeOpsOngoing --
  return result
}


function collHasIndex( dbName, collection ) {
  try {
    if ( db[ dbName ]?.collection[ collection ]?.index ) {
      return Object.keys( db[ dbName ].collection[ collection ].index ).length > 0
    }
  } catch ( exc ) {
    log.error(  'collHasIndex', dbName, collection, exc.message )
  }
  return false
}

// function getVal( doc, field ) {
//   log.debug( 'PERSIST >>>>>>>>>>>>>  getVal', doc, field )
//   let split = field.indexOf('.')
//   if ( split > 0 ) {
//     let subField = field.substring( 0, split )
//     if ( doc.hasOwnProperty( subField ) ) {
//       return { _keyVal: doc[ subField ] }
//     } else {
//       let nextField = field.substring( split + 1 )
//       return getVal( doc[ subField ], nextField )
//     }
//   } else {
//     return { _keyVal: doc[ field ] }
//   }
// }

// async function insertDocCommit( dbName, collection, docId, options ) {
//   return { _ok: true }
// }

// async function insertDocRollback( dbName, collection, docId, options ) {
//   cache.rmFrmCache( dbName, collection, docId )
//   let result = await dbFile.deleteDocById( dbName, collection, docId, options )
//   return result
// }

// ============================================================================
async function updateDocPrep( txnId, dbName, collName, doc, options ) {
  log.debug( txnId, 'updateDocPrep', dbName, collName, doc._id)
  writeOpsOngoing ++
  // let tstDoc = await getDoc( dbName, collection, doc._id, options )
  // if ( ! tstDoc._ok ) { return { _ok: false, _error: 'not found'} }
  cache.addToCache( dbName, collName, doc._id, doc )
  let result = await dbFile.replaceDocByIdPrep( dbName, collName, doc, options )
  writeOpsOngoing --
  return result 
}

async function updateDocCommit( txnId, dbName, collName, docId, options ) {
  log.debug( txnId, 'updateDocCommit', dbName, collName, docId )
  writeOpsOngoing ++
  // if ( db[ dbName ].collection[ collection ].doc[ docId ] ) {
  //   delete db[ dbName ].collection[ collection ].doc[ docId ]._txnId // unset transaction marker
  // }
  await dbFile.replaceDocByIdCommit( dbName, collName, docId, options )
  writeOpsOngoing --
  return { _ok: true }
}

async function updateDocIndex( txnId, dbName, collName, changedIdxField, doc ) {
  writeOpsOngoing ++
  await indexer.updateDocIndex( txnId, dbName, collName, changedIdxField, doc )
  writeOpsOngoing --
}


// async function updateDocRollback( dbName, collection, docId, options ) {
//   log.debug( 'updateDocRollback', dbName, collection, docId )
//   cache.rmFrmCache( dbName, collection, docId )
//   await dbFile.replaceDocByIdRollback( dbName, collection, docId, options )
//   return { _ok: true }
// }

// ============================================================================

async function deleteDoc( jobId, dbName, collName, docId, options ) {
  log.debug( 'deleteDoc', dbName, collName, docId )
  writeOpsOngoing ++
  let result = await dbFile.loadDocById( dbName, collName, docId, options )
  cache.rmFrmCache( dbName, collName, docId )
  await dbFile.deleteDocById( dbName, collName, docId, options )
  await indexer.deleteDoc( jobId, dbName, collName, result.doc )
  writeOpsOngoing --
  return { _ok: true }
}

// ============================================================================

async function getDocById( dbName, collName, docId, options = {} ) {
  if ( ! db[ dbName ] ) { 
    return { _error: 'Not found: DB' } 
  }
  if ( ! db[ dbName ].collection[ collName ] ) { 
    return { _error: 'Not found: Collection' } 
  }
  let docFrmCache = cache.getDocFrmCache( dbName, collName, docId ) 
  if ( docFrmCache ) {
    return  { 
      doc  :docFrmCache,
      node : nodeName,
      _ok  : true
    }
  }

  let result = await dbFile.loadDocById( dbName, collName, docId, options )
  // todo: check if cache is too large
  if ( ! result._error ) {
    cache.addToCache( dbName, collName, docId, result.doc )
  }
  return result
}

async function getAllDocIds( txnId, dbName, collName, options = {} ) {
  if ( ! options.MAX_ID_SCAN ) { options.MAX_ID_SCAN = cfg.MAX_ID_SCAN }
  return await dbFile.getAllDocIds( txnId, dbName, collName, options  )
}

async function findDocCandidates( txnId, dbName, collName, query, idx ) {
  return await idxQry.findDocCandidates( txnId, dbName, collName, query, idx ) 
}

async function getAllDoc( dbName, collName, query, options = { limit: 100 } ) {
  if ( ! db[ dbName ] ) { return { _error: 'Not found: DB' } }
  if ( ! db[ dbName ].collection[ collName ] ) {  return { _error: 'Not found: Collection' } }
  let docArr = await dbFile.loadAllDoc( dbName, collName, query, options )
  return docArr
}

// ============================================================================

async function getDbTree() {
  let dbTree = {}
  for ( let dbName in db ) {
    dbTree[ dbName ] = {
      c  : {}, // collections
      cd : db[ dbName ].collectionDeleted, // deleted collections
      cre: db[ dbName ].creDate,
      ver: db[ dbName ].version
    }
    for ( let collName in db[ dbName ].collection  ) {
      dbTree[ dbName ].c[ collName ] = {
        pk : db[ dbName ].collection[ collName ].primaryKey,
        i  : db[ dbName ].collection[ collName ].index,
        id : db[ dbName ].collection[ collName ].indexDeleted,
        cre: db[ dbName ].collection[ collName ].creDate,
        masterData : db[ dbName ].collection[ collName ].masterData
      }
    }
  }
  for ( let dbName in dbDeleted ) {
    dbTree[ dbName ] = { _deleted: dbDeleted[ dbName ] }
  }
  log.debug( 'getDbTree', dbTree )
  return dbTree
}

// ============================================================================

async function updateDbTree( jobId, dbUpd ) {
  log.debug( jobId, 'updateDbTree in', dbUpd )
  writeOpsOngoing ++
  let updates = []
  // update DBs
  for ( let dbName in dbUpd ) {
    
    if ( dbUpd[ dbName ]._deleted ) { // dbName was deleted

      if ( db[ dbName ] ) { 
        // check conflicting delete and create by timestamp
        if ( db[ dbName ].creDate < dbUpd[ dbName ]._deleted ) {
          log.debug( jobId, 'PERSIST updateDbTree delete db cx', dbUpd )
          updates.push( delDB( dbName ) )
        }
      } 

    } else if ( ! db[ dbName ] ) { // we have to cerate a new DB

      if ( dbDeleted[ dbName ] ) {
        // check conflicting delete and create by timestamp
        if ( dbUpd[ dbName ].cre > dbDeleted[ dbName ] ) {
          log.debug( jobId, 'PERSIST updateDbTree cre db cx', dbUpd )
          updates.push( creDB( dbName, dbUpd[ dbName ] ) )    
        }
      } else {
        log.debug( jobId, 'PERSIST updateDbTree cre db', dbUpd )
        updates.push( creDB( dbName, dbUpd[ dbName ] ) )  
      }
      
    } else {

      for ( let collName in dbUpd[ dbName ].cd ) {
        if ( db[ dbName ].collection[ collName ] ) { 
          log.debug( jobId, 'CD ... ', collName,  db[ dbName ].collection[ collName ],  dbUpd[ dbName ].cd[ collName ] )
          if (  db[ dbName ].collection[ collName ].creDate <  dbUpd[ dbName ].cd[ collName ] ) { // compare dates
            log.debug( jobId, 'CD ... ', collName,  db[ dbName ].collection[ collName ],  dbUpd[ dbName ].cd[ collName ] )

            updates.push( delColl( jobId, dbName, collName ) )
          }
        }
      }
      for ( let collName in dbUpd[ dbName ].c ) {
        // log.info( 'PERSIST collName', collName )
        if ( db[ dbName ] && ! db[ dbName ].collection[ collName ] ) { 
          // log.info( 'PERSIST dbUpd[ dbName ].c', dbUpd[ dbName ].c )
          // log.info( 'PERSIST db[ dbName ].collectionDeleted[ collName ]', db[ dbName ].collectionDeleted[ collName ] )
          // log.info( 'PERSIST  dbUpd[ dbName ].c[ collName ]',  dbUpd[ dbName ].c[ collName ] )
          if ( ! db[ dbName ].collectionDeleted[ collName ] || db[ dbName ].collectionDeleted[ collName ] <  dbUpd[ dbName ].c[ collName ].cre ) { // compare dates
            log.debug( jobId, 'PERSIST dbUpd[ dbName ].c', dbUpd[ dbName ].c )
            log.debug( jobId, 'PERSIST dbDeleted[ dbName ]',  db[ dbName ].collectionDeleted[ collName ])
            updates.push( creColl( jobId, dbName, collName, dbUpd[ dbName ].c[ collName ] ) )  
          }

        } else { 

          updates.push( updateIdx( jobId, dbName, collName, dbUpd[ dbName ].c[ collName ].i ) )
          // for ( let idxlName in dbUpd[ dbName ].c[ collName ].i ) {
          //   if ( ! db[ dbName ].collection[ collName ].index[ idxlName ] ) {

          //     updates.push( creCollIdx( dbName, collName, idxlName, dbUpd[ dbName ].c[ collName ].i[ idxlName ] ) )

          //   }
          // }
          // for ( let idxlName in dbUpd[ dbName ].c[ collName ].id ) {
          //   if ( db[ dbName ].collection[ collName ].index[ idxlName ] ) {

          //     updates.push( delCollIdx( db, collName, idxlName ) )

          //   }
          // }
        }
      }
    }
  } 
  await Promise.allSettled( updates )

 // TODO check or repair collections
  // try {
  //   for ( let dbName in db ) {
  //     for ( let coll in db[ dbName ].collection ) {
  //       log.info( 'DB', dbName, coll, db[ dbName ].collection[ coll ] )
  //     }
  //   }
  // } catch ( exc ) { log.warn( 'updateDbTree check', exc )}

  let result = await getDbTree()
  log.debug( 'updateDbTree out', result )
  writeOpsOngoing --
  return result
}

// ============================================================================

async function creDB( dbName, newDB ) {
  return await creDBjob( '--', dbName, newDB ) 
}

async function creDBjob( jobId, dbName, newDB ) {
  log.info( jobId, 'PERSIST createDB', dbName )
  if ( db[ dbName ] ) { 
    log.warn( jobId, 'PERSIST createDB', 'DB exists', dbName )
    return { _ok : 'DB already exists' }
  }
  writeOpsOngoing ++
  db[ dbName ] = {
    collection        : {},
    collectionDeleted : {},
    creDate           : Date.now()
  }
  if ( dbDeleted[ dbName ] ) {
    delete dbDeleted[ dbName ] 
  }
  await dbFile.creDb( dbName )
  if ( newDB ) for ( let collName in newDB.c ) {
    creColl( jobId, dbName, collName,  newDB.c[ collName ] )
    // db[ dbName ].collection[ collName ] = {
    //   primaryKey   : newDB.c[ collName ].pk,
    //   index        : ( newDB.c[ collName ].i  ? newDB.c[ collName ].i  : {} ),
    //   indexDeleted : ( newDB.c[ collName ].id ? newDB.c[ collName ].id : {} ),
    //   doc          : {}
    // }
  }
  if ( dbDeleted[ dbName ] ) {
    delete dbDeleted[ dbName ]
  }
  writeOpsOngoing --
  return { _ok: true }
}


async function getDB( dbName ) {
  log.debug( 'getDB', dbName )
  if ( ! db[ dbName ] ) { return null }
  return db[ dbName ]
}

async function updDB( dbName, details ) {
  log.info( 'updDB', dbName, details )
  writeOpsOngoing ++
  let result = await dbFile.updDb( dbName, details )
  writeOpsOngoing --
  return result
}

async function getDbVersion( dbName ) {
  if ( ! db[ dbName ] ) { return { _error: 'DB not found' } }
  return  db[ dbName ].getDbVersion
}

async function updDbVersion( dbName, version ) {
  if ( ! db[ dbName ] ) { return { _error: 'DB not found' } }
  writeOpsOngoing ++
  let result = await dbFile.updDbVersion( dbName, version )
  if ( result._ok ) {
    db[ dbName ].version = version
  }
  writeOpsOngoing --
  return result
}


async function listDBs() {
  let dbArr = []
  for ( let dbName in db ) {
    if ( dbName != 'admin' ) {
      dbArr.push( dbName )
    }
  }
  return dbArr
}

async function listAllDBs() {
  let dbArr = []
  for ( let dbName in db ) {
    dbArr.push( dbName )
  }
  return dbArr
}


async function delDB( dbName ) {
  return await delDbJob( '--', dbName ) 
}

async function delDbJob( jobId, dbName ) {
  log.info( jobId,'PERSIST deleteDB', dbName )
  if ( ! db[ dbName ] ) { 
    log.warn( jobId, 'PERSIST deleteDB', 'DB does not exist', dbName )
    return { _error : 'DB does not exist' }
  }
  writeOpsOngoing ++
  dbDeleted[ dbName ] = Date.now()
  delete db[ dbName ]
  await dbFile.delDb( dbName )
  writeOpsOngoing --
  return { _ok: true }
}

// ============================================================================

async function creColl( jobId, dbName, collName, collOpts ={} ) {
  if ( collName != 'audit-log') log.info( jobId, 'PERSIST createCollection', dbName, collName )
  if ( ! db[ dbName ] ) { return { _error : 'DB not found ' }  }
  if ( db[ dbName ].collection[ collName ] ) {
    return { _ok : 'Collection already exists' }
  }
  if ( ! Array.isArray( collOpts.pk) ) {
    log.warn( jobId, 'createCollection', 'Primary key must be an Array')
    return { _error: 'Primary key must be an Array' }
  }
  writeOpsOngoing ++
  let idx =  ( collOpts.index ? collOpts.index : {} )
  db[ dbName ].collection[ collName ] = {
    primaryKey   : collOpts.pk,
    noPK         : ( collOpts.noPK === true ? true : false ),
    index        : idx,
    indexDeleted : ( collOpts.id ? collOpts.id : {} ),
    // doc          : {},
    creDate      : Date.now(), 
    masterData   : ( collOpts.masterData ? true : false )
  }
  if ( db[ dbName ].collectionDeleted[ collName ] ) {
    delete db[ dbName ].collectionDeleted[ collName ]
  }
  await dbFile.creColl( dbName, collName, db[ dbName ].collection[ collName ] )
  await indexer.writeIndexDef( jobId, dbName, collName, idx )
  // for ( let idxName in collOpts.index ) {
  //   await creCollIdx( dbName, collName, idxName, collOpts.index[ idxName ] )
  // }
  writeOpsOngoing --
  return { _ok: true }
}

async function getColl( dbName, collName, options= {} ) {
  if ( ! db[ dbName ] ) { return null }
  
  if ( ! collName ) { // get all collections
    // log.info( 'PERSIST getColl all', dbName)
    let collArr = []
    for ( let coll in db[ dbName ].collection  ) {
      collArr.push( coll )
    }
    // log.info( 'PERSIST getColl all', collArr )
    return { _ok: true, collections: collArr }
  }

  if ( ! db[ dbName ].collection[ collName ] ) { return null }
  
  // TODO:
  return db[ dbName ].collection[ collName ]
}

async function getCollPK( dbName, collName ) {
  if ( ! db[ dbName ] ) { return null }
  if ( ! db[ dbName ].collection[ collName ] ) { return null }
  return db[ dbName ].collection[ collName ].primaryKey
}

async function getCollSpec( dbName, collName ) {
  if ( ! db[ dbName ] ) { return null }
  if ( ! db[ dbName ].collection[ collName ] ) { return null }
  let spec = {
    db         : dbName,
    collection : collName,
    pk         : db[ dbName ].collection[ collName ].primaryKey,
    idx        : db[ dbName ].collection[ collName ].index,
    masterData : db[ dbName ].collection[ collName ].masterData
  }
  // log.info( 'spec o', db[ dbName ].collection[ collName ])
  // log.info( 'spec', spec)
  return spec
}

async function renameColl( jobId, dbName, oldCollName, newCollName ) { 
  log.info( jobId, 'RENAME Coll', dbName, oldCollName, newCollName )
  writeOpsOngoing ++
  await dbFile.renameColl( jobId, dbName, oldCollName, newCollName )
  db[ dbName ].collection[ newCollName ] = db[ dbName ].collection[ oldCollName ]
  delete db[ dbName ].collection[ oldCollName ]
  writeOpsOngoing --
  return { _ok: true }

}

async function delColl( jobId, dbName, collName ) {
  log.info( jobId, 'PERSIST delColl', dbName, collName )
  writeOpsOngoing ++
  db[ dbName ].collectionDeleted[ collName ] = Date.now()
  delete db[ dbName ].collection[ collName ]
  await dbFile.delColl( dbName, collName )
  writeOpsOngoing --
  return { _ok: true }
}

// ============================================================================

async function updateIdx( jobId, dbName, collName, idx ) {
  log.debug( jobId, 'updateIdx', dbName, collName )
  if ( ! db[ dbName ] ) { return { _ok: false } }
  if ( ! db[ dbName ].collection[ collName ] ) { return { _ok: false } }
  if ( deepEqual( idx, db[ dbName ].collection[ collName ].index ) ) {
    return { _ok: true }
  }
  writeOpsOngoing ++
  log.info( jobId, 'updateIdx changed', dbName, collName )
  db[ dbName ].collection[ collName ].index = idx
  await indexer.updateIndex( jobId, dbName, collName, idx )
  // TODO: make this async
  let idxResult = await indexer.reIndex( jobId, dbName, collName, idx )
  writeOpsOngoing --
  return { _ok: true, result: idxResult.result }
}

function deepEqual( x, y ) {
  const ok = Object.keys, tx = typeof x, ty = typeof y
  return x && y && tx === 'object' && tx === ty ? (
    ok(x).length === ok(y).length &&
      ok(x).every(key => deepEqual(x[key], y[key]))
  ) : (x === y)
}


async function reIdx( jobId, dbName, collName ) {
  log.info( jobId, 'PERSIST reIdx', dbName, collName )
  if ( ! db[ dbName ] ) { return { _ok: false } }
  if ( ! db[ dbName ].collection[ collName ] ) { return { _ok: false } }
  writeOpsOngoing ++
  let idx = db[ dbName ].collection[ collName ].index
  let idxResult = await indexer.reIndex( jobId, dbName, collName, idx )
  writeOpsOngoing --
  return { _ok: true, result: idxResult.result }
}

async function listCollIdx( dbName, collName ) {
  log.debug( 'PERSIST listCollIdx', dbName, collName )
  if ( ! db[ dbName ] ) { return { _ok: false } }
  if ( ! db[ dbName ].collection[ collName ] ) { return { _ok: false } }
  let result =  { _ok: true }
  result['primaryKey'] = db[ dbName ].collection[ collName ].primaryKey
  result['index'] = db[ dbName ].collection[ collName ].index
  return result
}

async function geCollIdx( dbName, collName ) {
  log.debug( 'PERSIST listCollIdx', dbName, collName )
  if ( ! db[ dbName ] ) { return { _ok: false } }
  if ( ! db[ dbName ].collection[ collName ] ) { return { _ok: false } }
  return { _ok: true, idx: db[ dbName ].collection[ collName ].index }
}

async function delCollIdx( jobId, dbName, collName, idxlName ) {
  log.debug( jobId, 'PERSIST deleteCollectionIndex', dbName, collName, idxlName )
  writeOpsOngoing ++
  db[ dbName ].collection[ collName ].indexDeleted[ idxlName ] = db[ dbName ].collection[ collName ].index[ idxlName ]
  delete db[ dbName ].collection[ collName ].indexDeleted[ idxlName ]
  await indexer.delIndex( jobId, dbName, collName, idxlName )
  writeOpsOngoing --
  return { _ok: true }
}


// ============================================================================

// async function getUser( user, password ) { // TODO !!!!!!!!!!!!!!!!
//   log.info( 'PERSIST getUser', user )
//   if ( db.admin.collection.user.doc[ user ] && db.admin.collection.user.doc[ user ].password === password ) {
//     return db.admin.collection.user.doc[ user ]
//   } 
//   return null
// }

async function listUserRights() {
  log.debug( 'PERSIST listUserRights' )
  let result = []
  let users = await getAllDoc( 'admin', 'user' )
  if ( users._ok && Array.isArray( users.doc )) {
    for ( let u of users.doc ) {
      // log.info( 'PERSIST listUserRights', u )
      result.push({ userid: u.user, email: u.email, autz: u.autz })
    } 
  }
  return result
}
