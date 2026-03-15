const cfgHlp   = require( '../helper/config' )
const log      = require( '../helper/logger' ).log
const helper   = require( './db-helper' )
const pubsub   = require( '../cluster-mgr/pubsub' )

const docCache  = require( './db-cache' )
const manageDBs = require( './db-mgr' )
const dbDocCre  = require( './db-doc-cre' )
const dbDocFind = require( './db-doc-find' )
const dbDocUpd  = require( './db-doc-upd' )
const dbDocDel  = require( './db-doc-del' )
const dbMetrics = require( './db-metrics' )
const dbJobs    = require( './db-jobs' )
const persistence = require( './db-persistence' )

let nodeMgr = null

module.exports = {
  init,
  terminate,
  ownNodeAddr,

  dbOk                  : persistence.dbOk,
  getDB                 : persistence.getDB,
  updDB                 : persistence.updDB,
  listDBs               : persistence.listDBs,
  listAllDBs            : persistence.listAllDBs,
  getDbTree             : persistence.getDbTree,
  updateDbTree          : persistence.updateDbTree,

  getColl               : manageDBs.getColl,

  getPkID               : helper.getKeyHash,
  getTxnId              : helper.getTxnId,

  insertOneDoc,
  addAuditLog           : dbDocCre.addAuditLog,
  creDocByIdMsg         : dbDocCre.creDocByIdMsg,

  find,
  findDocs,
  findOneDoc,
  creDocsFoundResponse  : dbDocFind.creDocsFoundResponse,
  
  updateOneDoc,
  updateOneDocAllNodes,
  replaceOneDoc,

  deleteOneDoc,
  deleteOneDocAllNodes,

  getDocById,
  listUserRights        : persistence.listUserRights,
  // changeUserRights      : persistence.changeUserRights,

  sendDataBatch         : dbJobs.sendDataBatch,
  storeDataBatch        : dbJobs.sendDataBatch, 
  getJobs               : dbJobs.getJobs,
  manageJobs            : dbJobs.manageJobs,
  creTransferDataJobs   : dbJobs.creTransferDataJobs,

  startConsistencyChecks,
  checkDataConsistency,
  processQueuedDtaUpd
}

// ============================================================================

let cfg = {
  MODE : 'RMQ',
  DATA_REPLICATION   : 3,
  TEST_USER : null,
  TEST_PWD  : null,
  TOKEN_LEN : 1,
  MEASURE_PERF: false,
  ERR_LOG_EXPIRE_DAYS : 31
}

let REPLICATION_QUORUM = 2

let startupChecks = null

let scheduleTasksInterval = null

async function init( configParams, nodeMgrObj ) {
  log.info( 'Init DB ...')
  cfgHlp.setConfig( cfg, configParams )

  helper.setTokenLength( cfg.TOKEN_LEN )
  if ( cfg.MEASURE_PERF ) { helper.toggleMeasurePerfOn() }

  docCache.init( configParams )
  nodeMgr = nodeMgrObj 
  switch ( cfg.DATA_REPLICATION ) {
    case 1: // single node mode
      REPLICATION_QUORUM = 1
      break;
    case 2: // master / master
      REPLICATION_QUORUM = 2
      break;

    default: break;
  }
  dbDocFind.setReplQuorum( REPLICATION_QUORUM )
  dbDocFind.init( nodeMgr )
 
  await persistence.init( nodeMgr.ownNodeAddr(), configParams ) 
  
  // faster startup: check for jobs quite frequent
  startupChecks = setInterval( scheduleTasks, 300 )
  setTimeout( () => { clearInterval( startupChecks ) }, 9000 )

  // normal operations: check fir jobs
  scheduleTasksInterval = setInterval( scheduleTasks, 10000 ) // or 15_

  dbJobs.init( configParams, nodeMgr )
  dbMetrics.init( configParams, nodeMgr )
}


async function terminate() {
  log.info( 'Terminate DB...' )
  try {
    clearInterval( scheduleTasksInterval )
    clearInterval( sendDtaConsistentInterval )
    await dbMetrics.terminate()
  } catch ( exc ) { log.info( 'Terminate DB', exc ) }
  await persistence.terminate()
}

// ============================================================================

async function insertOneDoc( r, doc ) {
  let result = await dbDocCre.insertOneDoc( r, doc )
  dbMetrics.addDbMetric( r.db, r.coll, "ins", result )
  return result
}

//-----------------------------------------------------------------------------

async function find( dbName, collName, query, options = {} ) {
  let result = await dbDocFind.find( dbName, collName, query, options )
  dbMetrics.addDbMetric( dbName, collName, "fnd", result )
  return result
}

async function findDocs( r, query ) {
  let result = await dbDocFind.findDocs( r, query )
  dbMetrics.addDbMetric( r.db, r.coll, "fnd", result )
  return result
}

async function findOneDoc( r, filter ) {
  let result = await dbDocFind.findOneDoc( r, filter )
  dbMetrics.addDbMetric( r.db, r.coll, "fnd", result )
  return result
}

//-----------------------------------------------------------------------------
/* r = {
      db     : 'xz',
      coll   : 'xzy',
      update : { $set: { a: 'b' }},
      txnId  :  'someId',
      fn     : 'someName'
   }
   doc = { _id: '123' }
   opt =  { allNodes: true } // or null
*/
async function updateOneDoc( r, doc, opt = {} ) {
  if ( ! doc._id ) { return { _error: 'Require _id' } }
  let docById = await persistence.getDocById( r.db, r.coll,  doc._id )
  if ( docById._error ) { return docById }
  let result = await dbDocUpd.updateOneDoc( r, doc, docById.doc, opt )
  dbMetrics.addDbMetric( r.db, r.coll, "upd", result )
  return result
}

async function updateOneDocAllNodes( r, doc ) {
  let result = await updateOneDoc( r, doc, { allNodes: true } )
  return result
}

async function replaceOneDoc( txnId, dbName, coll, id, doc, opt = {} ) {
  log.info( txnId, 'replaceOneDoc', dbName, coll, id )
  let updMsg = {
    op    : 'replace one',
    txnId : txnId,
    db    : dbName,
    col   : coll,
    docId : id,
    doc   : doc
  }

  let collSpec =  await manageDBs.getCollSpec( dbName, coll )
  if ( collSpec.masterData ) {
    opt.allNodes = true
  }
  if ( collSpec.pk?.length > 0 ) {
    let pKeyHash = await helper.getPkHash( dbName, coll, doc, collSpec.pk )
  
    if ( id != pKeyHash ) {
      return { _error : 'id validation failed' }
    }  
  }
  updMsg.doc._id  = id           // just to be sure
  updMsg.doc._chg = Date.now()

  if ( opt.allNodes ) {
    pubsub.sendRequestAllNodes( txnId, updMsg )
  } else {
    let token = helper.extractToken( id )
    pubsub.sendRequest( txnId, token, updMsg )
  }
  let result = await pubsub.getReplies( txnId )
  dbMetrics.addDbMetric( dbName, coll, "upd", result )
  return { _ok: result._ok, _error: result._error, _id: doc._id }
}
//-----------------------------------------------------------------------------

async function deleteOneDoc( r, id ) {
  let result = await dbDocDel.deleteOneDoc( r, id )
  dbMetrics.addDbMetric( r.db, r.coll, "del", result )
  return result
}

async function deleteOneDocAllNodes( r, id ) {
  let result = await dbDocDel.deleteOneDocAllNodes( r, id )
  dbMetrics.addDbMetric( r.db, r.coll, "del", result )
  return result
}

//-----------------------------------------------------------------------------

async function getDocById( dbName, collName, docId, options = {} ) {
  let result = await persistence.getDocById( dbName, collName, docId, options )
  dbMetrics.addDbMetric( dbName, collName, "del", result )
  return result
}

// ============================================================================
// TODO move to module
async function scheduleTasks() {
  let tasks = nodeMgr.getDbTask()
  let jobId = helper.getTxnId( 'INI' )
  for ( let task of tasks ) {
    switch ( task ) {

      case 'initAdminDB': 
        // "admin" DB is required for all further DB initialization
        let needCreateInitialUsers = await manageDBs.initAdminDB( jobId, cfg.ERR_LOG_EXPIRE_DAYS )   
        if ( needCreateInitialUsers ) try {
          await dbDocCre.addAuditLog( 'admin', 'cluster', 'db', 'Created new database cluster.' )
          log.info( 'initAdminDB add users ####################################################')
          await dbDocCre.insertOneDoc( { db: 'admin', coll: 'user' }, await helper.newAdminUser() )
          await dbDocCre.addAuditLog( 'admin', 'user', 'db', 'Added "admin" user.' )
          if ( cfg.TEST_USER && cfg.TEST_PWD ) {
            await dbDocCre.insertOneDoc(
              { db: 'admin', coll: 'user' }, 
              await helper.newMochaTestUser(  cfg.TEST_USER, cfg.TEST_PWD ) 
            )
            await dbDocCre.addAuditLog( 'admin', 'cluster', 'db', 'Created test user "'+cfg.TEST_USER+'".' )
          }
        } catch ( exc ) { log.warn( 'cre admin', exc ) }
        nodeMgr.dbInitDone()
        break

      case 'ChkAdminDB':
        await manageDBs.initAdminDB( jobId, cfg.ERR_LOG_EXPIRE_DAYS ) 
        break

      case 'initDemoDB':  
        if ( ! await getColl(  'demo', 'test-coll')  ) {
          await manageDBs.creDB( 'demo' ) 
          await manageDBs.creColl( jobId, 'demo', 'test-coll', helper.demoCollSpec() )
          await dbDocCre.insertOneDoc( { db: 'demo', coll: 'test-coll', txnId: jobId },  helper.demoCollRec() )
        }
        break

      default: break
    }
  }
  if ( cfg.MODE != "SINGLE_NODE" ) { 
    let { inLeadJobs, passiveJobs } = await dbJobs.getJobs()
    dbJobs.manageJobs( inLeadJobs )
    dbJobs.checkJobToDos( passiveJobs )
  }
}


function ownNodeAddr() {
  return nodeMgr.ownNodeAddr()
}

// ============================================================================
let sendDtaConsistentInterval = null
let masterTokenArr = []
let nodeId = null

// started by nodeMgr if status initDone
function startConsistencyChecks( ownNodeId, tokenMap ) {
  log.info( 'startConsistencyChecks...' )
  nodeId = ownNodeId
  for ( const tk in tokenMap ) try {
    if ( tokenMap[ tk ].replNodeId[ ownNodeId ].status == 'master' ) {
      masterTokenArr.push( tk )
    }
  } catch ( exc ) { log.warn( 'startConsistencyChecks', exc ) }
  log.info( 'startConsistencyChecks tokenMap', ownNodeId, masterTokenArr )
  sendDtaConsistentInterval = setInterval( sendDataOk, 5000 )
}

// send out every seconds some data, to let other nodes check if these are consistent
async function sendDataOk( ) {
  try {
    const txnId = helper.getTxnId( 'DCO' )
    const check = await persistence.genConsistencyCheck( masterTokenArr )
    if ( ! check ) { return }
    let checkConsistency = {
      op    : 'CheckDataConsistency',
      txnId : txnId,
      frm   : nodeId,
      db    : check.db,
      col   : check.col,
      tkn   : check.tkn,
      doc   : check.doc
    }
    pubsub.sendRequest( txnId, check.tkn, checkConsistency )      
  } catch ( exc ) { log.warn( 'sendDataOk', exc ) }
}


async function checkDataConsistency( check ) {
  try {
    if ( check.frm == nodeId ) { 
      log.debug( check.txnId, 'checkDataConsistency ignore own message' )
      return 
    }
    let consistent = true
    log.debug( check.txnId, 'checkDataConsistency ... ', check.db, check.col, check.tkn )
    let myDocs = await persistence.getHashesOfToken( check.db, check.col, check.tkn )
    // log.info( '<', check.doc )
    // log.info( '>', myDocs )
    // if ( myDocs.length != check.doc.length ) { // TODO better check if equal
    //   log.warn( check.txnId, 'checkDataConsistency >> WARN doc count mismatch', check.db, check.col, check.tkn )
    //   consistent = false
    // }
    let inconsistentIDs = []
    for ( const id in check.doc ) {
      if ( ! myDocs[ id ] ) {
        log.warn( check.txnId, 'checkDataConsistency >> WARN doc missing', check.db, check.col, id )
        inconsistentIDs.push( id )
        consistent = false
      } else if ( check.doc[id] != myDocs[id] ) {
        log.warn( check.txnId, 'checkDataConsistency >> WARN doc inconsistent', check.db, check.col, id )
        inconsistentIDs.push( id )
        consistent = false
      }
    }
    for ( const id in myDocs ) {
      if ( ! check.doc[ id ] ) {
        log.warn( check.txnId, 'checkDataConsistency >> WARN doc only local pod', check.db, check.col, id )
        inconsistentIDs.push( id )
        consistent = false
      }
    }
    if ( consistent ) {
      log.info( check.txnId, 'checkDataConsistency >> OK', check.db, check.col, check.tkn )
    } else {
      await sleep( Math.floor( 5000 * Math.random() ) )
      for ( const id of inconsistentIDs ) {
        reSyncDoc( check.txnId, check.db, check.col, id ) 
      }  
    }
  } catch ( exc ) {
    log.error( check.txnId, 'checkDataConsistency', exc )
  }
}

const sleep = ms => new Promise( r => setTimeout( r, ms ) )


async function reSyncDoc( txnId, dbName, collName, id ) {
  try {
    log.info( txnId, 'Trying to sync document', dbName, collName, id  )
    let qryMsg = await dbDocCre.creDocByIdMsg( { db:dbName, coll:collName, txnId: txnId }, id )
    pubsub.sendRequest( txnId, id[0], qryMsg )
    let result = await pubsub.getReplies( txnId )
    if ( result._ok ) {
      let docs = {}
      let shaQuorum = {}
      let shaMax = null
      let i = 0
      for ( let binMsg of result.replyMsg ) try {
        let msg = ( binMsg.content ? JSON.parse( binMsg.content.toString() ) : binMsg )
        // log.info( 'msg', msg )
        if ( ! msg.doc ) { continue }
        let sha2 = await helper.checksum( JSON.stringify( msg.doc ) )
        docs[ i ] = {
          data : msg.doc,
          dt   : msg.doc._chg,
          sha2 : sha2
        }
        if ( ! shaQuorum[ sha2 ] ) { shaQuorum[ sha2 ] = { cnt : 0, no: [] } }
        shaQuorum[ sha2 ].cnt ++
        shaQuorum[ sha2 ].doc = msg.doc
        shaQuorum[ sha2 ].no.push( msg.node)
        if ( ! shaMax || shaQuorum[ sha2 ].cnt > shaQuorum[ shaMax ].cnt ) {
          shaMax = sha2
        }
        i ++
      } catch ( exc ) { log.warn( txnId, 'Error with document', dbName, collName, id, exc.message ) }
      log.debug( txnId, 'Inspect documents:', docs )
      log.debug( txnId, 'Trying to sync document shaMax:', shaMax )
      if ( shaQuorum.length == 1 && shaQuorum[ shaMax ].cnt > REPLICATION_QUORUM ) { 
        log.info( txnId, 'seemed sync done by other node' )
        return
      }
      if ( shaQuorum[ shaMax ].cnt >= REPLICATION_QUORUM ) {
        log.info( txnId, 'Trying to sync document shaMax quorum ok:', shaQuorum[ shaMax ].cnt )
        const txnIdUpd = helper.getTxnId( 'DCS' )
        let token  = helper.extractToken( id )
        let updMsg = {
          op    : 'replace one',
          txnId : txnIdUpd,
          db    : dbName,
          col   : collName,
          docId : id,
          doc   : shaQuorum[ shaMax ].doc
        }
        pubsub.sendRequest( txnIdUpd, token, updMsg )
        let syncResult = await pubsub.getReplies( txnIdUpd )
        // let syncResult = await dbDocUpd.replaceOneDoc( txnId, dbName, collName, id,  shaQuorum[ shaMax ].doc )
        if ( syncResult._ok ) {
          log.warn( txnId, 'Docs replaced by quorum, OK' )
        } else {
          log.error( txnId, 'Docs replaced by quorum, error', syncResult )
        }
      } else { // try to find newest
        log.error( txnId, 'FAILED to sync document shaMax quorum NOT OK', shaQuorum[ shaMax ].cnt )
      }
    } else {
      log.error( txnId, 'FAILED to sync document', dbName, collName, id )
    }
  } catch ( exc ) {
    log.error( txnId, 'reSyncDoc', exc )
  }
}

// ============================================================================

async function processQueuedDtaUpd( dbReq ) {
  try {
    let dta = dbReq.data
    // if ( ! dta.txnId.startsWith('MX') )
      // log.info( dta.txnId, 'DB process', dbReq.msgProp.headers.from, '"'+dta.op+'"', dta.db, dta.col )
    if ( dta.col != 'job' )
      log.debug( 'DB process', dbReq )
    let resultData = null 
    switch ( dta.op ) {
      case 'insert':
        resultData = await dbDocCre.insertQ( dbReq.data )
        break;
        
      case 'insertAllNodes':
        resultData = await dbDocCre.insertOneDoc( { db: dta.db, coll: dta.col }, dta.doc )
        break;
      
      case 'update':
        log.warn('TODO implement: update')
        break;

      case 'replace one':
        resultData = await dbDocUpd.replaceOneDoc( dta.txnId, dta.db, dta.col, dta.docId, dta.doc, dta.opt )
        await dbJobs.checkForJob( dta )
        break;
      
      case 'find all doc':
        resultData = await dbDocFind.getAllDoc( dta.txnId, dta.db, dta.col, dta.qry, dta.proj, dta.opt )
        dbReq.msgProp.headers.resultIsArray = true
        break;

      case 'find by PK':
        resultData = await persistence.getDocById( dta.db, dta.col, dta.docId, dta.opt )
        break;
      
      case 'find by IDX':
        resultData = await dbDocFind.getAllDoc( dta.txnId, dta.db, dta.col, dta.qry, dta.proj, dta.opt, true )
        log.debug( 'resultData', resultData )
        if ( ! resultData._error ) {
          dbReq.msgProp.headers.resultIsArray = true
        }
        break;
      
      case 'find full scan':
        // log.warn('find full scan .................................')
        if ( dta.opt && dta.opt.optimize == 'only master nodes' ) {
          if ( nodeMgr.allNodesOK() ) { // need to check if master nodes are alive
            log.warn( dta.txnId, 'DB process: All nodes OK ... OPTIMIZE full scan' )
            dta.opt.ownToken = nodeMgr.getOwnTokens()
          } else {
            log.warn( dta.txnId, 'DB process> Not all nodes OK, NO OPTIMIZE' )
            dta.opt.optimize = 'none'
          }
        }
        resultData = await dbDocFind.getAllDoc( dta.txnId, dta.db, dta.col, dta.qry, dta.proj, dta.opt )
        dbReq.msgProp.headers.resultIsArray = true
        // log.warn('find full scan ..........', resultData.docId )
        break;

      
      case 'get by id':
        resultData = await persistence.getDocById( dta.db, dta.col, dta.docId, dta.opt )
        break;

      case 'countDocuments':
        log.warn('TODO implement: countDocuments')
        break;

      case 'delete doc':
        resultData = await dbDocDel.deleteDocById( dta.txnId, dta.db, dta.col, dta.docId, dta.opt )
        break;

      case 'listUserRights':
        log.warn('TODO implement: listUserRights')
        break;

      case 'changeUserRights':
        log.warn('TODO implement: changeUserRights')
        break;

      case 'CheckDataConsistency':
        log.debug( dta.txnId, ' <<<< CheckDataConsistency ',dta )
        checkDataConsistency( dta )
        break

      default:
        break;
    }

    let result = {
      msgProp : dbReq.msgProp,
      data    : resultData
    }
    // if ( ! dta.txnId.startsWith('MX') )
    //   log.info( 'DB process >>>', result)
    await pubsub.sendResponse( dta.txnId, result )
  } catch ( exc ) {
    log.error( dbReq.data.txnId, 'DB process', exc, dbReq )
  }
}
