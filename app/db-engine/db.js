const cfgHlp   = require( '../helper/config' )
const log      = require( '../helper/logger' ).log
const logger   = require( '../helper/logger' )
const helper   = require( './db-helper' )
const pubsub   = require( '../cluster-mgr/pubsub' )

const docCache  = require( './db-cache' )
const manageDBs = require( './db-mgr' )
const dbDocCre  = require( './db-doc-cre' )
const dbDocFind = require( './db-doc-find' )
const dbDocUpd  = require( './db-doc-upd' )
const dbDocDel  = require( './db-doc-del' )

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
  changeUserRights      : persistence.changeUserRights,

  sendDataBatch,
  storeDataBatch,

  getJobs,
  manageJobs,
  processQueuedDtaUpd,
  createTransferTokenDataJobs
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
let persistMetricsInterval = null
let persistErrLogsInterval = null


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

  // persist db metrics
  persistMetricsInterval = setInterval( persistMetrics, 60000 + Math.floor( Math.random() * 10000 ) )
  persistErrLogsInterval = setInterval( persistErrLogs, 10000 ) // + Math.floor( Math.random() * 10000 ) )
}


async function terminate() {
  log.info( 'Terminate DB...' )
  try {
    clearInterval( scheduleTasksInterval )
    clearInterval( persistMetricsInterval )
    clearInterval( persistErrLogsInterval )      
  } catch ( exc ) { log.info(  'Terminate DB', exc )
  }
  await persistence.terminate()
}

// ============================================================================

async function insertOneDoc( r, doc ) {
  let result = await dbDocCre.insertOneDoc( r, doc )
  addDbMetric( r.db, r.coll, "ins", result )
  return result
}

//-----------------------------------------------------------------------------

async function find( dbName, collName, query, options = {} ) {
  let result = await dbDocFind.find( dbName, collName, query, options )
  addDbMetric( dbName, collName, "fnd", result )
  return result
}

async function findDocs( r, query ) {
  let result = await dbDocFind.findDocs( r, query )
  addDbMetric( r.db, r.coll, "fnd", result )
  return result
}

async function findOneDoc( r, filter ) {
  let result = await dbDocFind.findOneDoc( r, filter )
  addDbMetric( r.db, r.coll, "fnd", result )
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
  let docbyId = await persistence.getDocById( r.db, r.coll,  doc._id )
  if ( docbyId._error ) { return docbyId }
  let result = await dbDocUpd.updateOneDoc( r, doc, docbyId.doc )
  addDbMetric( r.db, r.coll, "upd", result )
  return result
}

async function updateOneDocAllNodes( r, doc ) {
  let result = await dbDocUpd.updateOneDoc( r, doc, { allNodes: true } )
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
  addDbMetric( dbName, coll, "upd", result )
  return { _ok: result._ok, _error: result._error, _id: doc._id }
}
//-----------------------------------------------------------------------------

async function deleteOneDoc( r, id ) {
  let result = await dbDocDel.deleteOneDoc( r, id )
  addDbMetric( r.db, r.coll, "del", result )
  return result
}

async function deleteOneDocAllNodes( r, id ) {
  let result = await dbDocDel.deleteOneDocAllNodes( r, id )
  addDbMetric( r.db, r.coll, "del", result )
  return result
}

//-----------------------------------------------------------------------------

async function getDocById( dbName, collName, docId, options = {} ) {
  let result = await persistence.getDocById( dbName, collName, docId, options )
  addDbMetric( dbName, collName, "del", result )
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
    let { inLeadJobs, passiveJobs } = await getJobs()
    manageJobs( inLeadJobs )
    checkJobToDos( passiveJobs )
  }
}


function ownNodeAddr() {
  return nodeMgr.ownNodeAddr()
}

// ============================================================================

async function getJobs() {
  let inLeadJobs = []
  let passiveJobs = []
  let jobsResult = await dbDocFind.find( 'admin', 'job', { } )
  if ( ! jobsResult._error && jobsResult.data ) { 
    for ( let job of jobsResult.data ) {
      if ( job.nodeId == nodeMgr.ownNodeId() ) {
        inLeadJobs.push( job )
      } else 
      if ( job.toNode == nodeMgr.ownNodeId() ) {
        passiveJobs.push( job )
      }
    }
    inLeadJobs.sort( (a,b) => { return ( a._cre - b._cre ) })
    passiveJobs.sort( (a,b) => { return ( a._cre - b._cre ) })
  }
  return { 
    inLeadJobs: inLeadJobs,
    passiveJobs: passiveJobs
  }
}

async function manageJobs( jobs ) {
  if ( ! jobs || jobs.length == 0 ) { return }
  let job = jobs[ 0 ]
  // log.info( 'JOOOOb', JSON.stringify(job))
  if ( ! job.started ) {
    await startBatch( job )
  } else {
    if ( job.sentNextBatch ) {
      await sendDataBatch( job )
    }
  }
  // let cre1 = jobsResult.data [0]._cre
  // for ( let job of jobs ) {
  //   log.info( 'jobs', job._cre, cre1 - job._cre, job.jobId, job.done, job.started )
  // }
}


async function checkJobToDos( jobs ) {
  if ( ! jobs || jobs.length == 0 ) { return }
  for ( let job of jobs ) {
    if ( job.started ) {
      log.info( job.jobId, 'CHECK CLIENT JOBS started ... ' )
      if ( job.waitingForReceiver ) {
        await requestNextBatch( job )
      } else {
        log.info( job.jobId, 'CHECK CLIENT JOBS',  Date.now() - job._chg,  Date.now() , job._chg)
        if ( Date.now() - job._chg > 60000 ) {
          await requestNextBatch( job )
        }
      }  
    } 
  }
}


const BATCH_COUNT = 10


async function sendDataBatch( job ) {
  log.info( job.jobId, 'jobs','######### sendDataBatch', job.db, job.coll, job.fromNode, job.token, job.done )
  
  let batchIds = await persistence.getAllDocIds( job.jobId, job.db, job.coll, { start: job.done, count: BATCH_COUNT } )
  if ( idArr._error ) { return batchIds }
  for ( let docId of batchIds ) {

    let doc = await persistence.getDocById( job.db, job.coll, docId )
    let dataMsg = {
      _id      : job._id,
      jobId    : job.jobId,
      action   : 'InsertLocally',
      fromNode : job.rmFrmNode,
      toNode   : job.toNode,
      db       : job.db, 
      coll     : job.coll,
      data     : [ doc ] // TODO, check size and send many
    }
    log.info( job.jobId, 'jobs','######### sendDataBatch DOC', job.db, job.coll, docId )

    await pubsub.sendToQueue( job.jobId, job.queue, 'TransferData', dataMsg )
  }

  let lastBatch = false
  if ( batchIds.length < BATCH_COUNT ) { 
    lastBatch = true
  }

  let batchEndMsg = {
    _id      : job._id,
    jobId    : job.jobId,
    action   : ( lastBatch ? 'Completed' : 'BatchEnd' ),
    fromNode : job.rmFrmNode,
    toNode   : job.toNode,
    db       : job.db, 
    coll     : job.coll,
  }
  await pubsub.sendToQueue( job.jobId, job.queue, 'TransferData', batchEndMsg )
}

async function storeDataBatch( job ) {
  log.info( 'storeDataBatch', job  )
  switch ( job.action  ) {

    case 'InsertLocally' :
      for ( let doc in job.data ) {
        persistence.insertDocPrep( job.jobId, job.db, job.coll, doc ) 
      }
      break

    case 'BatchEnd' :
      await requestNextBatch( job, BATCH_COUNT )
      break

    case 'Completed' :
      await endJob( job )
      break

    default:
      break;
  }
}


async function startBatch( job ) {
  log.info( job.jobId, 'jobs','######### startBatch', job.db, job.coll, job.fromNode+'>'+job.toNode, job.token )
  await updateJob( job, { 
    started            : true, 
    waitingForReceiver : true, 
    sentNextBatch      : false,
    batchDone          : false
  })
}

async function requestNextBatch( job, done = 0 ) {
  log.info( job.jobId, 'jobs','######### requestNextBatch', job.db, job.coll, job.fromNode+'>'+job.toNode, job.done + BATCH_COUNT,  pubsub.getReplyQueue() )
  await updateJob( job, { 
    sentNextBatch      : true,
    batchDone          : true,
    waitingForReceiver : false,
    done               : job.done + done,
    queue              : pubsub.getReplyQueue()
  })
}

async function updateJob( job, update ) {
  log.info( job.jobId, 'jobs','######### send updateJob', job.db, job.coll, job.fromNode+'>'+job.toNode, job.token )
  const JOB_UPD = { db : 'admin', coll: 'job', txnId : job.jobId, 
    update: { $set: update } 
  }
  await updateOneDoc( JOB_UPD, { _id: job._id }, { allNodes: true } )
}

async function endJob( job ) {
  log.info( job.jobId, 'jobs','######### send endJob', job )
  const JOB = { db : 'admin', coll: 'job', txnId : job.jobId+'.DEL' }
  await dbDocDel.deleteOneDocAllNodes( JOB, job._id)
}


async function checkForJob( dta ) {
  if ( dta.db == 'admin' && dta.col == 'job' ) {
    let job = await persistence.getDocById( dta.db, dta.col, dta.docId )
    //log.info('CHECK JOB >>>>>>>>>>>>>>>>>>>>>>>>>>>>',  dta.upd,  job.doc?.fromNode, nodeMgr.ownNodeId() )
    if ( job.doc?.fromNode == nodeMgr.ownNodeId() ) {
      manageJobs([ job.doc ])
    } else  
    if ( job.doc?.toNode == nodeMgr.ownNodeId() ) {
      checkJobToDos([ job.doc ])
    }
  }
}

// ============================================================================

async function createTransferTokenDataJobs( task ) {
  log.info( 'TransferTokenData >>>', task )
  const JOB_COLL = { db : 'admin', coll: 'job', txnId : task.jobId }
  let subTsk = 0
  let dbTree = await getDbTree()
  // log.info( 'TransferTokenData', dbTree )
  for ( let dbName in dbTree ) {
    log.info( 'TransferTokenData', dbName )
    for ( let collName in dbTree[ dbName ].c ) {
      if ( dbName == 'admin' && collName == 'job' ) { continue }
      let coll = dbTree[ dbName ].c[ collName ]
      log.info( 'TransferTokenData', dbName, collName, coll.masterData  )

      let transferJob = {
        job      : 'TransferTokenData',
        action   : task.action,
        jobId    : task.jobId +'.'+  nodeMgr.ownNodeId() +'.'+ subTsk,
        nodeId   : nodeMgr.ownNodeId(),
        fromNode : task.fromNode, // thats me
        toNode   : task.toNode,
        db       : dbName,
        coll     : collName,
        done     : 0
      }

      if ( coll.masterData ) {
        if ( task.action == 'CopyMasterData' ) { 
          transferJob.token  = '*'
        } else { continue }
      } else {
        transferJob.token  = task.token
      }

      if ( task.master ) {
        transferJob. master = task.masterNode,
        transferJob.replica = task.replicaNode
      }

      log.info( 'TransferTokenData', JSON.stringify( transferJob ) )
      await dbDocCre.insertOneDoc( JOB_COLL, transferJob )
      subTsk ++
      
    }
  }
}

// ============================================================================
// Callback if a message is received in the data queue

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
        await  checkForJob( dta )
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
        log.warn('TODO implement: ')
        break;

      case 'changeUserRights':
        log.warn('TODO implement: listUserRights')
        break;

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


// ============================================================================
let DB_METRICS = {}
let DB_METRICS_ID = {}
let metricChanged = false

async function persistMetrics() {
  try {
    log.debug( 'persistMetrics...' )
    // log.debug( 'persistMetrics', JSON.stringify( DB_METRICS, null, '  ' ), DB_METRICS_ID )
    if ( ! nodeMgr.isInitOK() ) { return }
    let now = Math.floor( Date.now() / 60000 )
    let txnId = 'DMX' + helper.randomChar( 10 )
    for ( let dbName in DB_METRICS ) {
      let db = DB_METRICS[ dbName ]
      let needSave = false
      let metricsUpd = { 
        db   : dbName, 
        coll : {}
      }
      for ( let collName in db ) {
        // log.info( 'persistMetrics', dbName, collName )
        let coll = db[ collName ]
        for ( ts in coll ) {
          // log.info( 'persistMetrics >>', dbName, collName, ts )
          let metrics = coll[ ts ]
          if ( metrics.ts != now  ) {
            if ( ! metricsUpd.coll[ collName ] ) { metricsUpd.coll[ collName ] = {} }
            if ( ! metricsUpd.coll[ collName ][ ts ] ) { metricsUpd.coll[ collName ][ ts ] = {} }
            for ( let act in metrics ) {
              if ( act != "ts" ) { // timestamp, not action id
                metricsUpd.coll[ collName ][ ts ][ act ] = metrics[ act ]
                needSave = true
              }  
            }
            delete coll[ ts ]
          }
        }
      }
      if ( ! needSave ) { continue }
      let metricsColl = {
        txnId : txnId,
        db    : 'admin',
        coll  : 'db-metrics'
      }
      // if ( ! DB_METRICS_ID[ dbName ] ) {
        let find = await findOneDoc( metricsColl, { db : dbName } )
        // log.info( 'persistMetrics findOneDoc', find  )
        if ( ! find.doc ) {
          log.debug( 'persistMetrics insertOne', metricsUpd )
          let result = await insertOneDoc( metricsColl, metricsUpd )
          log.info( 'persistMetrics insertOne', result )
          if ( ! result._error && result.ins?._id ) {
            DB_METRICS_ID[ dbName ] = result  //<<<<<<<<<<<<<<<<<<<<
          }
        } else {
          metricsUpd._id = find.doc._id
          log.debug( 'persistMetrics updateOneDoc',  metricsUpd )
          // { $set: { 'blah.text': txt }
          let incMetrics = {}
          for ( let coll in metricsUpd.coll ) {
            for ( let ts in metricsUpd.coll[ coll ] ) {
              for ( let op in metricsUpd.coll[ coll ][ ts ] ) {
                incMetrics[ 'coll.'+coll +'.'+ ts +'.'+ op ] =  metricsUpd.coll[ coll ][ ts ][ op ]
              }
            }
          }
          metricsColl.update = { $inc : incMetrics }
          let result = await updateOneDoc( metricsColl, { _id: find.doc._id , db: dbName } ) 
          // log.info( 'persistMetrics updateOneDoc', result )
        }
      // }
      // if ( DB_METRICS_ID[ dbName ] ) {
      //   metricsUpd._id =  DB_METRICS_ID[ dbName ] //<<<<<<<<<<<<<<<<<<<<
      //   await updateOneDoc( metricsColl, metricsUpd ) /// <<<<<<<<<<<<<<< FIX  
      // }
    }
  } catch ( exc ) { log.error( 'persistMetrics', exc ) }
}

function addDbMetric( db, coll, action, result ) {
  if ( db == 'admin' ) {
    if (  coll = 'api-metrics' ) return
    if (  coll = 'db-metrics'  ) return
  }
  if ( ! DB_METRICS[ db ] ) {
    DB_METRICS[ db ] = {} 
  }
  if ( ! DB_METRICS[ db ][ coll ] ) { 
    DB_METRICS[ db ][ coll ] = {} 
  }
  let timestamp = Math.floor( Date.now() / 60000 ) 
  if ( ! DB_METRICS[ db ][ coll ][ timestamp ] ) {
    DB_METRICS[ db ][ coll ][ timestamp ] = { ts: timestamp}
  }
  if ( ! DB_METRICS[ db ][ coll ][ timestamp ][ action ] ) {
    DB_METRICS[ db ][ coll ][ timestamp ][ action ] = 0
    DB_METRICS[ db ][ coll ][ timestamp ][ 'err' ]  = 0
  }
  DB_METRICS[ db ][ coll ][ timestamp ][ action ] ++
  if ( result._error ) {
    DB_METRICS[ db ][ coll ][ timestamp ][ 'err' ] ++
  }
  metricChanged = true
}


async function persistErrLogs() {
  // log.info( 'persistErrLogs', logger.getErrLogs().length)
  if ( await persistence.getColl( 'admin', 'log' ) ) {
    for ( let log of logger.getErrLogs() ) {
      dbDocCre.saveErrLog( log )
    }
  }
}