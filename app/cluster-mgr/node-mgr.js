const cfgHlp   = require( '../helper/config' )
const log      = require( '../helper/logger' ).log
const metric   = require( '../helper/logger' ).countMetric
const statMgr  = require( './node-status-mgr' ) 
const tokenMgr = require( './token-mgr' )
const pubsub   = require( './pubsub' )
const db       = require( '../db-engine/db' )
const manageDB = require( '../db-engine/db-mgr' )
const backup   = require( '../db-engine/db-backup' )

module.exports = {
  init,
  updNode,
  ownNodeAddr,
  terminate,
  checkStopReq      : statMgr.checkStopReq,
  ownNodeId         : statMgr.ownNodeId,
  getOwnNodeStatus  : statMgr.getOwnNodeStatus,
  getAllNodeStatus  : statMgr.getClusterNodes,
  allNodesOK        : statMgr.allNodesOK,
  getClusterSize    : statMgr.getClusterSize,
  getOwnTokens      : statMgr.getOwnTokens,
  // handoverToken,
  // handoverTokenStatus,

  getNodes,
  getAllNodeNames,
  getHelper,
  getDbTask,
  broadcastNodeUpdate,
  onboardNode,
  // startOnboarding,
  // onboardTokens,

  dbInitDone,
  isInitOK,
  getApiMetricKeys,
  getApiMetrics,
  getApiMetricsForTS,
  setApiMetric,
  setApiMetricsForTS,
  incApiMetric,
  incApiMetricsForTS,
  prepTimestamp,
  loadMetrics,
  saveMetrics
}

let helper = null

let cfg = {
  MODE : 'RMQ',
  API_PATH: '/db',
  PORT: 9000,
  DB_POD_NAME: null,
  OWN_NODE_ADDR : 'localhost:9000/db',
  DATA_REPLICATION: 3,
  DATA_REGION: 'EU',
  DATA_DIR : './db/',
  DB_SEED_PODS : null,
  TOKEN_LEN : 1,
  NODE_SYNC_INTERVAL_MS: 10000,
  MONITOR_STATUS_SEC: 10
}

let fastStartInterval     = null
let saveApiMetricInterval = null
let startBackupInterval   = null 

async function init( configParams, options ) { 
  cfgHlp.setConfig( cfg, configParams )
  
  // on Mac the DB_POD_NAME must be defined per process instance
  let DB_POD_NAME = ( cfg.DB_POD_NAME ?  cfg.DB_POD_NAME : require('os').hostname() )
  cfg.OWN_NODE_ADDR = DB_POD_NAME +':'+ cfg.PORT + cfg.API_PATH

  log.info( 'Init Cluster / OWN_NODE_ADDR', cfg.OWN_NODE_ADDR )
  helper = await statMgr.init( cfg )
  tokenMgr.init()
  if ( options && options.testingOnly ) { return }
  
  await pubsub.init( cfg.OWN_NODE_ADDR, cfg,  incApiMetric, jobDispatcher )
  // await pubsub.init( cfg.OWN_NODE_ADDR )
  await pubsub.subscribeJobs( jobDispatcher ) 

  await statMgr.initSeedNodes()
  log.info( 'init done ... start syncing' )

  if ( cfg.MODE == "SINGLE_NODE"  ) { 
    broadcastNodeUpdate()
    fastStartInterval = setInterval( broadcastNodeUpdate, 300 )
  }
  setInterval( broadcastNodeUpdate, cfg.NODE_SYNC_INTERVAL_MS )
  setInterval( sendStatusMetric, cfg.MONITOR_STATUS_SEC * 1000 )
  saveApiMetricInterval = setInterval( saveApiMetric, 60*1000 )
  startBackupInterval = setInterval( startBackup, 5500 )

  let s = statMgr.getOwnNodeStatus()
  if ( s.status == 'Onboarding' ) {
    log.info( 'Add DB task: onboardTokens' )
    dbTaskArr.push( 'onboardTokens' )
  }
}

async function startBackup() {
  if ( ! await manageDB.getColl( 'admin', 'backup' ) ) { return }
  backup.init( cfg.OWN_NODE_ADDR )
  clearInterval( startBackupInterval )
}

async function terminate() {
  log.info( 'Terminate node manager...')
  statMgr.setStatus( 'Restarting' )
  await broadcastNodeUpdate()
  await pubsub.terminate()
  await db.terminate()
  clearInterval( saveApiMetricInterval )
}

// ----------------------------------------------------------------------------

function sendStatusMetric() {
  let s = statMgr.getOwnNodeStatus()
  let status = s.status.replaceAll( ' ', '_' )
  // log.debug('send metric', 'DB_STAT_' + status )
  metric( 'DB_STAT_' + status )
}

async function jobDispatcher( job, msgProp ) {
  if ( ! job.jobId ) { job.jobId = db.getTxnId( 'JOB' ) }
  log.debug( job.jobId, 'jobDispatcher >>>> SyncNodes' )
  let ownNode = statMgr.getOwnNodeStatus()
  let myNodeId = ownNode.nodeId + ''

  switch ( job.jobType ) {

    case 'DB Op':
      await manageDB.processDbOp( job, msgProp )
    break

    case 'Backup Op':
      await backup.processJob( job )
    break

    case 'SyncNodes':
      if ( cfg.MODE == "SINGLE_NODE"  && statMgr.getOwnStatus() == 'OK' ) {return }
      log.debug( job.jobId, '<<<< SyncNodes <<<<<<< ',statMgr.getOwnStatus(), job.task )
      logSyncDta( '<< '+statMgr.getOwnStatus() + ' <<< SyncNodes <<< ('+ job.task.from+')', job.task )
      // log.info( 'jobProcessor >>>>', job.task.nodes )
      await updNode( job.task.nodes, job.task.nodeIdMap )
      await db.updateDbTree( job.jobId, job.task.db )
    break

    case 'AnnounceTokenReplica':
      log.info( job.jobId, 'jobProcessor >>>> AnnounceTokenReplica', job.task )
      await statMgr.addTokenReplica( job.task.tokens , job.task.nodeId )
    break

    case 'StartOnboarding':
      if ( job.task.podName === cfg.OWN_NODE_ADDR ) {
        log.info( job.jobId, 'jobProcessor >>>>>>>>>>>>>>>>>> StartOnboarding', job.task )
        await statMgr.setOnboardingStatus( job.task.nodeId )
        await broadcastNodeUpdate()
      }
    break

    case 'TransferTokenData':
      if ( job.task.fromNode === myNodeId ) { // my node must receive token
        // log.info( 'jobDispatcher <<<<<<<<<<<<<<<<', job.jobType, JSON.stringify( job.task ) )
        await db.createTransferTokenDataJobs( job.task )
        await statMgr.pushTaskToQueue( job.task ) // remind me
      } else {
        log.info( job.jobId, 'jobDispatcher <<', job.jobType, JSON.stringify( job.task ) )
      }
    break

    case 'TransferData':
      log.info( job.jobId, 'jobDispatcher <<<<<<<<<<<<<<<< TransferData:', job, msgProp )
      if ( job.task.toNode === myNodeId ) { // my node must receive token
        await db.storeDataBatch( job.task )
      }
    break

    case 'SetTokenMaster':
      log.info( job.jobId, 'jobDispatcher <<<< ', job.jobType,JSON.stringify( job.task ) )
      if ( job.task.node ===  myNodeId ) {
        log.info( job.jobId, 'jobDispatcher <<<<<<<< ', job.jobType, JSON.stringify( job.task ) )
        await statMgr.pushTaskToQueue( job.task ) // remind me to do after all data received
      }
    break


    default:
      log.warn( job.jobId, 'jobDispatcher', 'Unknown job type:', job.jobType )
  }
}

// ============================================================================

async function onboardNode( podName ) {
  log.warn( 'onboardNode', podName )
  let newNodeId = genNextNodeId()
  await tokenMgr.tasksToAddNode( podName, newNodeId )
}

// ============================================================================
function getHelper() {
  return helper
}

function ownNodeAddr( ) {
  log.info( 'ownNodeAddr', cfg.OWN_NODE_ADDR )
  return cfg.OWN_NODE_ADDR
}

// ============================================================================
// here update calls are processed

async function updNode( nodes, nodeIdMap ) {
  await statMgr.setNodeIdMap( nodeIdMap )
  await statMgr.mergeNodes( nodes )

  // subscribe data updates
  await pubsub.subscribeDataBroadcasts( db.processQueuedDtaUpd )

  let ownTokens = statMgr.getOwnMasterAndReplicaTokens() 
  for ( let token of ownTokens ) {
    await pubsub.subscribeDataUpdates( token, db.processQueuedDtaUpd )
  }
  
  return statMgr.getClusterNodes()
}


// ============================================================================

// function handoverToken( toNode ) {
//   return statMgr.handoverToken( toNode )
// }

// function handoverTokenStatus( node, status ) {
//   return statMgr.handoverTokenStatus( node, status  )
// }

function getAllNodeNames( ) {
  let nodes = helper.getNodeNamesSorted() 
  log.debug( 'getAllNodeNames', nodes )
  return nodes
}

function getNodes( token ) {
  let t = token.toLowerCase()
  let node = helper.tokenMap()
  log.debug( 'getNodes tokenMap', node )
  let nodes = []
  for ( let i = 0; i < cfg.DATA_REPLICATION; i++ ) {
    nodes.push( { t: t, node: node[ t ] } )
    t = tokenMgr.nextToken( t )
  }
  log.debug( 'getNodes tokenMap', token, nodes )
  return nodes
}

function genNextNodeId( ) {
  let  maxId = -1
  let stat = statMgr.getOwnNodeStatus()
  for ( let id in stat.nodeIdMap ) {
    let nId = parseInt( id )
    log.info( '>> onboardNode nId', nId  )
    if ( nId > maxId ) { maxId =  nId }
    log.info( '>> onboardNode nId', nId  )
  }
  let newNodeId = maxId + 1
  log.info( '>> onboardNode maxId=', maxId )
  return newNodeId + ''
}


// ============================================================================

async function broadcastNodeUpdate( options = {}) {
  if ( cfg.MODE == "SINGLE_NODE" ) {
    if ( statMgr.getOwnStatus() == 'OK' ) { 
      if ( fastStartInterval ) {
        clearInterval( fastStartInterval )
        fastStartInterval = null
      }
      return 
    }
    log.info( 'SINGLE_NODE broadcastNodeUpdate', statMgr.getOwnStatus() )
  }
  log.debug( 'broadcastNodeUpdate...' )
  try {

    let syncData = await statMgr.getSyncData()
    syncData.jobId = db.getTxnId( 'SYNC' )
    logSyncDta( '>>> SEND SyncNodes >>>', syncData )
    await pubsub.sendJob( 'SyncNodes', syncData, false )

    // still no tokens generated ?
    if ( timeForClusterStartup() && ! options.terminate ) {
      await runClusterStartup()
    }

    let own = statMgr.getOwnNodeStatus()
    //log.info( 'syncData', syncData.db.admin )
    if ( own.status == 'OK' && syncData.db.admin && syncData.db.admin .c['api-metrics'] ) {
      if ( ! initDone ) {
        log.info( 'Init metrics')
        loadMetrics()  
      }
    }
    // if ( own.status == 'OK' && checkAdminCollectionOK ) {
    //     // dbTaskArr.push( 'ChkAdminDB' )
    //     checkAdminCollectionOK = false
    // }
  } catch ( exc ) { log.fatal( 'broadcastNodeUpdate', exc ) }
}

// let checkAdminCollectionOK = true

// ===========================================================================

function timeForClusterStartup( ) {
  if ( cfg.MODE == "SINGLE_NODE" &&  statMgr.allNodesInSyncingStat() ) { return true }
  let timeInSyc = Date.now() - statMgr.getStatusSince()
  // log.info( 'timeForClusterStartup', timeInSyc, statMgr.getStatusSince(), statMgr.allNodesInSyncingStat() )
  if ( statMgr.allNodesInSyncingStat()  &&  ( timeInSyc > 10000 ) ) { 
    if ( statMgr.isFirstClusterNode() ) {
      return true
    } else {
      log.info( 'timeForClusterStartup', 'NOT 1st node ... waiting' )
    }
  }
  return false
}

async function runClusterStartup() {
  // welcome new cluster :-) we generate token for all nodes now 
  log.info( 'Gen Token >>>>>>>>>>>>>>>>>>>>' )
  helper.distributeTokensToNodes( )

  let syncData = await statMgr.getSyncData()
  syncData.jobId = db.getTxnId( 'SYNC' )
  logSyncDta( '>>> SEND start SyncNodes >>>', syncData )
  pubsub.sendJob( 'SyncNodes', syncData, false )
  
  dbTaskArr.push( 'initAdminDB' )
  // dbTaskArr.push( 'initDemoDB' )
}

function logSyncDta( inf, syncData ) {
  try {
    if ( process.env.LOG_SYNC ) {
      let outStr = ''
      for ( let node in syncData.nodes ) {
        outStr += node +' '+ syncData.nodes[node].status +'  '
      }
      outStr += JSON.stringify( syncData.nodeIdMap )
      log.info( inf, outStr )
    }      
  } catch (error) {
    log.error( 'logSyncDta', error )
  }
}


let dbTaskArr = []

function getDbTask() {
  let tasks = JSON.parse( JSON.stringify( dbTaskArr ) )
  dbTaskArr = []
  return tasks
}

// ===========================================================================
const API_METRICS = ['GET','DELETE','PUT','POST','sync','QMSGIN','QMSGOUT','QJOBIN','QJOBOUT']
function getApiMetricKeys() {
  return API_METRICS
}

let apiMetrics = {}

function getApiMetrics() {
  return apiMetrics
}

function getApiMetricsForTS( ts ) {
  return apiMetrics[ ts ]
}

function setApiMetric( metric ) {
  apiMetrics = metric
}

function setApiMetricsForTS( ts, metric  ) {
  apiMetrics[ ts ] = metric
}

function incApiMetricsForTS( ts, idx  ) {
  apiMetrics[ ts ][ idx ] ++
}

async function incApiMetric( idx ) {
  // log.info( 'incApiMetric', idx, JSON.stringify( apiMetrics ) )
  let ts = await prepTimestamp()
  if ( ! getApiMetricsForTS( ts ) ) {
    setApiMetricsForTS( ts, [ 0,0,0,0,0,0,0,0,0 ] )
  }
  incApiMetricsForTS( ts, idx )
}


let initDone = false 
let insertMetric = true
let needLoadMetric = true


async function prepTimestamp() {
  // load after re-start:
  let ownStatus = statMgr.getOwnNodeStatus()
  if ( ! ownStatus ) { return 0 }
  // if ( ! initDone && ownStatus.status == 'OK' ) {
  //   await  loadMetrics()
  // }
  let timestamp = Math.floor( Date.now() / 60000 ) 
  if ( ! getApiMetricsForTS( timestamp ) ) {
    setApiMetricsForTS( timestamp, [ 0,0,0,0,0,0,0,0,0 ] )

    // if ( ownStatus.status == 'OK' ) {
    //   // save metrics
    //   await saveMetrics()
    // }
  }
  return timestamp
}


let metricId = null

function dbInitDone() {
  insertMetric = true
  initDone = true
}

function isInitOK() {
  return initDone
}

async function saveApiMetric() {
  log.debug( 'saveApiMetric...' )
  if ( ! await manageDB.getColl( 'admin', 'api-metrics' ) ) { return }
  let ownStatus = statMgr.getOwnNodeStatus()
  if ( ownStatus?.status == 'OK' ) {
    if ( needLoadMetric ) {
      await loadMetrics()
      needLoadMetric = false
    }
    saveMetrics()
  }
}

async function loadMetrics() {
  if ( ! await manageDB.getColl( 'admin', 'api-metrics' ) ) { return }
  try {
    log.info( 'loadMetrics...' )
    initDone = true
    let metricsResult = await db.findOneDoc( {
      db :  'admin', 
      coll : 'api-metrics',
      txnId  :db.getTxnId( 'MXL' ),
      dt : Date.now(),
    }, { podName: ownNodeAddr() } ) 
    if ( metricsResult.doc  ) { 
      log.debug( '>>> loadMetrics', metricsResult )
      setApiMetric( metricsResult.doc.apiMetrics )
      metricId = metricsResult.doc._id
      insertMetric = false
    } 
    initDone = true
  } catch ( exc ) { log.warn( 'loadMetrics find', exc.message )}
}


async function saveMetrics() {
  if ( ! initDone ) { return }
  try {  
    let ownTokens = statMgr.getOwnTokens()
    let nodeStatus = statMgr.getOwnNodeStatus()
    let tokenStr = ''
    for ( let tkn in ownTokens ) {
      tokenStr += tkn + ' '
    }
    tokenStr = tokenStr.trim()
    if ( insertMetric ) {
      log.debug( 'saveMetrics >>> insert...' )
      let result = await db.insertOneDoc(
        { 
          db    : 'admin', 
          coll  : 'api-metrics',
          txnId :  db.getTxnId( 'MXI' )
        },
        { 
          podName: ownNodeAddr(),
          status : nodeStatus.status,
          tokens : tokenStr,
          nodeId : statMgr.ownNodeId(),
          apiMetrics : getApiMetrics(),
          keys       : getApiMetricKeys()
        } 
      ) 
      insertMetric = false
    } else {
      log.debug( 'saveMetrics >>> update' )

      // clean up old metrics
      let minTS = Math.floor(  Date.now()  / 60000 ) - 24*60
      let apiMetrics = getApiMetrics()
      for ( let ts in apiMetrics ) {
        if ( parseInt( ts ) < minTS ) {
          delete apiMetrics[ ts ]
        } 
      }

      await db.updateOneDoc( 
        { 
          db    : 'admin', 
          coll  : 'api-metrics',
          update: { 
            $set: { 
              apiMetrics : apiMetrics,
              status : nodeStatus.status,
              tokens : tokenStr,
              nodeId : statMgr.ownNodeId()

            }
          },
          txnId :  db.getTxnId( 'MXU' )
        },
        { _id : metricId } 
      )
      //log.info( '>>> update', upp )
    }

  } catch ( exc ) { log.warn( 'prepTimestamp', exc )}
}



// function randomChar( len ) {
//   var chrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
//   var token =''
//   for ( var i = 0; i < len; i++ ) {
//     var iRnd = Math.floor( Math.random() * chrs.length )
//     token += chrs.substring( iRnd, iRnd+1 )
//   }
//   return token
// }
