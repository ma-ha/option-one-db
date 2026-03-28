const cfgHlp   = require( '../helper/config' )
const log      = require( '../helper/logger' ).log
const metric   = require( '../helper/logger' ).countMetric
const statMgr  = require( './node-status-mgr' ) 
const tokenMgr = require( './token-mgr' )
const pubsub   = require( './pubsub' )
const db       = require( '../db-engine/db' )
const manageDB = require( '../db-engine/db-mgr' )
const backup   = require( '../db-engine/db-backup' )
const metrics  = require( './node-api-metrics' )

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

  getApiMetricKeys   : metrics.getApiMetricKeys,
  getApiMetrics      : metrics.getApiMetrics,
  getApiMetricsForTS : metrics.getApiMetricsForTS,
  setApiMetric       : metrics.setApiMetric,
  setApiMetricsForTS : metrics.setApiMetricsForTS,
  incApiMetric       : metrics.incApiMetric,
  incApiMetricsForTS : metrics.incApiMetricsForTS,
  prepTimestamp      : metrics.prepTimestamp,
  loadMetrics        : metrics.loadMetrics,
  saveMetrics        : metrics.saveMetrics
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
let startBackupInterval   = null 

async function init( configParams, options ) { 
  cfgHlp.setConfig( cfg, configParams )
  
  // on Mac the DB_POD_NAME must be defined per process instance
  let DB_POD_NAME = ( cfg.DB_POD_NAME ?  cfg.DB_POD_NAME : require('os').hostname() )
  cfg.OWN_NODE_ADDR = DB_POD_NAME +':'+ cfg.PORT + cfg.API_PATH

  log.info( 'Init Cluster / OWN_NODE_ADDR', cfg.OWN_NODE_ADDR )

  metrics.init( cfg )
  helper = await statMgr.init( cfg )
  tokenMgr.init()
  if ( options && options.testingOnly ) { return }
  
  await pubsub.init( cfg.OWN_NODE_ADDR, cfg,  metrics.incApiMetric, jobDispatcher )
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
  await metrics.terminate()
  await broadcastNodeUpdate()
  await pubsub.terminate()
  await db.terminate()
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
      await updNode( job.task )
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
        await db.creTransferDataJobs( job.task )
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
// here "update" calls are processed

async function updNode( task ) {
  const fromNode = task.from
  const nodes = task.nodes
  const nodeIdMap = task.nodeIdMap

  await statMgr.setNodeIdMap( nodeIdMap )
  statMgr.updateLastSeen( fromNode )
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
        metrics.loadMetrics()
        // if ( ! cfg.MODE == "SINGLE_NODE" ) {
          db.startConsistencyChecks( statMgr.ownNodeId(), statMgr.getOwnTokens() )
        // }
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

let initDone = false

function dbInitDone() {
  metrics.dbReady()
  initDone = true
}

function isInitOK() {
  return initDone
}