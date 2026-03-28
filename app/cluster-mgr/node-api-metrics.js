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
  dbReady,
  terminate,
  
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

let insertMetric = true
let needLoadMetric = true
let metricId = null

let saveApiMetricInterval = null

async function init( configParams ) { 
  log.info( 'Start node metrics...')
  cfgHlp.setConfig( cfg, configParams )
  saveApiMetricInterval = setInterval( saveApiMetric, 60*1000 )
}


function dbReady() {
  insertMetric = true
  initDone = true
}


async function terminate() {
  log.info( 'Terminate node metrics...')
  clearInterval( saveApiMetricInterval )
  await saveMetrics()
}

// ----------------------------------------------------------------------------

function ownNodeAddr( ) {
  log.info( 'ownNodeAddr', cfg.OWN_NODE_ADDR )
  return cfg.OWN_NODE_ADDR
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
          podName    : ownNodeAddr(),
          status     : nodeStatus.status,
          tokens     : tokenStr,
          nodeId     : statMgr.ownNodeId(),
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
              status     : nodeStatus.status,
              tokens     : tokenStr,
              nodeId     : statMgr.ownNodeId()

            }
          },
          txnId : db.getTxnId( 'MXU' )
        },
        { _id : metricId } 
      )
    }

  } catch ( exc ) { log.warn( 'prepTimestamp', exc )}
}
