const cfgHlp   = require( '../helper/config' )
const log      = require( '../helper/logger' ).log
const logger   = require( '../helper/logger' )
const helper   = require( './db-helper' )

const dbDocCre  = require( './db-doc-cre' )
const dbDocFind = require( './db-doc-find' )
const dbDocUpd  = require( './db-doc-upd' )
const persistence = require( './db-persistence' )

let nodeMgr = null

module.exports = {
  init,
  terminate,
  addDbMetric
}

// ============================================================================

let cfg = {
  ERR_LOG_EXPIRE_DAYS : 31
}
let persistMetricsInterval = null
let persistErrLogsInterval = null


async function init( configParams, nodeMgrObj ) {
  log.info( 'Init DB ...')
  cfgHlp.setConfig( cfg, configParams )
  nodeMgr = nodeMgrObj 
  // persist db metrics
  persistMetricsInterval = setInterval( persistMetrics, 60000 + Math.floor( Math.random() * 10000 ) )
  persistErrLogsInterval = setInterval( persistErrLogs, 10000 ) // + Math.floor( Math.random() * 10000 ) )
}


async function terminate() {
  log.info( 'Terminate Metrics...' )
  try {
    clearInterval( persistMetricsInterval )
    clearInterval( persistErrLogsInterval )
    await persistMetrics()
    await persistErrLogs()
  } catch ( exc ) { log.info( 'Terminate DB', exc ) }
}

// ============================================================================

// async function updateOneDoc( r, doc, opt = {} ) {
//   if ( ! doc._id ) { return { _error: 'Require _id' } }
//   let docbyId = await persistence.getDocById( r.db, r.coll,  doc._id )
//   if ( docbyId._error ) { return docbyId }
//   let result = await dbDocUpd.updateOneDoc( r, doc, docbyId.doc )
//   return result
// }

// ============================================================================
let DB_METRICS = {}
let DB_METRICS_ID = {}
let metricChanged = false


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

//-----------------------------------------------------------------------------

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
        let find = await  dbDocFind.findOneDoc( metricsColl, { db : dbName } )
        // log.info( 'persistMetrics findOneDoc', find  )
        if ( ! find.doc ) {
          log.debug( 'persistMetrics insertOne', metricsUpd )
          let result = await dbDocCre.insertOneDoc( metricsColl, metricsUpd )
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
          // let result = await updateOneDoc( metricsColl, { _id: find.doc._id , db: dbName } ) 

          let result = await dbDocUpd.updateOneDoc( metricsColl, { _id: find.doc._id , db: dbName }, find.doc )
          if ( result._error ) {
            log.error( 'persistMetrics', result._error )
          } else {
            log.debug( 'persistMetrics updateOneDoc', result )
          }

        }
      // }
      // if ( DB_METRICS_ID[ dbName ] ) {
      //   metricsUpd._id =  DB_METRICS_ID[ dbName ] //<<<<<<<<<<<<<<<<<<<<
      //   await updateOneDoc( metricsColl, metricsUpd ) /// <<<<<<<<<<<<<<< FIX  
      // }
    }
  } catch ( exc ) { log.error( 'persistMetrics', exc ) }
}

//-----------------------------------------------------------------------------

async function persistErrLogs() {
  // log.info( 'persistErrLogs', logger.getErrLogs().length)
  if ( await persistence.getColl( 'admin', 'log' ) ) {
    for ( let log of logger.getErrLogs() ) {
      dbDocCre.saveErrLog( log )
    }
  }
}