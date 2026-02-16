const nodeMgr = require( './node-mgr' )
const log     = require( '../helper/logger' ).log
const countMetric  = require( '../helper/logger' ).countMetric
const db      = require( '../db-engine/db' )
const helper  = require( '../db-engine/db-helper' )

module.exports = {
  apiStats,
  getMetrics,
  getDbMetrics,
  getOwnMetrics,
  incMetric
}


async function getMetrics( req, res ) {
  log.debug( 'getMetrics...')
  let podStatLst = []
  try {
    podStatLst =  await getPodMetrics2( ) 
    log.debug( 'podStatLst', podStatLst )
  } catch ( exc ) {
    log.warn( 'getMetrics', exc  )
  }
  res.send( podStatLst )
}

async function getDbMetrics( req, res ) {
  log.debug( 'getDbMetrics...', req.xUserAuthz )
  try {
    let result = []
    let now = Math.floor( Date.now() / 60000 ) 

    let dbLst = await db.listDBs( )
    if ( dbLst.length == 0 ) {
      return res.send([{ name: 'Please create a first database and collection ...' }])
    }
    for ( let dbName of dbLst ) {
      if (  req.xUserAuthz[ '*' ] ||  req.xUserAuthz[ dbName ] ) {
        // && TODO fix
        let dbMetric = await db.findOneDoc( { db : 'admin', coll : 'db-metrics' }, { db : dbName } )
        // log.info( 'Metric', dbMetric )
        let dbColls = await db.getColl( dbName )
        for ( let collName of dbColls.collections ) {
          let dbColMetrics = { name : dbName +'/'+ collName }
          if ( ! dbMetric._error && dbMetric.doc.coll && dbMetric.doc.coll[ collName ] ) {
            dbColMetrics.reqDB = []
            for ( let op of ['fnd','ins','upd','del','err']) {
              let rec = { name: op, data: [] }
              for ( let i = 0; i > -24; i-- ) {
                let h1 = now + i * 60
                let h2 = now + i * 60 - 60
                let cnt = 0
                for ( let ts in dbMetric.doc.coll[ collName ] ) {
                  // log.info( '>>', ts, h1, h2,  (ts < h1) , (ts > h2) )
                  if ( ts < h1 && ts > h2 ) {
                    if ( dbMetric.doc.coll[ collName ][ ts ][ op ] )
                    cnt += dbMetric.doc.coll[ collName ][ ts ][ op ] 
                  }
                }
                rec.data.push([ i, cnt ])
              }
              dbColMetrics.reqDB.push( rec )
            }
          }
          result.push( dbColMetrics )
        }  
      }
    }
  // log.info( 'getDbMetrics...', JSON.stringify(result, null, '   ') )
    res.send( result )
  } catch ( exc ) {
    log.warn( 'getDbMetrics', exc  )
  }
}

async function getPodMetrics2( ) {
  try {
    let pods = nodeMgr.getAllNodeStatus()
    log.debug( 'getPodMetrics2 pods', pods ) 
    let podNameArr = []
    for ( let podName in pods ) { podNameArr.push( podName ) }
    podNameArr.sort()

    log.debug( 'getPodMetrics', podNameArr )
    let podMetrics = await db.find( 'admin', 'api-metrics', { } )    
    log.debug( 'getPodMetrics', podMetrics ) 


    let result = []
    for ( let podName of podNameArr ) {
    
      let metric = null
      if ( podMetrics.data ) {
        for ( let podMetric of podMetrics.data  ) {
          if ( podName == podMetric.podName ) {
            metric = podMetric
          }
        }
      }

      let nodeId =  pods[ podName ].nodeId
      if ( pods[ podName ].status == 'NEW' ) {
        nodeId = '<a href="cluster/add?podName='+podName+'">ADD</a>'
      }

      let tokens = ''
      for ( let t in pods[ podName ].token ) {
        tokens += t + ' '
      }

      result.push({
        nodeId  : nodeId,
        podName : podName,
        status  : pods[ podName ].status,
        tokens  : tokens,
        reqPM   : ( metric ? getReqTimeSeries( metric.apiMetrics ) : [] )
      })

    }
    log.debug( 'getPodMetrics', result )
    return result

        // if ( metric  ) { 
        //   let nodeId = metric.nodeId
        //   if ( nodeId == null || nodeId == 'null' ) {  // TODO: Why is nId a string ??
        //     nodeId = '<a href="cluster/add?podName='+podName+'">ADD</a>'
        //   }
        //   result.push({
        //     nodeId  : metric.nodeId,
        //     podName : podName,
        //     status  : pods[ podName ].status,
        //     tokens  : metric.tokens,
        //     reqPM   : getReqTimeSeries( metric.apiMetrics )
        //   })
        // } else {
        //   result.push({
        //     nodeId  : '<a href="cluster/add?podName='+podName+'">ADD</a>',
        //     podName : podName,
        //     status  : 'NEW',
        //     tokens  : '-',
        //     reqPM   : []
        //   })
        // }
    
  } catch ( exc ) {
    log.warn( 'getPodMetrics', exc  )
  }
  return []
}

const API_METRICS = nodeMgr.getApiMetricKeys()

async function getOwnMetrics( req, res ) {
  log.debug( 'getOwnMetrics...' )
  try {
    let node = nodeMgr.getOwnNodeStatus()
    let tokenStr = ''
    for ( let tk in node.token ) {  tokenStr += tk + ' ' }
    let apiMetrics = nodeMgr.getApiMetrics()
    // let reqTimeseries = []
    // for ( let metricName of API_METRICS) {
    //   reqTimeseries.push({
    //     name : metricName,
    //     data : getOneDayMetric( metricName, apiMetrics )
    //   })
    // }
    let nId = nodeMgr.ownNodeId()
    log.debug( 'getOwnMetrics nId', nId)
    if ( nId == null || nId == 'null' ) {  // TODO: Why is nId a string ??
      nId = '<a href="cluster/add?podName='+nodeMgr.ownNodeAddr()+'">ADD</a>'
    }
    let result = {
      nodeId  : nId,
      podName : nodeMgr.ownNodeAddr(),
      status  : node.status,
      tokens  : tokenStr.trim(),
      reqPM   : getReqTimeSeries( apiMetrics )
    }
    log.debug( 'getOwnMetrics', result )
    if ( res ) {
      res.send( result )
    } else {
      return result
    }
      
  } catch ( exc ) {
    log.warn( 'getPodMetrics', exc  )
  }
}


function getReqTimeSeries( apiMetrics ) {
  let reqTimeSeries = []
  for ( let metricName of API_METRICS) {
    reqTimeSeries.push({
      name : metricName,
      data : getOneDayMetric( metricName, apiMetrics )
    })
  }
  return reqTimeSeries
}

function getOneDayMetric( metricName, apiMetrics ) {
  let result = []
  let timestamp = Math.floor( Date.now() / 60000 )
  let metricIdx = API_METRICS.indexOf( metricName )

  for ( let i = 0; i < 24*60; i += 10 ) {
    
    let min = Number.MAX_VALUE
    let max = 0
    for ( let j = 0; j < 10; j ++ ) {
      let val = ( apiMetrics[ timestamp - i -j ] ? apiMetrics[ timestamp - i -j ][ metricIdx ] : 0 )
      if ( val < min ) { min = val }
      if ( val > max ) { max = val } 
    }
    result.push( [ Math.round( - i / 60 * 100 ) / 100       , max ] )
    // result.push( [ Math.round( - i / 6  0 * 100 ) / 100 + 0.1 , min ] )
  }
  return result
}



function apiStats( ) {
  log.info( 'apiStats', 'init' )
  return async function logger ( req, res, next ) {
    try {
      let txnId = helper.dbgStart( 'apiStats_logger' )
      if ( req.url == '/cluster/nodes' ) {
        await incMetric( 'sync' )
      } else {
        await incMetric( req.method )
        countMetric( 'API_REQ') // open telemetry .. if configured
      }
      helper.dbgEnd( 'apiStats_logger', txnId )
    } catch ( error ) {
      log.error( 'apiStats.logger', error )
    }
    next()
  }
}

async function incMetric( metricName ) {
  try {
    let metricIdx = API_METRICS.indexOf( metricName )
    nodeMgr.incApiMetric(  metricIdx )  
  } catch ( error ) {
    log.error( 'incMetric', error )
  }
}
