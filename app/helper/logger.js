const pjson       = require( '../package.json' )
const bunyan      = require( 'bunyan' )
// const ErrorBuffer = require( './log-err-buffer' )
const otelMeter   = require( './otel-metrics' )

function initLog( configParams ) {
  otelMeter.init( configParams )
}


let errorLogs = {
  logArr : []
}

let logLevel = ( process.env.LOG_LEVEL ? process.env.LOG_LEVEL : 'info' )

let log = bunyan.createLogger({ 
  name : pjson.name, 
  level : logLevel,
  streams: [
    {
      level: logLevel,
      stream: process.stdout
    },
    {
      level: logLevel,
      stream: new ErrorBuffer( errorLogs )
    }
  ]
})


function getErrLogs() {
  let logs = errorLogs.logArr
  errorLogs.logArr = []
  return logs
}


function countMetric( metricName ) {
  try {
    otelMeter.count( metricName )
  } catch ( exc ) { log.warn( 'countMetric', exc.message ) }
}

function getMonitoringCfg() {
  return otelMeter.getMonitoringCfg()
}


function ErrorBuffer( errorLogs ) {

  const write = function write( r ) {
    const record = typeof r === 'string' ? JSON.parse(r) : r

    if ( record.level >= 40 ) {
      let level = 'WARN'
      if ( record.level >= 60 ) {
        level = 'FATAL'
      } else if ( record.level >= 50 ) {
        level = 'ERROR'
      }
      countMetric( 'LOG_' + level )
  
      errorLogs.logArr.push({
        t: Date.now(),
        l: level,
        h: record.hostname,
        m: record.msg
      })
      // console.log( '>>', record )

    }
  };

  return { write };
};

module.exports = {
  initLog,
  log,
  getErrLogs,
  countMetric,
  getMonitoringCfg

}