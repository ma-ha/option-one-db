const cfgHlp    = require( './config' )
const { MeterProvider, PeriodicExportingMetricReader } = require('@opentelemetry/sdk-metrics')
const { OTLPMetricExporter } = require('@opentelemetry/exporter-metrics-otlp-proto')

module.exports = {
  init,
  getMonitoringCfg,
  count
}

let counters = {}
let meterProvider = null
let meter = null

let cfg = {
  OTEL_SVC_NAME : null,
  DB_POD_NAME: null,
  OTEL_COLLECTOR_URL : null, // 'http://localhost:9090/api/v1/otlp/v1/metrics',
  OTEL_EXPORTER_MS: 1000
}

function init( configParams ) {
  cfgHlp.setConfig( cfg, configParams )
  if ( ! cfg.OTEL_COLLECTOR_URL ) { return }
  console.log( 'Init Metric Exporter', cfg.OTEL_COLLECTOR_URL )

  const exporter = new OTLPMetricExporter({
    url: cfg.OTEL_COLLECTOR_URL,
    concurrencyLimit: 1
  })

  meterProvider = new MeterProvider({
    readers: [
      new PeriodicExportingMetricReader({
        exporter,
        exportIntervalMillis: cfg.OTEL_EXPORTER_MS
      })
    ]
  })
  if ( ! cfg.OTEL_SVC_NAME ) {
    cfg.OTEL_SVC_NAME = ( cfg.DB_POD_NAME ?  cfg.DB_POD_NAME : require('os').hostname() )
  }
  meter = meterProvider.getMeter( cfg.OTEL_SVC_NAME )
}

function getMonitoringCfg() {
  if ( ! cfg.OTEL_COLLECTOR_URL ) { 
    return { metricsEnabled: false }
  } else {
    return { metricsEnabled: true }
  }

}


function getCounter( name ) {
  if ( ! counters[ name ] ) {
    counters[ name ] = meter.createCounter( name )
  }
  return counters[ name ] 
}


function count( metricName ) {
  if ( meterProvider ) {
    const counter = getCounter( metricName )
    counter.add( 1, { 'service_name': cfg.OTEL_SVC_NAME } )
  }
}