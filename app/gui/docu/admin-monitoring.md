# Set up monitoring

Configuration:

To enable the Open Telemetry metrics exporter the `OTEL_COLLECTOR_URL` must be configured.

E.g.

    export OTEL_COLLECTOR_URL="http://localhost:9090/api/v1/otlp/v1/metrics"

Additionally you can define the service name, otherwise the HOSTNAME will be used, e.g.:

    export OTEL_SVC_NAME="OPTION-ONE-DB"

# Metrics

As soon as the `OTEL_COLLECTOR_URL` is configured metrics are sent to the metric collector

## Database Status

Each database process sends the DB status every 10 sec. 

The healthy status is `DB_STAT_OK_total`. All other status should not be sent for a long time.

Hint: In Prometheus the query  should show a flat line:
- `count_over_time(DB_STAT_OK_total[1m])`

## Database Errors

Metrics:
- `LOG_WARN_total`
- `LOG_ERROR_total`
- `LOG_FATAL_total`

Warnings can appear at DB startup. 
In case of ERROR or FATAL you should immediately check the logs and fix the problem.

## API Metrics

API requests are counted in the `API_REQ_total` metric.