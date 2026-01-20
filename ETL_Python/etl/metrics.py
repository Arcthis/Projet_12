from prometheus_client import Counter, Histogram, Gauge, start_http_server

def start_metrics_server(port=8050):
    start_http_server(port)

ETL_RUNS = Counter(
    "etl_run_total",
    "Total ETL runs",
    ["status"]
)

ETL_CYCLE_DURATION = Histogram(
    "etl_cycle_duration_seconds",
    "Duration of one ETL cycle"
)

ETL_LINES_PROCESSED = Gauge(
    "etl_lines_processed",
    "Number of valid lines processed in last run"
)

ETL_LINES_INVALID = Gauge(
    "etl_lines_invalid",
    "Number of invalid lines in last run"
)

ETL_STEP_ERRORS = Gauge(
    'etl_step_errors_total', 
    'Errors per ETL step',
    ['step']
)

ETL_LINES_NON_ELIGIBLE = Gauge(
    "etl_lines_non_eligible", 
    "Number of lines processed but not eligible for prime"
)

ETL_NON_ELIGIBLE_REASON = Gauge(
    "etl_non_eligible_reason", 
    "Number of non-eligible lines per reason", 
    ['reason']
)

ETL_INVALID_GEOCODE = Gauge(
    "etl_invalid_geocode_total",
    "Number of addresses that could not be geocoded (latitude/longitude = 9999)"
)