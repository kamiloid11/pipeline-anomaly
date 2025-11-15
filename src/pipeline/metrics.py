from prometheus_client import Counter, Gauge, Histogram

rows_processed = Counter("pipeline_rows_processed", "Total rows processed")
anomalies_detected = Counter("pipeline_anomalies_detected", "Total anomalies detected")
anomaly_rate_gauge = Gauge("pipeline_anomaly_rate", "Last run anomaly rate")
run_duration = Histogram("pipeline_run_duration_ms", "Processing duration in ms")