CREATE DATABASE IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.timeseries
(
    ts DateTime,
    sensor_id String,
    value Float64
)
ENGINE = MergeTree()
ORDER BY ts;

CREATE DATABASE IF NOT EXISTS anomaly_demo;

CREATE TABLE IF NOT EXISTS anomaly_demo.anomaly_reports
(
    generated_at DateTime DEFAULT now(),
    window_start DateTime,
    window_end DateTime,
    severity Float64,
    severity_level String,
    anomalies_count UInt64
)
ENGINE = MergeTree()
ORDER BY (generated_at);
