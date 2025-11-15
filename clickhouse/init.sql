-- clickhouse/init.sql
CREATE DATABASE IF NOT EXISTS pipeline;

CREATE TABLE IF NOT EXISTS pipeline.timeseries (
    ts DateTime,
    sensor_id UInt32,
    value Float64
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (sensor_id, ts);

-- Создадим пользователя для приложения и дадим права на базу
-- Рекомендуется sha256_password; для dev можно использовать простой пароль.
CREATE USER IF NOT EXISTS pipeline IDENTIFIED WITH sha256_password BY 'pipeline_pass';

GRANT ALL ON pipeline.* TO pipeline;