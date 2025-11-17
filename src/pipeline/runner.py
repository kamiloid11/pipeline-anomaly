import argparse
import logging
import time
import json
import os
import shutil
import glob
from pathlib import Path
from typing import Optional
from .etl import extract_from_csv, transform, load
from .db import write_timeseries
from .anomaly import detect_anomalies, anomaly_stats
from .config import settings

settings.clickhouse_host = os.environ.get("CLICKHOUSE_HOST", settings.clickhouse_host)

try:
    from . import metrics as metrics_module
except Exception:
    metrics_module = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger('pipeline')


def _make_dirs(path: str):
    Path(path).mkdir(parents=True, exist_ok=True)

def process_file(csv_path: str, archive_dir: Optional[str] = None):
    
    start = time.time()
    logger.info("Processing file: %s", csv_path)
    df = extract_from_csv(csv_path)
    df = transform(df)
    df = detect_anomalies(df)
    stats = anomaly_stats(df)
    duration_ms = int((time.time() - start) * 1000)
    rows = len(df)

    logger.info('Anomaly stats: %s', stats)
    logger.info('rows_processed=%d anomalies=%d anomaly_rate=%.4f duration_ms=%d',
                rows, stats.get('anomalies', 0), stats.get('anomaly_rate', 0.0), duration_ms)

    metrics = {
        "file": os.path.basename(csv_path),
        "rows_processed": rows,
        "anomalies": stats.get("anomalies", 0),
        "anomaly_rate": stats.get("anomaly_rate", 0.0),
        "duration_ms": duration_ms,
        "timestamp": int(time.time())
    }
    
    print(json.dumps({"pipeline_metrics": metrics}, ensure_ascii=False))

    logger.info("Load to ClickHouse")
    load(df[['ts', 'sensor_id', 'value']], write_timeseries)

    try:
        if metrics_module is not None and getattr(settings, "prometheus_port", None):
            metrics_module.set_metrics(metrics)
    except Exception:
        logger.debug("Prometheus metrics update failed", exc_info=True)

    if archive_dir:
        _make_dirs(archive_dir)
        basename = os.path.basename(csv_path)
        ts = int(time.time())
        dest = os.path.join(archive_dir, f"{basename}.processed.{ts}")
        try:
            shutil.move(csv_path, dest)
            logger.info("Moved processed file to %s", dest)
        except Exception as e:
            logger.exception("Failed to move file %s to archive %s: %s", csv_path, archive_dir, e)

    return metrics

def run_once(csv_path: str):
    
    if os.path.isdir(csv_path):
        
        files = sorted(glob.glob(os.path.join(csv_path, "*.csv")))
        if not files:
            logger.info("No CSV files found in %s", csv_path)
            return
        for f in files:
            process_file(f)
    else:
        process_file(csv_path)

def watch_directory(incoming_dir: str, archive_dir: str, poll_interval: int = 5):
    
    logger.info("Starting watch mode: incoming=%s archive=%s interval=%ds", incoming_dir, archive_dir, poll_interval)
    _make_dirs(incoming_dir)
    _make_dirs(archive_dir)

    try:
        if getattr(settings, "prometheus_port", None) and metrics_module is not None:
            metrics_port = settings.prometheus_port
            metrics_module.start_server(metrics_port)
            logger.info("Prometheus exporter started on port %s", metrics_port)
    except Exception:
        logger.exception("Failed to start Prometheus exporter")

    try:
        while True:
            files = sorted(glob.glob(os.path.join(incoming_dir, "*.csv")))
            if not files:
                time.sleep(poll_interval)
                continue
            for f in files:
                try:
                    process_file(f, archive_dir=archive_dir)
                except Exception:
                    logger.exception("Error processing file %s", f)
            
            time.sleep(0.5)
    except KeyboardInterrupt:
        logger.info("Watch mode stopped by user")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('cmd', choices=['run', 'watch'], help='command: run (one-off) or watch (daemon)')
    parser.add_argument('--csv', default='/app/data/sample.csv', help='input CSV file or directory (for run)')
    parser.add_argument('--incoming-dir', default='/app/data/incoming', help='incoming directory for watch mode')
    parser.add_argument('--archive-dir', default='/app/data/processed', help='archive directory for processed files (watch mode)')
    parser.add_argument('--interval', type=int, default=5, help='poll interval seconds for watch mode')
    args = parser.parse_args()

    if args.cmd == 'run':
        run_once(args.csv)
    elif args.cmd == 'watch':
        watch_directory(args.incoming_dir, args.archive_dir, poll_interval=args.interval)

if __name__ == '__main__':
    main()