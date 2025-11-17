import logging
import os
import requests
from requests.adapters import HTTPAdapter, Retry
from .config import settings

logger = logging.getLogger("pipeline.db")

def _resolve_clickhouse_host():
    env_host = os.environ.get("CLICKHOUSE_HOST")
    if env_host:
        return env_host
    if os.path.exists("/.dockerenv"):
        return settings.clickhouse_host
    return "localhost"

CLICKHOUSE_HOST = _resolve_clickhouse_host()
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT", settings.clickhouse_http_port)
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", settings.clickhouse_user)
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", settings.clickhouse_password)
CLICKHOUSE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"

def _http_session(retries: int = 3, backoff: float = 0.3) -> requests.Session:
    s = requests.Session()
    retry = Retry(total=retries, backoff_factor=backoff, status_forcelist=(500, 502, 503, 504))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def write_timeseries(df, timeout: int = 10):
    if df is None or df.empty:
        return

    lines = []
    for _, row in df.iterrows():
        lines.append(f"{row['ts']}\t{row['sensor_id']}\t{row['value']}")
    payload = "\n".join(lines).encode("utf-8")

    query = "INSERT INTO pipeline.timeseries (ts, sensor_id, value) FORMAT TSV"
    url = f"{CLICKHOUSE_URL}/?query={query}"

    session = _http_session()
    try:
        r = session.post(url, data=payload, auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD), timeout=timeout)
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error("ClickHouse insert error: %s (url=%s host=%s)", e, url, CLICKHOUSE_HOST)
        raise RuntimeError(f"ClickHouse insert failed: {e}")