# src/pipeline/db.py
import logging
import os
import requests
from .config import settings

logger = logging.getLogger("pipeline.db")

# Используем переменные окружения, fallback на настройки из config.py
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", settings.clickhouse_host)
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", settings.clickhouse_http_port)
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", settings.clickhouse_user)
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", settings.clickhouse_password)

CLICKHOUSE_URL = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}"


def write_timeseries(df):
    """
    Write timeseries dataframe with columns: ts, sensor_id, value
    via ClickHouse HTTP interface.
    """
    if df.empty:
        return

    # Convert df to TSV
    lines = []
    for _, row in df.iterrows():
        lines.append(f"{row['ts']}\t{row['sensor_id']}\t{row['value']}")
    payload = "\n".join(lines)

    query = "INSERT INTO pipeline.timeseries (ts, sensor_id, value) FORMAT TSV"
    url = f"{CLICKHOUSE_URL}/?query={query}"

    try:
        r = requests.post(url, data=payload.encode("utf-8"), auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD))
        r.raise_for_status()
    except requests.RequestException as e:
        logger.error("ClickHouse insert error: %s", e)
        raise RuntimeError(f"ClickHouse insert failed: {e}")
# src/pipeline/db.py
import logging
import os
import requests
from .config import settings

logger = logging.getLogger("pipeline.db")

# Используем переменную окружения CI, если она есть
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", settings.clickhouse_host)
CLICKHOUSE_URL = f"http://{CLICKHOUSE_HOST}:{settings.clickhouse_http_port}"


def write_timeseries(df):
    """
    Write timeseries dataframe with columns: ts, sensor_id, value
    via ClickHouse HTTP interface.
    """
    if df.empty:
        return

    # Convert df to TSV
    lines = []
    for _, row in df.iterrows():
        lines.append(f"{row['ts']}\t{row['sensor_id']}\t{row['value']}")
    payload = "\n".join(lines)

    query = "INSERT INTO pipeline.timeseries (ts, sensor_id, value) FORMAT TSV"
    url = f"{CLICKHOUSE_URL}/?query={query}"

    r = requests.post(
        url,
        data=payload.encode("utf-8"),
        auth=(settings.clickhouse_user, settings.clickhouse_password)
    )
    if r.status_code != 200:
        logger.error("ClickHouse insert error: %s", r.text)
        raise RuntimeError(f"ClickHouse insert failed: {r.text}")
