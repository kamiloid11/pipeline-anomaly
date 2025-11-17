from dataclasses import dataclass
import os
from types import SimpleNamespace
from typing import Optional, Any, Dict

def _env(name: str, default=None):
    v = os.getenv(name)
    return v if v is not None else default

def _env_int(name: str, default: Optional[int]):
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

settings = SimpleNamespace(
    clickhouse_host = _env("CLICKHOUSE_HOST", "clickhouse"),
    clickhouse_port = _env_int("CLICKHOUSE_PORT", 9000),
    clickhouse_http_port = _env_int("CLICKHOUSE_HTTP_PORT", 8123),
    clickhouse_user = _env("CLICKHOUSE_USER", "pipeline"),
    clickhouse_password = _env("CLICKHOUSE_PASSWORD", "pipeline_pass"),
    clickhouse_database = _env("CLICKHOUSE_DATABASE", "pipeline"),
    prometheus_port = _env_int("PROMETHEUS_PORT", None),
    detect_method = _env("DETECT_METHOD", "isolation"),
)

@dataclass
class PipelineConfig:
    clickhouse_host: str = settings.clickhouse_host
    clickhouse_port: int = settings.clickhouse_port
    clickhouse_http_port: int = settings.clickhouse_http_port
    clickhouse_user: str = settings.clickhouse_user
    clickhouse_password: str = settings.clickhouse_password
    clickhouse_database: str = settings.clickhouse_database
    clickhouse_table: str = "timeseries"
    prometheus_port: Optional[int] = settings.prometheus_port
    detect_method: str = settings.detect_method

def load_pipeline_config(path: Optional[str] = None) -> SimpleNamespace:
    default = SimpleNamespace(detectors=[], raw={})
    if not path:
        return default

    if not os.path.exists(path):
        return default

    try:
        import yaml  
    except Exception:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                raw = f.read()
            return SimpleNamespace(detectors=[], raw=raw)
        except Exception:
            return default

    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f) or {}
        detectors = data.get("detectors", [])
        return SimpleNamespace(detectors=detectors, raw=data)
    except Exception:
        return default