from pydantic import BaseModel, Field
from typing import List, Dict, Any

class ClickHouseConfig(BaseModel):
    host: str = 'localhost'
    port: int = 8123
    username: str = 'default'
    password: str = ''
    database: str = 'anomaly_demo'

class DetectorConfig(BaseModel):
    type: str
    params: Dict[str, Any] = Field(default_factory=dict)

class PipelineConfig(BaseModel):
    clickhouse: ClickHouseConfig = ClickHouseConfig()
    window: Dict[str, str] = Field(default_factory=lambda: {'duration': '1h', 'step': '5m'})
    detectors: List[Dict[str, Any]] = Field(default_factory=list)
    sinks: List[Dict[str, Any]] = Field(default_factory=lambda: [{'type': 'stdout'}])
