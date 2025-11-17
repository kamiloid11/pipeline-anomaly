from dataclasses import dataclass
from typing import Protocol, Dict, Any, Optional, List   
from datetime import datetime
import pandas as pd

@dataclass
class DetectionResult:
    
    anomalies: pd.Series
    severity: float

@dataclass
class Anomaly:
    index: int
    value: float
    score: float

@dataclass
class AnomalyReport:
    sensor_id: Optional[int]        
    window_start: Optional[datetime]
    window_end: Optional[datetime]
    anomalies: List[int]
    severity: float
    severity_level: str
    detector_stats: Dict[str, Dict[str, Any]]

class AnomalyDetector(Protocol):
    def detect(self, series_or_df):
        ...

class AlertSink(Protocol):
    def send(self, report: AnomalyReport) -> None:
        ...