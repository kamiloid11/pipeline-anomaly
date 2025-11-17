from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
import pandas as pd

from domain.models import (
    AnomalyReport,
    DetectionResult,
    AnomalyDetector,
    AlertSink,
)

logger = logging.getLogger(__name__)

class AnomalyDetectionService:
    def __init__(self, repository, detectors: List[AnomalyDetector], sinks: List[AlertSink] = None):
        self.repository = repository
        self.detectors = detectors
        self.sinks = sinks or []

    def _normalize_detector_result(self, det, raw):
        detector_name = getattr(det, "name", det.__class__.__name__)

        if isinstance(raw, DetectionResult):
            series = raw.anomalies if raw.anomalies is not None else pd.Series(dtype=float)
            severity = float(raw.severity or 0.0)
        
        elif isinstance(raw, pd.Series):
            series = raw
            severity = float(series.sum()) if not series.empty else 0.0
        
        elif isinstance(raw, list):
            if raw and isinstance(raw[0], dict):
                normalized = []
                for a in raw:
                    norm = dict(a)  
                    if "detector" not in norm:
                        norm["detector"] = detector_name
                    if "severity" not in norm:
                        
                        norm["severity"] = float(norm.get("value", 0.0) or 0.0)
                    normalized.append({
                        "timestamp": norm.get("timestamp"),
                        "severity": float(norm.get("severity") or 0.0),
                        "detector": norm.get("detector"),
                    })
                count = len(normalized)
                return normalized, sum(a["severity"] for a in normalized), count
            else:
                
                normalized = []
                for idx, v in enumerate(raw):
                    try:
                        sev = float(v)
                    except Exception:
                        sev = 0.0
                    if sev > 0:
                        normalized.append({"timestamp": idx, "severity": sev, "detector": detector_name})
                count = len(normalized)
                return normalized, sum(a["severity"] for a in normalized), count
        else:
            series = pd.Series(dtype=float)
            severity = 0.0

        anomalies_list = []

        if isinstance(series, pd.Series):
            for idx, sev in series.items():
                try:
                    sevf = float(sev)
                except Exception:
                    sevf = 0.0
                if sevf > 0:
                    anomalies_list.append({"timestamp": idx, "severity": sevf, "detector": detector_name})

        count = len(anomalies_list)
        return anomalies_list, float(severity), count

    def run_once(self, start: Optional[datetime] = None, end: Optional[datetime] = None) -> AnomalyReport:

        df = None
        for method in ("read_window", "read_latest_window", "load_timeseries"):
            fn = getattr(self.repository, method, None)
            if callable(fn):
                try:
                    if method == "read_window":
                        df = fn(start=start, end=end)
                    else:
                        df = fn()
                    break
                except TypeError:
                    
                    try:
                        df = fn()
                        break
                    except Exception:
                        continue
                except Exception:
                    logger.exception("Repository.%s failed", method)
                    continue

        if df is None:
            df = pd.DataFrame()
        
        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            report = AnomalyReport(
                sensor_id=None,
                window_start=start,
                window_end=end,
                anomalies=[],
                severity=0.0,
                severity_level="low",
                detector_stats={},
            )
            
            for s in self.sinks:
                try:
                    s.send(report)
                except Exception:
                    logger.exception("Sink %s failed", getattr(s, "__class__", s))
            
            if hasattr(self.repository, "persist_report"):
                try:
                    self.repository.persist_report(report)
                except Exception:
                    logger.exception("persist_report failed")
            return report

        sensor_id = None
        if "sensor_id" in df.columns:
            unique = df["sensor_id"].unique()
            sensor_id = int(unique[0]) if len(unique) == 1 else None

        all_anomalies = []
        total_severity = 0.0
        stats: Dict[str, Dict[str, Any]] = {}

        for det in self.detectors:
            try:
                raw = det.detect(df)
            except Exception as e:
                logger.exception("Detector %s failed: %s", getattr(det, "__class__", det), e)
                raw = None

            anomalies_list, severity, count = self._normalize_detector_result(det, raw)

            stats[det.__class__.__name__] = {"count": int(count), "severity": float(severity)}
            
            normalized = []
            for a in anomalies_list:
                ts = a.get("timestamp")
                
                if isinstance(ts, (int,)) and "timestamp" in df.columns:
                    try:
                        mapped = df.loc[int(ts), "timestamp"]
                        ts = mapped
                    except Exception:
                        pass
                normalized.append({"timestamp": ts, "severity": float(a.get("severity", 0.0)), "detector": a.get("detector")})
            all_anomalies.extend(normalized)
            total_severity += float(severity)

        if total_severity > 100:
            severity_level = "high"
        elif total_severity > 10:
            severity_level = "medium"
        else:
            severity_level = "low"

        report = AnomalyReport(
            sensor_id=sensor_id,
            window_start=start,
            window_end=end,
            anomalies=all_anomalies,
            severity=float(total_severity),
            severity_level=severity_level,
            detector_stats=stats,
        )

        for s in self.sinks:
            try:
                s.send(report)
            except Exception:
                logger.exception("Sink %s failed", getattr(s, "__class__", s))

        if hasattr(self.repository, "persist_report"):
            try:
                self.repository.persist_report(report)
            except Exception:
                logger.exception("persist_report failed")

        return report