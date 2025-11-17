from __future__ import annotations
import pandas as pd
import numpy as np
from domain.models import DetectionResult

class MADDetector:
    def __init__(self, threshold: float = 3.5, name: str | None = None):
        self.threshold = threshold
        self.name = name or self.__class__.__name__

    def _to_series(self, obj):
        if isinstance(obj, pd.DataFrame):
            if "value" in obj.columns:
                s = obj["value"]
            else:
                num = obj.select_dtypes(include=[np.number]).columns
                if len(num) == 0:
                    return pd.Series(dtype=float)
                s = obj[num[0]]
        else:
            s = obj
        return pd.to_numeric(s, errors="coerce").fillna(0.0)

    def detect(self, series_or_df) -> DetectionResult:
        s = self._to_series(series_or_df)
        if s.empty:
            return DetectionResult(pd.Series(dtype=float), 0.0)

        med = s.median()
        abs_dev = (s - med).abs()
        mad = abs_dev.median()
        
        if mad == 0:
            anomalies = abs_dev.where(abs_dev > self.threshold, 0.0)
            anomalies.name = "severity"
            severity = float(anomalies.sum())
            return DetectionResult(anomalies, severity)

        robust_z = 0.6745 * abs_dev / mad
        anomalies = robust_z.where(robust_z > self.threshold, 0.0)
        anomalies.name = "severity"
        severity = float(anomalies.sum())
        return DetectionResult(anomalies, severity)