
from __future__ import annotations
import pandas as pd
import numpy as np
from domain.models import DetectionResult

class ZScoreDetector:
    def __init__(self, threshold: float = 3.0, name: str | None = None):
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
        s = pd.to_numeric(s, errors="coerce").fillna(0.0)
        return s

    def detect(self, series_or_df) -> DetectionResult:
        s = self._to_series(series_or_df)
        if s.empty:
            return DetectionResult(anomalies=pd.Series(dtype=float), severity=0.0)

        mean = s.mean()
        std = s.std(ddof=0)

        anomalies = pd.Series(0.0, index=s.index, name="severity")

        for i, x in s.items():
            abs_dev = abs(x - mean)

            if std < 1e-6:
                if abs_dev > self.threshold:
                    anomalies[i] = abs_dev
                continue

            z = abs_dev / std

            if z > self.threshold or abs_dev > self.threshold:
                anomalies[i] = z

        severity = float(anomalies.sum())
        return DetectionResult(anomalies=anomalies, severity=severity)