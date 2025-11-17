from __future__ import annotations
import pandas as pd
import numpy as np
from domain.models import DetectionResult

class RollingDetector:
    def __init__(self, window: int = 3, z_threshold: float = 2.0, name: str | None = None):
        self.window = int(window)
        self.z_threshold = float(z_threshold)
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

        rmean = s.rolling(self.window, min_periods=1).mean()
        rstd = s.rolling(self.window, min_periods=1).std(ddof=0)

        anomalies = pd.Series(0.0, index=s.index, name="severity")

        for i in s.index:
            m = rmean[i]
            std = rstd[i]
            x = s[i]

            abs_dev = abs(x - m)

            if std == 0 or std < 1e-6:
                if abs_dev > self.z_threshold:
                    anomalies[i] = abs_dev
                continue

            z = abs_dev / std

            if z > self.z_threshold or abs_dev > self.z_threshold:
                
                anomalies[i] = z if z > 0 else abs_dev

        severity = float(anomalies.sum())
        return DetectionResult(anomalies, severity)