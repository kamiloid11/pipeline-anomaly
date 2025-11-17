import pandas as pd
import numpy as np

try:
    from sklearn.neighbors import LocalOutlierFactor
    _SKLEARN_AVAILABLE = True
except Exception:
    _SKLEARN_AVAILABLE = False

class LOFDetector:
    name = 'lof'
    def __init__(self, n_neighbors: int = 20, contamination: float = 0.01):
        self.n_neighbors = int(n_neighbors)
        self.contamination = float(contamination)
        if not _SKLEARN_AVAILABLE:
            raise RuntimeError('sklearn is not available in environment')

    def detect(self, series: pd.Series) -> pd.Series:
        
        if series.empty:
            return pd.Series([], dtype=float)
        X = series.values.reshape(-1, 1)
        clf = LocalOutlierFactor(n_neighbors=self.n_neighbors, contamination=self.contamination)
        pred = clf.fit_predict(X)  
        
        lof_scores = -clf.negative_outlier_factor_

        minv = lof_scores.min()
        scores = pd.Series((lof_scores - minv), index=series.index)
        
        return scores