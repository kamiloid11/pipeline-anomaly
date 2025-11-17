from typing import Dict
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from .config import settings

def detect_anomalies_isolation(df: pd.DataFrame, contamination=0.01) -> pd.DataFrame:
    df = df.copy()
    df['anomaly'] = 0

    for sid, g in df.groupby('sensor_id'):
        X = g['value'].astype(float).values.reshape(-1, 1)
        if len(X) < 10:
            continue
        clf = IsolationForest(contamination=contamination, random_state=42)
        preds = clf.fit_predict(X)
        
        mask = (preds == -1)
        df.loc[g.index, 'anomaly'] = mask.astype(int)

    return df

def _modified_z_scores(vals: np.ndarray) -> np.ndarray:
    vals = np.asarray(vals, dtype=float)
    median = np.median(vals)
    abs_dev = np.abs(vals - median)
    mad = np.median(abs_dev)
    if mad == 0:
        std = vals.std(ddof=0)
        if std == 0:
            return np.zeros_like(vals)
        return (vals - vals.mean()) / std
    return 0.6745 * (vals - median) / mad


def detect_anomalies_zscore(df: pd.DataFrame, z_thresh: float = 3.5) -> pd.DataFrame:
    df = df.copy()
    df['anomaly'] = 0

    for sid, g in df.groupby('sensor_id'):
        if len(g) < 2:
            continue
        vals = g['value'].astype(float).values
        mz = _modified_z_scores(vals)
        mask = np.abs(mz) > z_thresh
        df.loc[g.index, 'anomaly'] = mask.astype(int)

    return df

def anomaly_stats(df: pd.DataFrame) -> Dict[str, float]:
    total = len(df)
    an = int(df['anomaly'].sum()) if 'anomaly' in df else 0
    return {'total_rows': total, 'anomalies': an, 'anomaly_rate': an / total if total else 0.0}

def detect_anomalies(df: pd.DataFrame, contamination=0.01) -> pd.DataFrame:
    
    method = getattr(settings, 'detect_method', 'isolation')
    if method == 'zscore':
        return detect_anomalies_zscore(df)
    return detect_anomalies_isolation(df, contamination=contamination)