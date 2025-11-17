import pandas as pd
from pipeline.etl import transform
from pipeline.anomaly import detect_anomalies_zscore, detect_anomalies_isolation

def test_transform_basic():
    df = pd.DataFrame({
        'ts': ['2025-01-01T00:00:00', '2025-01-01T00:01:00', '2025-01-01T00:02:00'],
        'sensor_id': [1, 1, 1],
        'value': ['1.0', '2.0', None]
    })
    out = transform(df)

    assert out['ts'].dtype.kind == 'M'
    assert out['value'].dtype.kind in ('f', 'i')
    assert len(out) == 3

def test_zscore_detector_detects_outlier():
    df = pd.DataFrame({
        'ts': ['2025-01-01T00:00:00', '2025-01-01T00:01:00', '2025-01-01T00:02:00'],
        'sensor_id': [1, 1, 1],
        'value': [1.0, 1000.0, 1.1]
    })
    t = transform(df)
    out = detect_anomalies_zscore(t, z_thresh=3.0)

    assert 'anomaly' in out.columns
    assert out['anomaly'].sum() >= 1


def test_isolation_detector_runs():
    df = pd.DataFrame({
        'ts': pd.date_range('2025-01-01', periods=20, freq='T'),
        'sensor_id': [1] * 20,
        'value': [1.0] * 19 + [100.0]
    })
    out = detect_anomalies_isolation(df, contamination=0.05)

    assert 'anomaly' in out.columns
    assert out['anomaly'].dtype.kind in ('i', 'f')