import pandas as pd
from infrastructure.detectors.zscore import ZScoreDetector
from infrastructure.detectors.mad import MADDetector
from infrastructure.detectors.rolling import RollingDetector

def test_zscore_constant():
    s = pd.Series([1.0] * 10)
    det = ZScoreDetector(threshold=2.0)

    res = det.detect(s)
    assert res.severity == 0
    assert len(res.anomalies) == 10
    assert (res.anomalies == 0).all()

def test_zscore_outlier():
    s = pd.Series([1.0] * 9 + [10.0])
    det = ZScoreDetector(threshold=2.0)

    res = det.detect(s)
    assert res.severity > 0
    assert res.anomalies.iloc[-1] > 0

def test_mad_outlier():
    s = pd.Series([1.0] * 9 + [100.0])
    det = MADDetector(threshold=3.5)

    res = det.detect(s)
    assert res.severity > 0
    assert res.anomalies.iloc[-1] > 0

def test_rolling_detects_outlier():
    s = pd.Series([1.0] * 10 + [10.0])
    det = RollingDetector(window=3, z_threshold=2.0)

    res = det.detect(s)
    assert res.severity > 0
    assert res.anomalies.iloc[-1] > 0