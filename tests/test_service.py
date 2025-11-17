import pandas as pd, os
from infrastructure.repository.clickhouse_stub import ClickHouseRepositoryStub
from infrastructure.detectors.zscore import ZScoreDetector
from application.service import AnomalyDetectionService

def test_service_end_to_end(tmp_path):
    repo_root = str(tmp_path)
    data_dir = os.path.join(repo_root, 'data')
    os.makedirs(data_dir, exist_ok=True)
    df = pd.DataFrame({
        'timestamp': pd.date_range('2025-01-01', periods=10, freq='T'),
        'value': [1] * 9 + [100]
    })

    csv_path = os.path.join(data_dir, 'sample.csv')
    df.to_csv(csv_path, index=False)
    repo = ClickHouseRepositoryStub(repo_root=repo_root)
    det = ZScoreDetector(threshold=2.0)
    svc = AnomalyDetectionService(repository=repo, detectors=[det], sinks=[])
    report = svc.run_once()

    assert report is not None
    assert isinstance(report.severity, float)
    assert report.severity > 0
    assert len(report.anomalies) >= 1