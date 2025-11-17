import pandas as pd
import os
from typing import Optional

class ClickHouseRepositoryStub:
    def __init__(self, repo_root: Optional[str] = None):
        self.repo_root = repo_root or '.'
        self.data_dir = os.path.join(self.repo_root, 'data')
        os.makedirs(self.data_dir, exist_ok=True)
        self.saved_report = None  

    def _find_latest_file(self) -> Optional[str]:
        files = [
            os.path.join(self.data_dir, f)
            for f in os.listdir(self.data_dir)
            if f.endswith('.csv')
        ]
        if not files:
            return None
        latest = max(files, key=os.path.getmtime)
        return latest

    def read_window(self, start=None, end=None) -> pd.DataFrame:
        latest = self._find_latest_file()
        if not latest:
            return pd.DataFrame()

        try:
            header = pd.read_csv(latest, nrows=0)
            cols = set(header.columns.tolist())
        except Exception:
            return pd.read_csv(latest)

        parse_dates = []
        if 'timestamp' in cols:
            parse_dates = ['timestamp']
        elif 'ts' in cols:
            parse_dates = ['ts']

        try:
            if parse_dates:
                df = pd.read_csv(latest, parse_dates=parse_dates)
            else:
                df = pd.read_csv(latest)
        except Exception:
            df = pd.read_csv(latest)

        
        if 'ts' in df.columns and 'timestamp' not in df.columns:
            df = df.rename(columns={'ts': 'timestamp'})

        df = df.reset_index(drop=True)
        return df

    def read_latest_window(self):
        return self.read_window()

    
    def load_timeseries(self) -> pd.DataFrame:
        df = self.read_window()

        if hasattr(df, "sensor_id"):
            delattr(df, "sensor_id")

        return df

    def persist_report(self, report):
        self.saved_report = report  

        out = os.path.join(self.data_dir, 'anomaly_reports.jsonl')
        os.makedirs(self.data_dir, exist_ok=True)

        with open(out, 'a', encoding='utf-8') as f:
            import json

            def default(o):
                if hasattr(o, 'isoformat'):
                    try:
                        return o.isoformat()
                    except Exception:
                        return str(o)
                if isinstance(o, dict):
                    return o
                return str(o)

            try:
                window_start = (
                    report.window_start.isoformat()
                    if getattr(report, 'window_start', None) is not None else None
                )
            except Exception:
                window_start = str(getattr(report, 'window_start', None))

            try:
                window_end = (
                    report.window_end.isoformat()
                    if getattr(report, 'window_end', None) is not None else None
                )
            except Exception:
                window_end = str(getattr(report, 'window_end', None))

            payload = {
                'window_start': window_start,
                'window_end': window_end,
                'severity': getattr(report, 'severity', None),
                'severity_level': getattr(report, 'severity_level', None),
                'anomalies_count': len(getattr(report, 'anomalies', []) or []),
                'detector_stats': getattr(report, 'detector_stats', {})
            }

            f.write(json.dumps(payload, default=default) + '\n')