from typing import Any
from domain.models import AnomalyReport
import json, datetime

class StdOutAlertSink:
    def send(self, report: AnomalyReport) -> None:
        print('--- ALERT ---')
        print(f'window: {report.window_start} -> {report.window_end}')
        print(f'severity: {report.severity} ({report.severity_level})')
        print(f'anomalies: {len(report.anomalies)}')
        
        print(json.dumps({'window_start': report.window_start.isoformat(),
                          'window_end': report.window_end.isoformat(),
                          'severity': report.severity,
                          'severity_level': report.severity_level,
                          'anomalies_count': len(report.anomalies)}, default=str))
