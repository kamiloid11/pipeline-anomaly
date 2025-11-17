from typing import List, Dict, Any

def build_detectors(config: List[Dict[str, Any]]):
    detectors = []
    for item in config:
        t = item.get('type')
        params = {k: v for k, v in item.items() if k != 'type'}

        if t == 'zscore':
            from .detectors.zscore import ZScoreDetector
            detectors.append(ZScoreDetector(**params))

        elif t == 'mad':
            from .detectors.mad import MADDetector
            detectors.append(MADDetector(**params))

        elif t == 'lof':
            from .detectors.lof import LOFDetector
            detectors.append(LOFDetector(**params))

        elif t == 'rolling':
            from .detectors.rolling import RollingDetector
            
            if "threshold" in params:
                params["z_threshold"] = params.pop("threshold")
            detectors.append(RollingDetector(**params))

    return detectors