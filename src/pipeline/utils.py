import pandas as pd

def ensure_datetime(series):
    return pd.to_datetime(series, utc=False)