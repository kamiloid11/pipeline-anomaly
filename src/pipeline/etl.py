import pandas as pd
from datetime import datetime
from .utils import ensure_datetime

def extract_from_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "ts" in df.columns:
        ts_col = "ts"
    elif "timestamp" in df.columns:
        ts_col = "timestamp"
        df = df.rename(columns={"timestamp": "ts"})
        ts_col = "ts"
    else:
        raise KeyError("Input DataFrame must contain either 'ts' or 'timestamp' column")

    df["ts"] = ensure_datetime(df["ts"])

    df["value"] = pd.to_numeric(df.get("value"), errors="coerce").fillna(0.0)

    if "sensor_id" not in df.columns:
        
        df["sensor_id"] = 0

    df = df.sort_values(["sensor_id", "ts"])

    df["value"] = df.groupby("sensor_id")["value"].ffill().fillna(0.0)

    return df

def load(df, client_writer):
    client_writer(df)