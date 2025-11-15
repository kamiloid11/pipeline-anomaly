import pandas as pd
from datetime import datetime
from .utils import ensure_datetime

def extract_from_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # базовый трансформ — конвертируем ts, сортируем, заполняем NA
    df = df.copy()
    df['ts'] = ensure_datetime(df['ts'])
    df = df.sort_values(['sensor_id', 'ts'])
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['value'] = df['value'].ffill().fillna(0.0)
    return df


def load(df, client_writer):
    #client_writer — функция записи в БД
    client_writer(df)