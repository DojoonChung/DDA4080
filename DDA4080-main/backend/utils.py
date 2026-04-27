import math
from pathlib import Path

import numpy as np
import pandas as pd


def normalize_station_name(name: str) -> str:
    name = str(name).strip()
    if not name:
        return ""
    return name if name.endswith("站") else f"{name}站"


def safe_read_excel(path: Path) -> pd.DataFrame:
    return pd.read_excel(path)


def safe_read_csv(path: Path, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, **kwargs)


def safe_json_value(v):
    if v is None:
        return None

    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    if isinstance(v, (np.integer,)):
        return int(v)

    if isinstance(v, (np.floating,)):
        v = float(v)

    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        return v

    if isinstance(v, dict):
        return {k: safe_json_value(val) for k, val in v.items()}

    if isinstance(v, list):
        return [safe_json_value(x) for x in v]

    if isinstance(v, tuple):
        return [safe_json_value(x) for x in v]

    return v


def records(df: pd.DataFrame):
    if df is None or df.empty:
        return []
    df = df.replace([np.inf, -np.inf], np.nan)
    return safe_json_value(df.to_dict("records"))