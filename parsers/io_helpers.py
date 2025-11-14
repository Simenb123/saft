# app/parsers/io_helpers.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import pandas as pd

__all__ = ["read_csv_safe", "write_csv_no", "to_numeric_series", "parse_date_series"]

def read_csv_safe(path: Path | str, dtype: str | dict = "str") -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, dtype=dtype, encoding="utf-8-sig", sep=None, engine="python")
    except Exception:
        try:
            return pd.read_csv(p, dtype=dtype, encoding="utf-8-sig", sep=";")
        except Exception:
            return pd.read_csv(p, dtype=dtype, encoding="utf-8")

def write_csv_no(df: pd.DataFrame, path: Path | str) -> None:
    p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False, sep=";", encoding="utf-8-sig")

def to_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def parse_date_series(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")
