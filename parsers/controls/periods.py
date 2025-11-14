# -*- coding: utf-8 -*-
"""
controls/periods.py – periodekompletthet o.l.
"""
from __future__ import annotations
import pandas as pd
from .common import period_ym

def period_completeness(tx: pd.DataFrame, dfrom, dto) -> pd.DataFrame:
    t = tx.copy()
    d = pd.to_datetime(t.get("PostingDate")).fillna(pd.to_datetime(t.get("TransactionDate")))
    t["YM"] = period_ym(d)
    months = pd.period_range(dfrom, dto, freq="M").astype(str).tolist()
    have = set(t["YM"].dropna().unique())
    return pd.DataFrame([{"PeriodYM":m, "HasTx": (m in have), "Missing": (m not in have)} for m in months])
