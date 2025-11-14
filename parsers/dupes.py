# -*- coding: utf-8 -*-
"""
controls/dupes.py – kandidater til duplikatbilag.
"""
from __future__ import annotations
import pandas as pd
from .common import to_num

def duplicate_candidates(tx: pd.DataFrame) -> pd.DataFrame:
    t = tx.copy(); to_num(t, ["Debit","Credit"])
    if {"VoucherNo","JournalID","PostingDate"}.issubset(t.columns):
        grp = t.groupby(["VoucherNo","JournalID","PostingDate"])[["Debit","Credit"]].sum().reset_index()
        grp["Net"] = (grp["Debit"] - grp["Credit"]).round(2)
        cnt = t.groupby(["VoucherNo","JournalID","PostingDate"]).size().reset_index(name="Lines")
        return (grp.merge(cnt, on=["VoucherNo","JournalID","PostingDate"], how="left")
                  .query("Lines>1")
                  .sort_values(["PostingDate","JournalID","VoucherNo"]))
    return pd.DataFrame(columns=["VoucherNo","JournalID","PostingDate","Debit","Credit","Net","Lines"])
