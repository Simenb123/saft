# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
from .common import load_subledger_xlsx

def check_negative_balances(base: Path):
    """Kunder med negativ saldo (kreditor) og leverandører med positiv saldo (debet)."""
    details = {}
    ar = load_subledger_xlsx(base, "AR")["bal"]
    ap = load_subledger_xlsx(base, "AP")["bal"]
    if ar is not None and not ar.empty and "UB_Amount" in ar.columns:
        ar2 = ar.copy()
        ar2["UB_Amount"] = pd.to_numeric(ar2["UB_Amount"], errors="coerce").fillna(0.0)
        neg = ar2.loc[ar2["UB_Amount"] < 0].sort_values("UB_Amount")
        details["AR_Negative"] = neg
    if ap is not None and not ap.empty and "UB_Amount" in ap.columns:
        ap2 = ap.copy()
        ap2["UB_Amount"] = pd.to_numeric(ap2["UB_Amount"], errors="coerce").fillna(0.0)
        pos = ap2.loc[ap2["UB_Amount"] > 0].sort_values("UB_Amount", ascending=False)
        details["AP_Positive"] = pos
    return details
