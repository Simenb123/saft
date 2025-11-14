# -*- coding: utf-8 -*-
"""
common.py
---------
Felles, uavhengige hjelpefunksjoner som kan gjenbrukes av rapportmodulene.
Denne modulen har *ingen* avhengighet til saft_reports/run_saft_pro_gui,
og kan derfor trygt importeres fra hvor som helst uten sirkulære imports.

NB: Denne filen er et tilbud for å rydde opp dupliserte helpers. Du kan
innføre den gradvis. Eksisterende logikk i run_saft_pro_gui endres ikke.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Sequence
import re
import pandas as pd

# --- I/O helpers ---------------------------------------------------------

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

def find_csv_file(outdir: Path, name: str) -> Optional[Path]:
    p = Path(outdir) / name
    if p.exists():
        return p
    # vanlige alternative plasseringer
    for cand in [Path(outdir).parent / "csv" / name, Path(outdir) / "csv" / name]:
        if cand.exists():
            return cand
    return None

# --- Data helpers --------------------------------------------------------

def parse_dates(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

def to_numeric_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def to_numeric_df(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = to_numeric_series(out[c])
    return out

def has_value(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().ne("").fillna(False)

# --- Konto-normalisering -------------------------------------------------

_NON_DIGIT = re.compile(r"[^0-9]")

def norm_acc(value: str) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    s = _NON_DIGIT.sub("", s)
    if s == "":
        return ""
    s = s.lstrip("0")
    return s if s else "0"

def norm_acc_series(s: pd.Series) -> pd.Series:
    return s.apply(norm_acc)

# --- Periode -------------------------------------------------------------

def range_dates(header_df: Optional[pd.DataFrame], date_from: Optional[str], date_to: Optional[str], tx: pd.DataFrame):
    d_min = pd.to_datetime(tx.get("Date"), errors="coerce").min()
    d_max = pd.to_datetime(tx.get("Date"), errors="coerce").max()
    if header_df is not None and not header_df.empty:
        for c in ("StartDate","FromDate","PeriodStart","FiscalYearStart"):
            if c in header_df.columns:
                cand = pd.to_datetime(header_df[c].iloc[0], errors="coerce")
                if pd.notna(cand):
                    d_min = cand; break
        for c in ("EndDate","ToDate","PeriodEnd","FiscalYearEnd"):
            if c in header_df.columns:
                cand = pd.to_datetime(header_df[c].iloc[0], errors="coerce")
                if pd.notna(cand):
                    d_max = cand; break
    if date_from: d_min = pd.to_datetime(date_from)
    if date_to:   d_max = pd.to_datetime(date_to)
    if pd.isna(d_min): d_min = pd.Timestamp("1900-01-01")
    if pd.isna(d_max): d_max = pd.Timestamp("2999-12-31")
    if d_max < d_min: d_min, d_max = d_max, d_min
    return d_min.normalize(), d_max.normalize()

# --- Kontrollkontoer (valgfritt å bruke) --------------------------------

AR_CONTROL_ACCOUNTS = ("1500","1501","1505","1510","1515","1520","1530")
AP_CONTROL_ACCOUNTS = ("2400","2401","2405","2410","2415","2420","2430")

def pick_control_accounts(defaults: Iterable[str] = AR_CONTROL_ACCOUNTS) -> List[str]:
    return [str(x) for x in defaults]
