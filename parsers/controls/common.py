# -*- coding: utf-8 -*-
"""
controls/common.py – felles konstanter og hjelpere for kontroller.
"""
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Optional, Tuple, Set
import pandas as pd

# ---- terskler og konstanter ----
CENT_TOL = 0.01           # for "0-feil" (balanse)
NOK_TOL  = 1.00           # praktisk 1-kroners toleranse
VAT_PREFIXES = ("27",)    # MVA-konti (27xx) som utgangspunkt

# ---- valgfri rapportformattering ----
def _import_formatter():
    try:
        # typisk prosjektsti
        from app.parsers.controls.report_fmt import apply_common_format  # type: ignore
        return apply_common_format
    except Exception:
        try:
            # relativt fra parsers/
            from app.parsers.controls.report_fmt import apply_common_format  # type: ignore
            return apply_common_format
        except Exception:
            def _noop(writer, sheet_name: str, df: pd.DataFrame, **kwargs) -> None:
                return None
            return _noop

apply_common_format = _import_formatter()

# ---- I/O-hjelpere ----
def read_csv_any(p: Path, dtype=str) -> Optional[pd.DataFrame]:
    if not p or not p.exists():
        return None
    for sep in (",", ";", "\t"):
        try:
            return pd.read_csv(p, dtype=dtype, keep_default_na=False, sep=sep)
        except Exception:
            continue
    try:
        return pd.read_csv(p, dtype=dtype, keep_default_na=False)
    except Exception:
        return None

def find_near(outdir: Path, name: str) -> Optional[Path]:
    # søk i outdir, outdir/csv, og et nivå opp
    for cand in (outdir / name, outdir / "csv" / name):
        if cand.exists():
            return cand
    for base in (outdir, outdir.parent, Path.cwd()):
        try:
            for fp in base.rglob(name):
                return fp
        except Exception:
            pass
    return None

# ---- datavask ----
def to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = (df[c].astype(str)
                        .str.replace("\u00A0","",regex=False)  # NBSP
                        .str.replace(" ", "", regex=False)
                        .str.replace(",",".", regex=False))
            df[c] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return df

def parse_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def norm_acc(s: str) -> str:
    t = str(s or "").strip()
    if t.endswith(".0"):
        t = t[:-2]
    t = t.lstrip("0") or "0"
    return t

def norm_acc_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(norm_acc)

def period_ym(d: pd.Series) -> pd.Series:
    return pd.to_datetime(d, errors="coerce").dt.to_period("M").astype(str)

def year_term(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d, errors="coerce")
    def _lab(x):
        if pd.isna(x): return ""
        t = (x.month + 1) // 2
        return f"{x.year}-T{t}"
    return d.apply(_lab)

def status(ok: bool=None, warn: bool=False) -> str:
    if ok is True: return "OK"
    if ok is False: return "FEIL"
    return "OBS" if warn else "OK"

# ---- kontrollkonti AR/AP ----
def pick_ar_ap_controls(outdir: Path) -> Tuple[Set[str], Set[str]]:
    cfg = read_csv_any(outdir / "arap_control_accounts.csv", dtype=str)
    if cfg is not None and {"PartyType", "AccountID"}.issubset(cfg.columns):
        cfg["AccountID"] = norm_acc_series(cfg["AccountID"])
        ar = set(cfg.loc[cfg["PartyType"].str.lower()=="customer","AccountID"])
        ap = set(cfg.loc[cfg["PartyType"].str.lower()=="supplier","AccountID"])
        if ar or ap:
            return (ar or {"1510","1550"}, ap or {"2410","2460"})
    return {"1510","1550"}, {"2410","2460"}

# ---- TB/Accounts og subledger UB ----
def load_tb_ub_and_accounts_ub(outdir: Path, ctrl: Set[str]) -> Tuple[Optional[float], Optional[float]]:
    """(UB_GL fra TrialBalance.xlsx, UB_Accounts fra accounts.csv) for gitt kontrollkontoliste."""
    ub_gl = None; ub_acc = None
    tbp = outdir / "trial_balance.xlsx"
    if tbp.exists():
        try:
            tb = pd.read_excel(tbp, sheet_name="TrialBalance")
            if {"AccountID","UB"}.issubset(tb.columns):
                tb["AccountID"] = norm_acc_series(tb["AccountID"]); to_num(tb, ["UB"])
                mask = tb["AccountID"].isin(ctrl)
                if mask.any(): ub_gl = float(tb.loc[mask,"UB"].sum())
        except Exception:
            pass
    acc = read_csv_any(outdir / "accounts.csv", dtype=str)
    if acc is not None and {"AccountID","ClosingDebit","ClosingCredit"}.issubset(acc.columns):
        acc["AccountID"] = norm_acc_series(acc["AccountID"]); to_num(acc, ["ClosingDebit","ClosingCredit"])
        mask = acc["AccountID"].isin(ctrl)
        if mask.any(): ub_acc = float((acc.loc[mask,"ClosingDebit"] - acc.loc[mask,"ClosingCredit"]).sum())
    return ub_gl, ub_acc

def read_subledger_ub(excel_path: Path, sheet: str) -> Optional[float]:
    try:
        if excel_path.exists():
            return float(pd.read_excel(excel_path, sheet_name=sheet)["UB_Amount"].sum())
    except Exception:
        pass
    return None
