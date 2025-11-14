# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple
import pandas as pd

# Standard kontrollkontoer hvis ikke eksplisitt angitt i arap_control_accounts.csv
AR_CONTROL_ACCOUNTS: Set[str] = {"1510", "1550"}
AP_CONTROL_ACCOUNTS: Set[str] = {"2410", "2460"}

# ---------------- CSV/Excel IO ----------------

def read_csv_safe(path: Path, **kwargs) -> Optional[pd.DataFrame]:
    """Les CSV robust: prøv komma, deretter semikolon. Returner None hvis fil mangler."""
    if path is None or not Path(path).exists():
        return None
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        try:
            return pd.read_csv(path, sep=";", **kwargs)
        except Exception:
            return None

def read_excel_safe(path: Path, **kwargs) -> Optional[pd.DataFrame]:
    """Les første ark i en Excel-fil. Returner None hvis mangler/feiler."""
    if path is None or not Path(path).exists():
        return None
    try:
        return pd.read_excel(path, **kwargs)
    except Exception:
        return None

# ---------------- Filsøk ----------------

def find_csv(base: Path, filename: str) -> Optional[Path]:
    """Finn fil ved å søke i base, foreldre og underkataloger (robust søk)."""
    base = Path(base or ".").resolve()
    tried = []
    # direkte treff i base
    p = base / filename
    if p.exists():
        return p
    # foreldre
    cur = base
    while True:
        candidate = cur / filename
        if candidate.exists():
            return candidate
        tried.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent
    # rekursivt i base og foreldre
    for root in tried + [base]:
        try:
            for hit in Path(root).rglob(filename):
                if hit.is_file():
                    return hit
        except Exception:
            continue
    return None

# ---------------- Dato/numerikk/tekst ----------------

def to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = df[c].astype(str)
            s = (s.str.replace("\u00A0", "", regex=False)
                   .str.replace(" ", "", regex=False)
                   .str.replace(",", ".", regex=False))
            df[c] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return df

def parse_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def has_value(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip().str.lower()
    return ~t.isin(["", "nan", "none", "nat"])

def norm_acc(s: str) -> str:
    v = str(s or "").strip()
    if v.endswith(".0"):
        v = v[:-2]
    v = v.lstrip("0") or "0"
    return v

def norm_acc_series(s: pd.Series) -> pd.Series:
    return s.astype(str).apply(norm_acc)

# ---------------- Periode ----------------

def pick_period(header: Optional[pd.DataFrame], tx: Optional[pd.DataFrame],
                date_from: Optional[str], date_to: Optional[str]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    dfrom = pd.to_datetime(date_from) if date_from else None
    dto   = pd.to_datetime(date_to) if date_to else None
    if header is not None and not header.empty:
        row = header.iloc[0]
        if dfrom is None:
            dfrom = pd.to_datetime(row.get("SelectionStart") or row.get("SelectionStartDate") or row.get("StartDate"),
                                   errors="coerce")
        if dto is None:
            dto = pd.to_datetime(row.get("SelectionEnd") or row.get("SelectionEndDate") or row.get("EndDate"),
                                 errors="coerce")
    if (dfrom is None or pd.isna(dfrom) or dto is None or pd.isna(dto)) and tx is not None and "Date" in tx.columns:
        years = tx["Date"].dropna().dt.year
        if not years.empty:
            year = int(years.value_counts().idxmax())
            dfrom = dfrom or pd.Timestamp(year=year, month=1, day=1)
            dto   = dto   or pd.Timestamp(year=year, month=12, day=31)
    return (dfrom or pd.Timestamp.min).normalize(), (dto or pd.Timestamp.max).normalize()

# ---------------- Kontrollkontoer og mål-UB ----------------

def pick_control_accounts(outdir: Path, which: str) -> Set[str]:
    """Les arap_control_accounts.csv hvis den finnes; ellers defaults."""
    m = read_csv_safe(Path(outdir) / "arap_control_accounts.csv", dtype=str)
    if m is not None and not m.empty and {"PartyType", "AccountID"}.issubset(m.columns):
        desired = "Customer" if which.upper() == "AR" else "Supplier"
        s = m.loc[m["PartyType"] == desired, "AccountID"].dropna().astype(str)
        s = s.map(norm_acc)
        accs = set(s.tolist())
        if accs:
            return accs
    return AR_CONTROL_ACCOUNTS if which.upper() == "AR" else AP_CONTROL_ACCOUNTS

def compute_target_closing(outdir: Path, control_accounts: Set[str]) -> Optional[float]:
    """Returner sum(ClosingDebit-ClosingCredit) for kontrollkontoene, evt. fra trial_balance.xlsx."""
    if not control_accounts:
        return None
    # 1) accounts.csv
    acc_path = find_csv(outdir, "accounts.csv")
    acc = read_csv_safe(acc_path, dtype=str) if acc_path else None
    if acc is not None and {"AccountID", "ClosingDebit", "ClosingCredit"}.issubset(acc.columns):
        acc = acc.copy()
        acc["AccountID"] = norm_acc_series(acc["AccountID"])
        to_num(acc, ["ClosingDebit", "ClosingCredit"])
        mask = acc["AccountID"].isin(control_accounts)
        if mask.any():
            return float((acc.loc[mask, "ClosingDebit"] - acc.loc[mask, "ClosingCredit"]).sum())
    # 2) trial_balance.xlsx
    tb_path = Path(outdir) / "trial_balance.xlsx"
    tb = read_excel_safe(tb_path)
    if tb is not None and "AccountID" in tb.columns:
        t = tb.copy()
        t["AccountID"] = t["AccountID"].astype(str).map(norm_acc)
        if "UB_CloseNet" in t.columns:
            to_num(t, ["UB_CloseNet"])
            mask = t["AccountID"].isin(control_accounts)
            if mask.any():
                return float(t.loc[mask, "UB_CloseNet"].sum())
        if {"ClosingDebit", "ClosingCredit"}.issubset(t.columns):
            to_num(t, ["ClosingDebit", "ClosingCredit"])
            mask = t["AccountID"].isin(control_accounts)
            if mask.any():
                return float((t.loc[mask, "ClosingDebit"] - t.loc[mask, "ClosingCredit"]).sum())
    return None
