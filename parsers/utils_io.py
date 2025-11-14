# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional, Set, Tuple
import pandas as pd

# Standard reskontro-konti hvis arap_control_accounts.csv ikke oppgis
AR_CONTROL_ACCOUNTS: Set[str] = {"1510", "1550"}
AP_CONTROL_ACCOUNTS: Set[str] = {"2410", "2460"}

# ---------------- CSV / fil-finnere ----------------

def read_csv_safe(path: Path, dtype=str, **kwargs) -> Optional[pd.DataFrame]:
    """Les CSV trygt; returner None ved feil/ikke funnet."""
    try:
        if not path or not path.exists():
            return None
        return pd.read_csv(path, dtype=dtype, keep_default_na=False, **kwargs)
    except Exception:
        # fallback for ; separerte
        try:
            return pd.read_csv(path, dtype=dtype, keep_default_na=False, sep=";", **kwargs)
        except Exception:
            return None

def find_csv_file(root: Path, filename: str) -> Optional[Path]:
    """Finn en fil ved å sjekke root, foreldre og rekursivt i underkataloger."""
    dirs = []
    cur = Path(root)
    while True:
        dirs.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent
    cwd = Path.cwd()
    cur = cwd
    while True:
        if cur not in dirs:
            dirs.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent

    for d in dirs:
        p = d / filename
        if p.is_file():
            return p

    for base in dirs:
        try:
            for p in base.rglob(filename):
                if p.is_file():
                    return p
        except Exception:
            continue
    return None

# ---------------- Dataprepp ----------------

def to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df

def parse_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def has_value(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip().str.lower()
    return ~t.isin(["", "nan", "none", "nat"])

def _norm_acc(s: str) -> str:
    s = str(s).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = s.lstrip("0") or "0"
    return s

def norm_acc_series(s: pd.Series) -> pd.Series:
    return s.apply(_norm_acc)

# ---------------- Dato/utvalg ----------------

def range_dates(
    header: Optional[pd.DataFrame],
    date_from: Optional[str],
    date_to: Optional[str],
    tx: Optional[pd.DataFrame],
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Bestem datoperiode fra header + evt. eksplisitt, ellers dominerende år i data."""
    dfrom = pd.to_datetime(date_from) if date_from else None
    dto = pd.to_datetime(date_to) if date_to else None

    if header is not None and not header.empty:
        row = header.iloc[0]
        if dfrom is None:
            dfrom = pd.to_datetime(
                row.get("SelectionStart")
                or row.get("SelectionStartDate")
                or row.get("StartDate"),
                errors="coerce",
            )
        if dto is None:
            dto = pd.to_datetime(
                row.get("SelectionEnd")
                or row.get("SelectionEndDate")
                or row.get("EndDate"),
                errors="coerce",
            )

    if ((dfrom is None or pd.isna(dfrom)) or (dto is None or pd.isna(dto))) and tx is not None and not tx.empty and "Date" in tx.columns:
        years = tx["Date"].dropna().dt.year
        if not years.empty:
            year = int(years.value_counts().idxmax())
            if dfrom is None or pd.isna(dfrom):
                dfrom = pd.Timestamp(year=year, month=1, day=1)
            if dto is None or pd.isna(dto):
                dto = pd.Timestamp(year=year, month=12, day=31)

    if dfrom is None or pd.isna(dfrom):
        dfrom = pd.Timestamp.min
    if dto is None or pd.isna(dto):
        dto = pd.Timestamp.max
    return dfrom.normalize(), dto.normalize()

# ---------------- Kontroller/kontoplan ----------------

def pick_control_accounts(outdir: Path, which: str) -> Set[str]:
    """Hent kontrollkontoer for 'AR' eller 'AP' (fra arap_control_accounts.csv hvis mulig)."""
    p = outdir / "arap_control_accounts.csv"
    arap = read_csv_safe(p, dtype=str)
    if arap is not None and not arap.empty and {"PartyType", "AccountID"}.issubset(arap.columns):
        desired = "Customer" if which.upper() == "AR" else "Supplier"
        s = arap.loc[arap["PartyType"] == desired, "AccountID"].dropna().astype(str).map(_norm_acc)
        vals = set(s.tolist())
        if vals:
            return vals
    return AR_CONTROL_ACCOUNTS if which.upper() == "AR" else AP_CONTROL_ACCOUNTS

def _load_accounts_anywhere(outdir: Path) -> Optional[pd.DataFrame]:
    acc_path = find_csv_file(outdir, "accounts.csv")
    if not acc_path:
        return None
    df = read_csv_safe(acc_path, dtype=str)
    return df if (df is not None and not df.empty) else None

def compute_target_closing(outdir: Path, control_accounts: Set[str]) -> Optional[float]:
    """Mål for UB på reskontro-kontoer (fra accounts.csv eller trial_balance.xlsx)."""
    if not control_accounts:
        return None

    acc = _load_accounts_anywhere(outdir)
    if acc is not None and {"AccountID", "ClosingDebit", "ClosingCredit"}.issubset(acc.columns):
        tmp = acc.copy()
        tmp["AccountID"] = tmp["AccountID"].astype(str).map(_norm_acc)
        tmp = to_num(tmp, ["ClosingDebit", "ClosingCredit"])
        mask = tmp["AccountID"].isin(control_accounts)
        if mask.any():
            return float((tmp.loc[mask, "ClosingDebit"] - tmp.loc[mask, "ClosingCredit"]).sum())

    tb_xlsx = outdir / "trial_balance.xlsx"
    if tb_xlsx.exists():
        try:
            tb = pd.read_excel(tb_xlsx, sheet_name=0)
            if "AccountID" in tb.columns:
                tmp = tb.copy()
                tmp["AccountID"] = tmp["AccountID"].astype(str).map(_norm_acc)
                if "UB_CloseNet" in tmp.columns:
                    tmp = to_num(tmp, ["UB_CloseNet"])
                    mask = tmp["AccountID"].isin(control_accounts)
                    if mask.any():
                        return float(tmp.loc[mask, "UB_CloseNet"].sum())
                if {"ClosingDebit", "ClosingCredit"}.issubset(tmp.columns):
                    tmp = to_num(tmp, ["ClosingDebit", "ClosingCredit"])
                    mask = tmp["AccountID"].isin(control_accounts)
                    if mask.any():
                        return float((tmp.loc[mask, "ClosingDebit"] - tmp.loc[mask, "ClosingCredit"]).sum())
        except Exception:
            pass
    return None

def complete_accounts_file(outdir: Path) -> None:
    """Kompletter accounts.csv med alle kontoer som finnes i transactions.csv (beregner IB/UB)."""
    tx_path = find_csv_file(outdir, "transactions.csv")
    if tx_path is None:
        return
    tx = read_csv_safe(tx_path, dtype=str)
    if tx is None or tx.empty or "AccountID" not in tx.columns:
        return

    hdr_path = find_csv_file(outdir, "header.csv")
    hdr = read_csv_safe(hdr_path, dtype=str) if hdr_path else None

    tx = parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    tx["AccountID"] = norm_acc_series(tx["AccountID"].astype(str))
    tx = to_num(tx, ["Debit", "Credit"])
    if "IsGL" in tx.columns:
        tx = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()

    dfrom, dto = range_dates(hdr, None, None, tx)

    acc_path = find_csv_file(outdir, "accounts.csv")
    acc_df: Optional[pd.DataFrame] = None
    if acc_path is not None and acc_path.is_file():
        acc_df = read_csv_safe(acc_path, dtype=str)

        # fjern dupliserte AccountDescription kolonner (noen flettinger kan ha laget det)
        if acc_df is not None:
            desc_cols = [c for c in acc_df.columns if c.lower().startswith("accountdescription")]
            if desc_cols:
                main = desc_cols[0]
                if main != "AccountDescription":
                    acc_df.rename(columns={main: "AccountDescription"}, inplace=True)
                for dc in desc_cols:
                    if dc != main and dc in acc_df.columns:
                        acc_df.drop(columns=[dc], inplace=True)

    desc_df: Optional[pd.DataFrame] = None
    if acc_df is not None and not acc_df.empty and "AccountID" in acc_df.columns:
        acc_df["AccountID"] = norm_acc_series(acc_df["AccountID"])
        if "AccountDescription" in acc_df.columns:
            desc_df = acc_df[["AccountID", "AccountDescription"]].copy()

    all_tx_accounts = sorted(set(tx["AccountID"].dropna().astype(str).tolist()))
    tx_open = tx[tx["Date"] < dfrom]
    tx_close = tx[tx["Date"] <= dto]
    open_sum = tx_open.groupby("AccountID")[["Debit", "Credit"]].sum()
    close_sum = tx_close.groupby("AccountID")[["Debit", "Credit"]].sum()

    rows = []
    for acc_id in all_tx_accounts:
        od = float(open_sum.loc[acc_id, "Debit"]) if acc_id in open_sum.index else 0.0
        oc = float(open_sum.loc[acc_id, "Credit"]) if acc_id in open_sum.index else 0.0
        cd = float(close_sum.loc[acc_id, "Debit"]) if acc_id in close_sum.index else 0.0
        cc = float(close_sum.loc[acc_id, "Credit"]) if acc_id in close_sum.index else 0.0
        rows.append(
            {
                "AccountID": acc_id,
                "OpeningDebit": od,
                "OpeningCredit": oc,
                "ClosingDebit": cd,
                "ClosingCredit": cc,
            }
        )
    computed_df = pd.DataFrame(rows)

    if not computed_df.empty and "AccountID" in computed_df.columns:
        computed_df["AccountID"] = computed_df["AccountID"].fillna("").astype(str)
        computed_df.loc[computed_df["AccountID"].str.lower() == "nan", "AccountID"] = "UNDEFINED"

    if desc_df is not None:
        computed_df = computed_df.merge(desc_df, on="AccountID", how="left")

    if acc_df is None or acc_df.empty:
        cols = [
            "AccountID",
            "AccountDescription",
            "OpeningDebit",
            "OpeningCredit",
            "ClosingDebit",
            "ClosingCredit",
        ]
        if "AccountDescription" not in computed_df.columns:
            computed_df["AccountDescription"] = ""
        computed_df[cols].to_csv(outdir / "accounts.csv", index=False)
        return

    acc_df = acc_df.copy()
    num_cols = ["OpeningDebit", "OpeningCredit", "ClosingDebit", "ClosingCredit"]
    for col in num_cols:
        if col in acc_df.columns:
            acc_df[col] = pd.to_numeric(acc_df[col], errors="coerce").fillna(0.0)
        else:
            acc_df[col] = 0.0
    if "AccountDescription" not in acc_df.columns:
        acc_df["AccountDescription"] = ""

    existing_ids = set(acc_df["AccountID"].astype(str))
    missing_df = computed_df[~computed_df["AccountID"].astype(str).isin(existing_ids)].copy()
    if "AccountDescription" not in missing_df.columns:
        missing_df["AccountDescription"] = ""
    else:
        missing_df["AccountDescription"] = missing_df["AccountDescription"].fillna("")

    combined = pd.concat([acc_df, missing_df], ignore_index=True, sort=False)
    cols = ["AccountID", "AccountDescription", "OpeningDebit", "OpeningCredit", "ClosingDebit", "ClosingCredit"]
    for col in cols:
        if col not in combined.columns:
            combined[col] = 0.0 if col in num_cols else ""
    combined[cols].to_csv(acc_path if acc_path is not None else outdir / "accounts.csv", index=False)
