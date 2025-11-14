# app/parsers/saft_common.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from functools import lru_cache
from typing import Iterable, Optional, Tuple
import re
import pandas as pd

# ---------------- I/O og filfunn ----------------

def _read_csv_safe(path: Optional[Path], dtype=str) -> Optional[pd.DataFrame]:
    if path is None:
        return None
    try:
        return pd.read_csv(path, dtype=dtype, keep_default_na=False)
    except Exception:
        return None

@lru_cache(maxsize=256)
def _find_csv_file_cached(outdir_str: str, filename: str) -> Optional[str]:
    outdir = Path(outdir_str)
    bases = [outdir, outdir.parent]
    subs = ["", "csv", "CSV", "excel", "Excel"]
    for base in bases:
        for sub in subs:
            p = (base / sub / filename) if sub else (base / filename)
            if p.is_file():
                return str(p)
    return None

def _find_csv_file(outdir: Path, filename: str) -> Optional[Path]:
    res = _find_csv_file_cached(str(outdir), filename)
    return Path(res) if res else None

# ---------------- Datatyper/normalisering ----------------

def _to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df

def _parse_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def _has_value(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip().str.lower()
    return ~t.isin(["", "nan", "none", "nat"])

def _norm_acc(acc: str) -> str:
    s = str(acc).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = s.lstrip("0") or "0"
    return s

def _norm_acc_series(s: pd.Series) -> pd.Series:
    return s.apply(_norm_acc)

# ---------------- Excel-rensing/format ----------------

_CTRL_CHARS = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")

def _sanitize_text_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(lambda x: _CTRL_CHARS.sub("", x))

def _sanitize_df_for_excel(df: pd.DataFrame, stringify_dates: bool = False) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_object_dtype(out[c]):
            out[c] = _sanitize_text_series(out[c])
        if stringify_dates and pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d")
    return out

def _format_sheet_xlsxwriter(writer: pd.ExcelWriter, sheet_name: str,
                             df: pd.DataFrame, freeze_cols: int = 0) -> None:
    try:
        ws = writer.sheets[sheet_name]
        book = writer.book
        fmt_num = book.add_format({"num_format": "# ##0,00;[Red]-# ##0,00"})
        fmt_txt = book.add_format({})
        for idx, col in enumerate(df.columns):
            if pd.api.types.is_numeric_dtype(df[col]):
                ws.set_column(idx, idx, 14, fmt_num)
            else:
                try:
                    w = max(10, min(44, int(df[col].astype(str).map(len).quantile(0.90)) + 2))
                except Exception:
                    w = 18
                ws.set_column(idx, idx, w, fmt_txt)
        if not df.empty:
            ws.freeze_panes(1, freeze_cols)
            ws.autofilter(0, 0, max(1, len(df)), max(0, len(df.columns) - 1))
    except Exception:
        pass

# ---------------- Dato/periode ----------------

def _range_dates(header: Optional[pd.DataFrame],
                 date_from: Optional[str],
                 date_to: Optional[str],
                 tx: Optional[pd.DataFrame]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    dfrom = pd.to_datetime(date_from) if date_from else None
    dto = pd.to_datetime(date_to) if date_to else None
    if header is not None and not header.empty:
        row = header.iloc[0]
        if dfrom is None:
            dfrom = pd.to_datetime(
                row.get("SelectionStart") or row.get("SelectionStartDate") or row.get("StartDate"),
                errors="coerce",
            )
        if dto is None:
            dto = pd.to_datetime(
                row.get("SelectionEnd") or row.get("SelectionEndDate") or row.get("EndDate"),
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

# ---------------- Kontoplan + IB/UB-sikring ----------------

def _find_accounts_file(outdir: Path) -> Optional[pd.DataFrame]:
    p = _find_csv_file(outdir, "accounts.csv")
    if p is None:
        return None
    try:
        df = pd.read_csv(p, dtype=str, keep_default_na=False)
        if not df.empty:
            return df
    except Exception:
        pass
    return None

def _complete_accounts_file(outdir: Path) -> None:
    tx_path = _find_csv_file(outdir, "transactions.csv")
    if tx_path is None:
        return
    tx = _read_csv_safe(tx_path, dtype=str)
    if tx is None or tx.empty or "AccountID" not in tx.columns:
        return
    hdr_path = _find_csv_file(outdir, "header.csv")
    hdr = _read_csv_safe(hdr_path, dtype=str) if hdr_path else None

    tx = _parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    tx["AccountID"] = _norm_acc_series(tx["AccountID"].astype(str))
    tx = _to_num(tx, ["Debit", "Credit"])
    if "IsGL" in tx.columns:
        tx = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()

    dfrom, dto = _range_dates(hdr, None, None, tx)

    acc_path = _find_csv_file(outdir, "accounts.csv")
    acc_df: Optional[pd.DataFrame] = None
    if acc_path is not None and acc_path.is_file():
        try:
            acc_df = pd.read_csv(acc_path, dtype=str, keep_default_na=False)
            desc_cols = [c for c in acc_df.columns if c.lower().startswith("accountdescription")]
            if desc_cols:
                main = desc_cols[0]
                if main != "AccountDescription":
                    acc_df.rename(columns={main: "AccountDescription"}, inplace=True)
                for dc in desc_cols:
                    if dc != main and dc in acc_df.columns:
                        acc_df.drop(columns=[dc], inplace=True)
        except Exception:
            acc_df = None

    desc_df: Optional[pd.DataFrame] = None
    if acc_df is not None and not acc_df.empty and "AccountID" in acc_df.columns:
        acc_df["AccountID"] = _norm_acc_series(acc_df["AccountID"])
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
        rows.append({"AccountID": acc_id, "OpeningDebit": od, "OpeningCredit": oc, "ClosingDebit": cd, "ClosingCredit": cc})
    computed_df = pd.DataFrame(rows)
    if not computed_df.empty and "AccountID" in computed_df.columns:
        computed_df["AccountID"] = computed_df["AccountID"].fillna("").astype(str)
        computed_df.loc[computed_df["AccountID"].str.lower() == "nan", "AccountID"] = "UNDEFINED"
    if desc_df is not None:
        computed_df = computed_df.merge(desc_df, on="AccountID", how="left")

    if acc_df is None or acc_df.empty:
        cols = ["AccountID", "AccountDescription", "OpeningDebit", "OpeningCredit", "ClosingDebit", "ClosingCredit"]
        if "AccountDescription" not in computed_df.columns:
            computed_df["AccountDescription"] = ""
        computed_df[cols].to_csv(Path(outdir) / "accounts.csv", index=False)
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
    combined[cols].to_csv(acc_path if acc_path is not None else Path(outdir) / "accounts.csv", index=False)
