# app/parsers/saft_general_ledger.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Dict

import pandas as pd

# Robust import-guard (pakke/standalone)
try:
    from .saft_common import (  # type: ignore
        _read_csv_safe, _find_csv_file, _parse_dates, _to_num,
        _sanitize_df_for_excel, _sanitize_text_series
    )
except Exception:
    try:
        from saft_common import (  # type: ignore
            _read_csv_safe, _find_csv_file, _parse_dates, _to_num,
            _sanitize_df_for_excel, _sanitize_text_series
        )
    except Exception:
        # Fallbacks hvis sanitizers ikke finnes i repoet (lav risiko)
        def _read_csv_safe(path, **kwargs):
            return pd.read_csv(path, **kwargs)
        def _find_csv_file(outdir: Path, name: str) -> Optional[Path]:
            p = Path(outdir) / name
            return p if p.exists() else None
        def _parse_dates(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
            for c in cols:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce")
            return df
        def _to_num(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
            for c in cols:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
            return df
        def _sanitize_df_for_excel(df: pd.DataFrame, stringify_dates: bool = True) -> pd.DataFrame:
            out = df.copy()
            if stringify_dates:
                for c in out.columns:
                    if pd.api.types.is_datetime64_any_dtype(out[c]):
                        out[c] = out[c].dt.strftime("%Y-%m-%d")
            return out.fillna("")
        def _sanitize_text_series(s: pd.Series) -> pd.Series:
            return s.fillna("").astype(str)


def _xlsxwriter_options() -> Dict:
    # Slår av auto-detektering av URLer/formler for å unngå «Repair»
    return {"options": {
        "strings_to_urls": False,
        "strings_to_numbers": False,
        "strings_to_formulas": False
    }}


def _safe_write_sheet(xw: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame,
                      numeric_cols: Optional[List[str]] = None,
                      col_widths: Optional[Dict[int, int]] = None,
                      stringify_dates: bool = True) -> None:
    """Skriv et ark uten 'View'-greier – unngår 'Repair' i Excel."""
    out = _sanitize_df_for_excel(df, stringify_dates=stringify_dates)
    out.to_excel(xw, index=False, sheet_name=sheet_name)
    try:
        ws = xw.sheets[sheet_name]
        book = xw.book
        if numeric_cols:
            fmt_num = book.add_format({"num_format": "# ##0,00;[Red]-# ##0,00"})
            for c in numeric_cols:
                if c in out.columns:
                    idx = list(out.columns).index(c)
                    ws.set_column(idx, idx, 16, fmt_num)
        if col_widths:
            for c, w in col_widths.items():
                ws.set_column(c, c, w)
    except Exception:
        pass


def make_general_ledger(out_csv_dir: Path,
                        date_from: Optional[str] = None, date_to: Optional[str] = None) -> Path:
    """
    Skriver general_ledger.xlsx fra transactions.csv med trygg Excel-skriving
    (fjerner 'Repair'-dialogen) og riktig tallformat.
    """
    out_csv_dir = Path(out_csv_dir)

    tx_path = _find_csv_file(out_csv_dir, "transactions.csv")
    if not tx_path:
        return out_csv_dir / "general_ledger.xlsx"

    df = _read_csv_safe(tx_path, dtype=str)
    if df is None or df.empty:
        return out_csv_dir / "general_ledger.xlsx"

    # Dato/tekst/tall
    df = _parse_dates(df, ["TransactionDate", "PostingDate"])
    # Robust Date-kolonne
    post = df["PostingDate"] if "PostingDate" in df.columns else None
    trans = df["TransactionDate"] if "TransactionDate" in df.columns else None
    if post is not None and trans is not None:
        df["Date"] = post.fillna(trans)
    elif post is not None:
        df["Date"] = post
    else:
        df["Date"] = trans

    txt_cols = [
        "AccountID", "AccountDescription", "CustomerID", "CustomerName",
        "SupplierID", "SupplierName", "Text", "Description",
        "VoucherID", "VoucherNo", "JournalID", "DocumentNumber", "TaxCode",
    ]
    for c in txt_cols:
        if c in df.columns:
            df[c] = _sanitize_text_series(df[c].astype(str))

    # tallfelt
    df = _to_num(df, [
        "Debit", "Credit", "Amount", "TaxAmount",
        "DebitTaxAmount", "CreditTaxAmount"
    ])
    # Sørg for Amount (hvis ikke fantes)
    if "Amount" not in df.columns:
        df["Amount"] = df.get("Debit", 0.0) - df.get("Credit", 0.0)

    # Ordre/kols
    prefer = [
        "Date", "VoucherID", "VoucherNo", "JournalID", "DocumentNumber",
        "AccountID", "AccountDescription",
        "CustomerID", "CustomerName", "SupplierID", "SupplierName",
        "Debit", "Credit", "Amount",
        "TaxCode", "TaxAmount", "DebitTaxAmount", "CreditTaxAmount",
        "PostingDate", "TransactionDate", "Year", "Period",
    ]
    keep = [c for c in prefer if c in df.columns]
    rest = [c for c in df.columns if c not in keep]
    out = df[keep + rest].sort_values(
        [c for c in ["Date", "AccountID", "VoucherID", "VoucherNo"] if c in (keep + rest)],
        na_position="last"
    )

    # Skriv
    out_path = out_csv_dir / "general_ledger.xlsx"
    with pd.ExcelWriter(
        out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd",
        engine_kwargs=_xlsxwriter_options()
    ) as xw:
        _safe_write_sheet(
            xw, "GeneralLedger", out,
            numeric_cols=[c for c in ["Debit", "Credit", "Amount", "TaxAmount", "DebitTaxAmount", "CreditTaxAmount"] if c in out.columns],
            col_widths={0: 12, 1: 12, 2: 10, 3: 10, 4: 18, 5: 12, 6: 42}
        )
    return out_path