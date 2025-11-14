# app/parsers/excel_writer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence, List
import pandas as pd

__all__ = ["xlsx_writer", "write_sheet", "autofit_columns"]

def xlsx_writer(path: Path | str) -> pd.ExcelWriter:
    return pd.ExcelWriter(
        str(path),
        engine="xlsxwriter",
        datetime_format="yyyy-mm-dd",
        engine_kwargs={"options": {
            "strings_to_urls": False,
            "strings_to_numbers": False,
            "strings_to_formulas": False,
        }},
    )

def _infer_numeric_cols(df: pd.DataFrame, numeric_cols: Optional[Sequence[str]]) -> List[int]:
    if df is None or df.empty:
        return []
    cols = list(df.columns)
    if numeric_cols:
        return [cols.index(c) for c in numeric_cols if c in cols]
    idxs: List[int] = []
    sample = df.head(200)
    for i, c in enumerate(cols):
        ratio = pd.to_numeric(sample[c], errors="coerce").notna().mean()
        if ratio > 0.85:
            idxs.append(i)
    return idxs

def autofit_columns(ws, df: pd.DataFrame, max_width: int = 60, min_width: int = 6) -> None:
    try:
        if df is None or df.empty:
            return
        cols = list(df.columns)
        sample = df.head(500)
        def _disp_len(x) -> int:
            if pd.isna(x): return 0
            s = str(x); base = len(s)
            if any(ord(ch) > 127 for ch in s): base = int(base * 1.1)
            return base
        widths = []
        for c in cols:
            header_len = _disp_len(c)
            body_len = max((_disp_len(v) for v in sample[c].tolist()), default=0)
            w = min(max(header_len, body_len) + 2, max_width)
            w = max(w, min_width)
            widths.append(w)
        for i, w in enumerate(widths):
            ws.set_column(i, i, w)
    except Exception:
        pass

def write_sheet(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame,
                numeric_cols: Optional[Sequence[str]] = None,
                freeze_header: bool = True, autofit: bool = True) -> None:
    out = df.copy() if df is not None else pd.DataFrame()
    out.to_excel(writer, index=False, sheet_name=sheet_name)
    try:
        book = writer.book
        ws = writer.sheets[sheet_name]
        fmt_num = book.add_format({"num_format": "# ##0,00;[Red]-# ##0,00"})
        idxs = _infer_numeric_cols(out, numeric_cols)
        for idx in idxs:
            ws.set_column(idx, idx, None, fmt_num)
        if freeze_header and len(out.columns) > 0:
            ws.freeze_panes(1, 0)
        if autofit:
            autofit_columns(ws, out)
    except Exception:
        pass
