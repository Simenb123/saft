# -*- coding: utf-8 -*-
"""
report_fmt.py – felles Excel-formattering:
- Norsk datoformat (dd.mm.yyyy)
- Tusenskiller og komma som desimal (# ##0,00)
- Frys første rad + lys blå header
- Rimelig auto-bredde på kolonner (maks 46)
"""
from __future__ import annotations
from typing import Iterable, Optional
import pandas as pd

MAX_COL_WIDTH = 46
SAMPLE_ROWS_FOR_WIDTH = 500  # bruk de første N radene for å anslå bredde

def _guess_is_date(colname: str) -> bool:
    n = colname.lower()
    return ("date" in n) or n.endswith("_dato") or n in {"postingdate", "transactiondate"}

def _width_from_series(s: pd.Series, header: str) -> int:
    # konverter noen få rader til str, ta maks-lengde + litt luft
    sample = s.head(SAMPLE_ROWS_FOR_WIDTH)
    try:
        vals = sample.astype(str).tolist()
    except Exception:
        vals = [str(x) for x in sample.tolist()]
    w = max(len(header), max((len(v) for v in vals), default=0)) + 2
    return min(MAX_COL_WIDTH, max(8, w))

def format_sheet(xw, sheet_name: str, df: pd.DataFrame,
                 freeze_first_row: bool = True,
                 explicit_date_cols: Optional[Iterable[str]] = None) -> None:
    """
    Bruk etter at df er skrevet til xw via df.to_excel(..., sheet_name=sheet_name).
    """
    ws = xw.sheets[sheet_name]
    book = xw.book

    # formater
    header_fmt = book.add_format({"bold": True, "bg_color": "#D9E1F2"})
    date_fmt   = book.add_format({"num_format": "dd.mm.yyyy"})
    num0_fmt   = book.add_format({"num_format": "# ##0"})
    num2_fmt   = book.add_format({"num_format": "# ##0,00"})

    # frys header
    if freeze_first_row:
        try:
            ws.freeze_panes(1, 0)
        except Exception:
            pass

    # header-stil
    try:
        ws.set_row(0, None, header_fmt)
    except Exception:
        pass

    # sett bredde og kolonneformat
    exp_dates = set([c for c in (explicit_date_cols or [])])
    for idx, col in enumerate(df.columns):
        width = _width_from_series(df[col], str(col))
        series = df[col]

        # bestem format
        fmt = None
        if (
            pd.api.types.is_datetime64_any_dtype(series)
            or col in exp_dates
            or _guess_is_date(str(col))
        ):
            fmt = date_fmt
            width = min(width, 16)
        elif pd.api.types.is_numeric_dtype(series):
            # To desimaler på beløpskolonner, ellers hele tall
            nameu = str(col).upper()
            if any(nameu.endswith(suf) for suf in ("_AMOUNT", "DEBIT", "CREDIT", "IB", "PR", "UB")) \
               or nameu in {"AMOUNT", "DEBIT", "CREDIT"}:
                fmt = num2_fmt
            else:
                # sjekk om float
                fmt = num2_fmt if pd.api.types.is_float_dtype(series) else num0_fmt

        try:
            if fmt is not None:
                ws.set_column(idx, idx, width, fmt)
            else:
                ws.set_column(idx, idx, width)
        except Exception:
            pass
