# -*- coding: utf-8 -*-
"""
controls/reports_ledger.py

Genererer:
  - general_ledger.xlsx (ark: GeneralLedger / AllTransactions)
  - trial_balance.xlsx  (ark: TrialBalance [kun IB | Movement | UB] + TB_Detail)

Designvalg:
  - TrialBalance-arket er "revisor-kompakt": IB | Movement | UB, evt. hentet fra accounts.csv
    (Opening/Closing) når det finnes, ellers beregnet fra GL-transaksjoner.
  - Tekniske kolonner (GL_IB/PR/UB, Accounts IB/UB, differanser) legges i ark: TB_Detail.
  - Excel-format: auto-kolonnebredde, frys header, lys blå header, norske datoer dd.mm.yyyy,
    tall med tusenskiller og decimaler.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterable, Tuple, List
import pandas as pd
import numpy as np
import re

# --------------------------- Robust I/O ---------------------------

def _find_csv_file(base: Path, name: str) -> Optional[Path]:
    """Finn CSV enten i base, base/csv eller underliggende mapper."""
    base = Path(base)
    candidates: List[Path] = [
        base / name,
        base / "csv" / name,
    ]
    for p in candidates:
        if p.exists():
            return p
    # fallback: rglob (kan være dyrt, men hendig i dev)
    try:
        for p in base.rglob(name):
            return p
    except Exception:
        pass
    return None

def _read_csv_safe(path: Optional[Path], dtype=str) -> Optional[pd.DataFrame]:
    """Les CSV med flere separator-forsøk. Returnerer None ved feil."""
    if not path or not Path(path).exists():
        return None
    for sep in (",", ";", "\t", "|"):
        try:
            return pd.read_csv(path, dtype=dtype, keep_default_na=False, low_memory=False, sep=sep)
        except Exception:
            continue
    try:
        return pd.read_csv(path, dtype=dtype, keep_default_na=False, low_memory=False)
    except Exception:
        return None

def _to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = (df[c].astype(str)
                        .str.replace("\u00A0", "", regex=False)  # NBSP
                        .str.replace(" ", "", regex=False)
                        .str.replace(",", ".", regex=False))
            df[c] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return df

def _parse_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def _norm_acc(s: str) -> str:
    t = str(s or "").strip()
    if t.endswith(".0"):
        t = t[:-2]
    t = t.lstrip("0") or "0"
    return t

def _norm_acc_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(_norm_acc)

def _has_value(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().ne("").fillna(False)

def _pick_period(hdr: Optional[pd.DataFrame], tx: pd.DataFrame) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Finn periode (dfrom, dto) fra header hvis mulig, ellers fra transaksjoner."""
    dfrom = None
    dto = None
    if hdr is not None and not hdr.empty:
        # ulike leverandører bruker ulike feltnavn
        cand_from = ["SelectionStartDate", "SelectionStart", "StartDate"]
        cand_to   = ["SelectionEndDate",   "SelectionEnd",   "EndDate"]
        for c in cand_from:
            if c in hdr.columns:
                dfrom = pd.to_datetime(hdr.iloc[0].get(c), errors="coerce")
                if pd.notna(dfrom):
                    break
        for c in cand_to:
            if c in hdr.columns:
                dto = pd.to_datetime(hdr.iloc[0].get(c), errors="coerce")
                if pd.notna(dto):
                    break
    if dfrom is None or pd.isna(dfrom):
        dfrom = pd.to_datetime(tx["Date"].min(), errors="coerce")
    if dto is None or pd.isna(dto):
        dto = pd.to_datetime(tx["Date"].max(), errors="coerce")
    return dfrom.normalize(), dto.normalize()  # normalize for å droppe klokkeslett

# --------------------------- Excel-format ---------------------------

def _apply_sheet_formatting(xw, sheet_name: str, df: pd.DataFrame) -> None:
    """
    - Frys header
    - Lys blå header
    - Auto-kolonnebredde (8..46)
    - Tall-format: tusenskiller og to desimaler
    - Datoformat: dd.mm.yyyy
    """
    ws = xw.sheets[sheet_name]
    # Frys header
    try:
        ws.freeze_panes(1, 0)
    except Exception:
        pass

    # Formater
    header_fmt = xw.book.add_format({
        "bold": True,
        "bg_color": "#E6F2FF",  # lys blå
        "border": 0
    })
    # Sett headerformat
    try:
        ws.set_row(0, None, header_fmt)
    except Exception:
        pass

    # Tall- og datoformat
    num_fmt2 = xw.book.add_format({"num_format": "#,##0.00"})       # tusenskiller, 2 des
    int_fmt  = xw.book.add_format({"num_format": "#,##0"})          # hele tall
    date_fmt = xw.book.add_format({"num_format": "dd.mm.yyyy"})

    # Auto width per kolonne (grei heuristikk)
    # Vi måler på str(len) av head + noen rader.
    sample_rows = 200
    for idx, col in enumerate(df.columns):
        # best width fra data (begrens)
        values = df[col].astype(str).head(sample_rows).tolist()
        maxlen = max([len(col)] + [len(v) for v in values]) if values else len(col)
        # heuristikk for bredde (Excel- tegnbredde ~1)
        width = min(max(8, maxlen + 2), 46)
        try:
            ws.set_column(idx, idx, width)
        except Exception:
            pass

        # Format etter dtype/kolonnenavn
        ser = df[col]
        if pd.api.types.is_numeric_dtype(ser):
            # velg int- eller desimalformat
            if pd.isna(ser).all():
                fmt = num_fmt2
            else:
                # hvis alle .0 → int
                vals = pd.to_numeric(ser, errors="coerce")
                if (vals.dropna() == vals.dropna().round(0)).all():
                    fmt = int_fmt
                else:
                    fmt = num_fmt2
            try:
                ws.set_column(idx, idx, None, fmt)
            except Exception:
                pass
        elif pd.api.types.is_datetime64_any_dtype(ser):
            try:
                ws.set_column(idx, idx, None, date_fmt)
            except Exception:
                pass
        elif re.search(r"(date|dato)$", col, flags=re.I):
            try:
                ws.set_column(idx, idx, None, date_fmt)
            except Exception:
                pass

# --------------------------- Kjernefunksjoner ---------------------------

def _load_tx_and_header(outdir: Path) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Les transactions + header og normaliser felter vi trenger."""
    tx_path = _find_csv_file(outdir, "transactions.csv")
    tx = _read_csv_safe(tx_path, dtype=str)
    if tx is None or tx.empty:
        raise FileNotFoundError("transactions.csv mangler eller er tom")
    hdr = _read_csv_safe(_find_csv_file(outdir, "header.csv"), dtype=str)

    # Dato og beløp
    _parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    # Nøkler som str
    for c in ["AccountID", "CustomerID", "SupplierID"]:
        if c in tx.columns:
            tx[c] = tx[c].astype(str)
    if "AccountID" in tx.columns:
        tx["AccountID"] = _norm_acc_series(tx["AccountID"])
    _to_num(tx, ["Debit", "Credit", "TaxAmount", "DebitTaxAmount", "CreditTaxAmount"])
    # Summer
    tx["Amount"] = tx["Debit"] - tx["Credit"]
    return tx, hdr

def make_general_ledger(outdir: Path) -> Path:
    """
    Skriv general_ledger.xlsx.
    Dersom 'IsGL' finnes brukes kun GL-linjer i arket 'GeneralLedger', og ALLE linjer i 'AllTransactions'.
    Ellers skrives alt til 'GeneralLedger'.
    """
    outdir = Path(outdir)
    tx, _ = _load_tx_and_header(outdir)

    path = outdir / "general_ledger.xlsx"
    with pd.ExcelWriter(path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
        if "IsGL" in tx.columns:
            mask_gl = tx["IsGL"].astype(str).str.lower() == "true"
            tx_gl = tx.loc[mask_gl].sort_values(["AccountID", "Date"]).copy()
            tx_all = tx.sort_values(["AccountID", "Date"]).copy()
            tx_gl.to_excel(xw, index=False, sheet_name="GeneralLedger")
            tx_all.to_excel(xw, index=False, sheet_name="AllTransactions")
            _apply_sheet_formatting(xw, "GeneralLedger", tx_gl)
            _apply_sheet_formatting(xw, "AllTransactions", tx_all)
        else:
            tx_sorted = tx.sort_values(["AccountID", "Date"]).copy()
            tx_sorted.to_excel(xw, index=False, sheet_name="GeneralLedger")
            _apply_sheet_formatting(xw, "GeneralLedger", tx_sorted)
    return path

def make_trial_balance(outdir: Path,
                       date_from: Optional[str] = None,
                       date_to: Optional[str] = None) -> Path:
    """
    Skriv trial_balance.xlsx.

    Ark:
      - TrialBalance:    AccountID | AccountDescription | IB | Movement | UB
      - TB_Detail:       tekniske kolonner (GL_IB/PR/UB vs Accounts IB/UB og differanser)

    Regler:
      * Hvis accounts.csv har OpeningDebit/OpeningCredit/ClosingDebit/ClosingCredit, brukes disse som IB/UB
        og Movement=UB-IB (GL-summer vises i TB_Detail).
      * Ellers beregnes IB/PR/UB fra GL (transactions).
    """
    outdir = Path(outdir)
    tx, hdr = _load_tx_and_header(outdir)
    # periode
    dfrom, dto = _pick_period(hdr, tx)

    # --- GL-basert IB/PR/UB ---
    def _sum(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame({"AccountID": [], label: []})
        g = df.groupby("AccountID")[["Debit", "Credit"]].sum().reset_index()
        g[label] = g["Debit"] - g["Credit"]
        return g[["AccountID", label]]

    gl_ib = _sum(tx.loc[tx["Date"] < dfrom], "GL_IB")
    gl_pr = _sum(tx.loc[(tx["Date"] >= dfrom) & (tx["Date"] <= dto)], "GL_PR")
    gl_ub = _sum(tx.loc[tx["Date"] <= dto], "GL_UB")
    tb_gl = (gl_ub.merge(gl_ib, on="AccountID", how="outer")
                  .merge(gl_pr, on="AccountID", how="outer")).fillna(0.0)

    # --- Accounts (hvis finnes) ---
    acc = _read_csv_safe(_find_csv_file(outdir, "accounts.csv"), dtype=str)
    detail = tb_gl.copy()
    use_accounts = False
    acc_view = None
    if acc is not None and "AccountID" in acc.columns:
        a = acc.copy()
        a["AccountID"] = _norm_acc_series(a["AccountID"])
        if {"OpeningDebit", "OpeningCredit", "ClosingDebit", "ClosingCredit"}.issubset(a.columns):
            _to_num(a, ["OpeningDebit", "OpeningCredit", "ClosingDebit", "ClosingCredit"])
            a["IB_OpenNet"]  = a["OpeningDebit"] - a["OpeningCredit"]
            a["UB_CloseNet"] = a["ClosingDebit"] - a["ClosingCredit"]
            a["PR_Accounts"] = a["UB_CloseNet"] - a["IB_OpenNet"]
            keep = ["AccountID", "IB_OpenNet", "PR_Accounts", "UB_CloseNet"]
            if "AccountDescription" in a.columns:
                keep.insert(1, "AccountDescription")
            acc_view = a[keep].copy()
            # for detaljer
            detail = acc_view.merge(tb_gl, on="AccountID", how="outer").fillna(0.0)
            detail["Diff_IB"] = (detail.get("GL_IB", 0.0) - detail.get("IB_OpenNet", 0.0)).round(2)
            detail["Diff_PR"] = (detail.get("GL_PR", 0.0) - detail.get("PR_Accounts", 0.0)).round(2)
            detail["Diff_UB"] = (detail.get("GL_UB", 0.0) - detail.get("UB_CloseNet", 0.0)).round(2)
            use_accounts = True
        else:
            # mangler open/close – men vi prøver å få med beskrivelse i visningene
            if "AccountDescription" in a.columns:
                acc_view = a[["AccountID", "AccountDescription"]].copy()
                detail = acc_view.merge(tb_gl, on="AccountID", how="outer").fillna(0.0)

    # --- TrialBalance (enkelt ark) ---
    if use_accounts:
        # primært fra accounts
        simple = acc_view.copy()
        simple.rename(columns={"IB_OpenNet": "IB", "PR_Accounts": "Movement", "UB_CloseNet": "UB"}, inplace=True)
    else:
        # fra GL
        simple = tb_gl.copy()
        simple.rename(columns={"GL_IB": "IB", "GL_PR": "Movement", "GL_UB": "UB"}, inplace=True)
        if acc_view is not None:
            simple = simple.merge(acc_view, on="AccountID", how="left")

    # rekkefølge og avrunding
    for c in ["IB", "Movement", "UB"]:
        if c in simple.columns:
            simple[c] = pd.to_numeric(simple[c], errors="coerce").round(2)
    # ønsket rekkefølge kolonner
    first_cols = ["AccountID"]
    if "AccountDescription" in simple.columns:
        first_cols.append("AccountDescription")
    val_cols = [c for c in ["IB", "Movement", "UB"] if c in simple.columns]
    simple = simple[first_cols + val_cols].sort_values("AccountID")

    # --- Skriv Excel ---
    path = outdir / "trial_balance.xlsx"
    with pd.ExcelWriter(path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
        simple.to_excel(xw, index=False, sheet_name="TrialBalance")
        _apply_sheet_formatting(xw, "TrialBalance", simple)

        # TB_Detail (fullt datagrunnlag)
        # legg på beskrivelse hvis ikke allerede der
        det = detail.copy()
        if "AccountDescription" not in det.columns and acc_view is not None and "AccountDescription" in acc_view.columns:
            det = det.merge(acc_view[["AccountID", "AccountDescription"]], on="AccountID", how="left")
        # sorter og rund flyttall
        for c in det.select_dtypes(include=["float", "float64"]).columns:
            det[c] = det[c].round(2)
        cols_order = ["AccountID"]
        if "AccountDescription" in det.columns:
            cols_order.append("AccountDescription")
        # GL først, så Accounts, så Diff
        cols_gl  = [c for c in ["GL_IB", "GL_PR", "GL_UB"] if c in det.columns]
        cols_acc = [c for c in ["IB_OpenNet", "PR_Accounts", "UB_CloseNet"] if c in det.columns]
        cols_dif = [c for c in ["Diff_IB", "Diff_PR", "Diff_UB"] if c in det.columns]
        cols_rest = [c for c in det.columns if c not in (cols_order + cols_gl + cols_acc + cols_dif)]
        det = det[cols_order + cols_gl + cols_acc + cols_dif + cols_rest].sort_values("AccountID")
        det.to_excel(xw, index=False, sheet_name="TB_Detail")
        _apply_sheet_formatting(xw, "TB_Detail", det)

    return path

# --------------------------- CLI (frivillig) ---------------------------

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Lag General Ledger og Trial Balance fra SAF‑T CSV‑mapper.")
    ap.add_argument("outdir", help="Mappe som inneholder SAF‑T CSV")
    args = ap.parse_args()
    p1 = make_general_ledger(Path(args.outdir))
    p2 = make_trial_balance(Path(args.outdir))
    print("Skrev:\n ", p1, "\n ", p2)
