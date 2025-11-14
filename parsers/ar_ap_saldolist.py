# -*- coding: utf-8 -*-
"""
ar_ap_saldolist.py
------------------
Genererer en kombinert AR/AP-saldoliste og enkel avstemmingsrapport ved å:
  1) Sørge for at subledger-rapportene (AR/AP) er generert via fasaden
     `saft_reports.make_subledger(...)`.
  2) Summere UB i subledgerenes "Balances"-ark (summen av partenes UB).
  3) Sammenligne mot hovedbokens kontrollkontoer (UB) fra transactions.csv.
  4) Skrive en Excel-bok `ar_ap_saldolist.xlsx` med:
     - Summary
     - AR_Recon (total + per kontrollkonto)
     - AP_Recon (total + per kontrollkonto)
     - Customers_UB (topp kunder etter |UB|)
     - Suppliers_UB (topp leverandører etter |UB|)

Lav risiko: ingen forretningsregler endres — vi bruker eksisterende rapporter
som sannhet (subledger via fasaden) og leser transactions.csv på samme måte
som øvrige moduler. Hjelpefunksjoner importeres fra common.py.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Tuple
import pandas as pd

# Felles helpers (ingen sirkulær avhengighet)
from .common import (
    read_csv_safe, find_csv_file, parse_dates,
    to_numeric_series, norm_acc_series, has_value,
    range_dates, AR_CONTROL_ACCOUNTS, AP_CONTROL_ACCOUNTS
)

# Fasade/proxy (stabilt inngangspunkt)
from . import saft_reports as reports


# ---------------- Intern helpers ----------------

def _ensure_excel_dir(csv_dir: Path) -> Path:
    candidates = [csv_dir.parent / "excel", csv_dir / "excel"]
    for p in candidates:
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception:
            continue
    # fallback
    d = csv_dir / "excel"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _find_excel(csv_dir: Path, name: str) -> Optional[Path]:
    for cand in (csv_dir.parent / "excel" / name, csv_dir / "excel" / name, csv_dir / name):
        if cand.exists():
            return cand
    return None

def _load_subledger_balances(xlsx: Path, sheet: str = "Balances") -> pd.DataFrame:
    if not xlsx or not xlsx.exists():
        return pd.DataFrame()
    try:
        df = pd.read_excel(xlsx, sheet_name=sheet, dtype={"UB": "float64"})
    except Exception:
        return pd.DataFrame()
    # Standardiser kolonner
    cols = [c.strip() for c in df.columns]
    df.columns = cols
    for c in ("IB","PR","UB"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    # Kandidater for id/navn
    id_cols = [c for c in df.columns if c.lower() in ("customerid","supplierid","partyid","kundenr","leverandornr")]
    name_cols = [c for c in df.columns if c.lower() in ("partyname","customername","suppliername","name","kundenavn","leverandornavn")]
    if not id_cols:
        df["PartyID"] = ""
        id_cols = ["PartyID"]
    if not name_cols:
        df["PartyName"] = ""
        name_cols = ["PartyName"]
    # Velg første forekomst
    df.rename(columns={id_cols[0]: "PartyID", name_cols[0]: "PartyName"}, inplace=True)
    # Rydd
    return df[["PartyID","PartyName"] + [c for c in ("IB","PR","UB") if c in df.columns]]

def _read_transactions(csv_dir: Path) -> pd.DataFrame:
    tx_path = find_csv_file(csv_dir, "transactions.csv")
    tx = read_csv_safe(tx_path, dtype="str") if tx_path else pd.DataFrame()
    if tx.empty:
        return pd.DataFrame()
    tx = parse_dates(tx, ["PostingDate","TransactionDate"])
    tx["Date"] = tx.get("PostingDate", pd.NaT).fillna(tx.get("TransactionDate"))
    # tall
    for c in ("Debit","Credit","Amount","TaxAmount"):
        if c in tx.columns:
            tx[c] = to_numeric_series(tx[c])
    if "Amount" not in tx.columns:
        tx["Amount"] = tx.get("Debit", 0.0) - tx.get("Credit", 0.0)
    if "AccountID" in tx.columns:
        tx["AccountID"] = norm_acc_series(tx["AccountID"])
    # Kun GL hvis flagg finnes
    if "IsGL" in tx.columns:
        tx = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()
    return tx

def _ledger_ub_by_account(tx: pd.DataFrame, accounts: List[str], dfrom: Optional[pd.Timestamp], dto: Optional[pd.Timestamp]) -> pd.Series:
    if tx.empty or "AccountID" not in tx.columns or "Date" not in tx.columns:
        return pd.Series(dtype="float64")
    accs = pd.Series(accounts, dtype="str").map(lambda x: x.strip()).tolist()
    # Normaliser til tallstrenger uten ledende nuller
    accs = [a.lstrip("0") if isinstance(a, str) else a for a in accs]
    mask = tx["AccountID"].isin(accs)
    if dfrom is not None:
        mask &= tx["Date"] <= dto if dto is not None else True
    if dto is not None:
        mask &= tx["Date"] <= dto
    # UB = sum til og med dto (eller hele settet)
    scope = tx.loc[mask] if (dfrom is not None or dto is not None) else tx.loc[mask]
    series = scope.groupby("AccountID")["Amount"].sum().sort_index()
    return series

def _sum_ub(df: pd.DataFrame) -> float:
    return float(pd.to_numeric(df.get("UB", pd.Series(dtype="float64")), errors="coerce").fillna(0.0).sum()) if not df.empty else 0.0

def _top_by_abs(df: pd.DataFrame, n: int = 50) -> pd.DataFrame:
    if df.empty or "UB" not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out["_abs"] = out["UB"].abs()
    out = out.sort_values("_abs", ascending=False).drop(columns=["_abs"]).head(n)
    return out


# ---------------- Public API ----------------

def generate_saldolist(csv_dir: Path,
                       date_from: Optional[str] = None,
                       date_to: Optional[str] = None,
                       control_accounts_ar: Optional[List[str]] = None,
                       control_accounts_ap: Optional[List[str]] = None,
                       top_n: int = 50) -> Path:
    """
    Generer kombinert AR/AP saldoliste og avstemming.
    Skriver `ar_ap_saldolist.xlsx` i ../excel.
    """
    csv_dir = Path(csv_dir)
    excel_dir = _ensure_excel_dir(csv_dir)

    # 1) Sørg for at subledgers finnes (lav risiko: eksisterende fasade)
    try:
        reports.make_subledger(csv_dir, "AR", date_from=date_from, date_to=date_to)
    except Exception as e:
        print(f"[warn] AR subledger kunne ikke genereres: {e}")
    try:
        reports.make_subledger(csv_dir, "AP", date_from=date_from, date_to=date_to)
    except Exception as e:
        print(f"[warn] AP subledger kunne ikke genereres: {e}")

    # 2) Les subledger Balances
    ar_xlsx = _find_excel(csv_dir, "ar_subledger.xlsx")
    ap_xlsx = _find_excel(csv_dir, "ap_subledger.xlsx")
    ar_bal = _load_subledger_balances(ar_xlsx) if ar_xlsx else pd.DataFrame()
    ap_bal = _load_subledger_balances(ap_xlsx) if ap_xlsx else pd.DataFrame()

    # 3) Hent transaksjoner og periode
    tx = _read_transactions(csv_dir)
    header_df = None
    hpath = find_csv_file(csv_dir, "header.csv")
    if hpath:
        header_df = read_csv_safe(hpath, dtype="str")
    dfrom, dto = range_dates(header_df, date_from, date_to, tx) if not tx.empty else (None, None)

    # 4) UB kontrollkontoer fra GL
    ar_ctrl = control_accounts_ar or list(AR_CONTROL_ACCOUNTS)
    ap_ctrl = control_accounts_ap or list(AP_CONTROL_ACCOUNTS)
    ar_ub_by_acc = _ledger_ub_by_account(tx, ar_ctrl, dfrom, dto)
    ap_ub_by_acc = _ledger_ub_by_account(tx, ap_ctrl, dfrom, dto)
    ar_ub_total = float(ar_ub_by_acc.sum()) if not ar_ub_by_acc.empty else 0.0
    ap_ub_total = float(ap_ub_by_acc.sum()) if not ap_ub_by_acc.empty else 0.0

    # 5) UB fra subledger
    ar_sub_ub = _sum_ub(ar_bal)
    ap_sub_ub = _sum_ub(ap_bal)

    # 6) Avstemming
    ar_diff = ar_ub_total - ar_sub_ub
    ap_diff = ap_ub_total - ap_sub_ub

    # 7) Topp‑lister
    customers_top = _top_by_abs(ar_bal, top_n)
    suppliers_top = _top_by_abs(ap_bal, top_n)

    # 8) Skriv Excel
    out_path = excel_dir / "ar_ap_saldolist.xlsx"
    with pd.ExcelWriter(out_path, engine="xlsxwriter", datetime_format="yyyy-mm-dd",
                        engine_kwargs={"options": {"strings_to_urls": False, "strings_to_numbers": False, "strings_to_formulas": False}}) as xw:
        book = xw.book
        fmt_num = book.add_format({"num_format": "# ##0,00;[Red]-# ##0,00"})
        fmt_pct = book.add_format({"num_format": "0.00%"})
        fmt_hdr = book.add_format({"bold": True})

        # Summary
        summary_rows = [
            {"Metric": "Period From", "Value": str(dfrom.date()) if dfrom is not None else ""},
            {"Metric": "Period To", "Value": str(dto.date()) if dto is not None else ""},
            {"Metric": "AR Ledger UB (control)", "Value": ar_ub_total},
            {"Metric": "AR Subledger UB (sum)", "Value": ar_sub_ub},
            {"Metric": "AR Difference (Ledger - Subledger)", "Value": ar_diff},
            {"Metric": "AP Ledger UB (control)", "Value": ap_ub_total},
            {"Metric": "AP Subledger UB (sum)", "Value": ap_sub_ub},
            {"Metric": "AP Difference (Ledger - Subledger)", "Value": ap_diff},
        ]
        df_summary = pd.DataFrame(summary_rows)
        df_summary.to_excel(xw, sheet_name="Summary", index=False)
        ws = xw.sheets["Summary"]
        # format numeric col
        if "Value" in df_summary.columns:
            col_idx = list(df_summary.columns).index("Value")
            ws.set_column(col_idx, col_idx, 22, fmt_num)
        ws.set_column(0, 0, 36)

        # AR_Recon
        ar_recon = pd.DataFrame({
            "Type": ["AR_Total"],
            "Ledger_UB": [ar_ub_total],
            "Subledger_UB": [ar_sub_ub],
            "Difference": [ar_diff],
        })
        ar_recon.to_excel(xw, sheet_name="AR_Recon", index=False)
        ws = xw.sheets["AR_Recon"]
        ws.set_column(0, 0, 16)
        ws.set_column(1, 3, 18, fmt_num)

        if not ar_ub_by_acc.empty:
            df_acc = ar_ub_by_acc.reset_index().rename(columns={"index": "AccountID", "AccountID": "AccountID", "Amount": "UB"})
            # ensure consistent names
            if "Amount" in df_acc.columns and "UB" not in df_acc.columns:
                df_acc.rename(columns={"Amount":"UB"}, inplace=True)
            ws.write(len(ar_recon)+2, 0, "By Control Account", fmt_hdr)
            start_row = len(ar_recon)+3
            df_acc.to_excel(xw, sheet_name="AR_Recon", index=False, startrow=start_row)
            ws.set_column(0, 0, 14)
            try:
                ub_idx = list(df_acc.columns).index("UB")
                ws.set_column(ub_idx, ub_idx, 18, fmt_num)
            except Exception:
                pass

        # AP_Recon
        ap_recon = pd.DataFrame({
            "Type": ["AP_Total"],
            "Ledger_UB": [ap_ub_total],
            "Subledger_UB": [ap_sub_ub],
            "Difference": [ap_diff],
        })
        ap_recon.to_excel(xw, sheet_name="AP_Recon", index=False)
        ws = xw.sheets["AP_Recon"]
        ws.set_column(0, 0, 16)
        ws.set_column(1, 3, 18, fmt_num)

        if not ap_ub_by_acc.empty:
            df_acc = ap_ub_by_acc.reset_index().rename(columns={"index": "AccountID", "AccountID": "AccountID", "Amount": "UB"})
            if "Amount" in df_acc.columns and "UB" not in df_acc.columns:
                df_acc.rename(columns={"Amount":"UB"}, inplace=True)
            ws.write(len(ap_recon)+2, 0, "By Control Account", fmt_hdr)
            start_row = len(ap_recon)+3
            df_acc.to_excel(xw, sheet_name="AP_Recon", index=False, startrow=start_row)
            ws.set_column(0, 0, 14)
            try:
                ub_idx = list(df_acc.columns).index("UB")
                ws.set_column(ub_idx, ub_idx, 18, fmt_num)
            except Exception:
                pass

        # Customers_UB
        if not customers_top.empty:
            customers_top.to_excel(xw, sheet_name="Customers_UB", index=False)
            ws = xw.sheets["Customers_UB"]
            ws.set_column(0, 0, 18)
            ws.set_column(1, 1, 38)
            for col in ("IB","PR","UB"):
                if col in customers_top.columns:
                    idx = list(customers_top.columns).index(col)
                    ws.set_column(idx, idx, 16, fmt_num)

        # Suppliers_UB
        if not suppliers_top.empty:
            suppliers_top.to_excel(xw, sheet_name="Suppliers_UB", index=False)
            ws = xw.sheets["Suppliers_UB"]
            ws.set_column(0, 0, 18)
            ws.set_column(1, 1, 38)
            for col in ("IB","PR","UB"):
                if col in suppliers_top.columns:
                    idx = list(suppliers_top.columns).index(col)
                    ws.set_column(idx, idx, 16, fmt_num)

    print(f"[excel] Skrev AR/AP saldoliste: {out_path}")
    return out_path


# ---------------- CLI ----------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Generer AR/AP saldoliste og avstemming")
    p.add_argument("--csv-dir", required=True, help="Mappe med SAF-T CSV (transactions.csv, header.csv, accounts.csv)")
    p.add_argument("--date-from", default=None, help="Startdato (yyyy-mm-dd)", dest="date_from")
    p.add_argument("--date-to", default=None, help="Sluttdato (yyyy-mm-dd)", dest="date_to")
    p.add_argument("--top-n", type=int, default=50, help="Antall toppkunder/-leverandører")
    args = p.parse_args()
    generate_saldolist(Path(args.csv_dir), date_from=args.date_from, date_to=args.date_to, top_n=args.top_n)
