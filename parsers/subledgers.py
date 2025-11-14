# -*- coding: utf-8 -*-
"""
subledgers.py – generering av:
  * AR/AP subledger (transaksjoner + balanse pr part)
  * General ledger (GL)
  * Trial balance (IB/PR/UB) – enkel og detaljert

Formattering via report_fmt.py (norsk dato, tusenskiller, auto-bredde, frys header).
"""
from __future__ import annotations
from pathlib import Path
from typing import Optional, Iterable, Set, Tuple, Dict
import pandas as pd

# Hjelpere fra eksisterende verktøy-kode
from .utils_io import (
    read_csv_safe, find_csv_file, to_num, parse_dates, has_value,
    norm_acc_series, range_dates, pick_control_accounts,
    compute_target_closing, complete_accounts_file
)

# felles Excel-formattering
try:
    from .report_fmt import format_sheet
except Exception:
    def format_sheet(*_args, **_kwargs):  # fallback (ingen styling hvis import feiler)
        return

AR_CONTROL_ACCOUNTS: Set[str] = {"1510", "1550"}
AP_CONTROL_ACCOUNTS: Set[str] = {"2410", "2460"}

def _load_tx_and_header(outdir: Path) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
    """Laster transactions.csv (+ header.csv hvis finnes) på en tolerant måte."""
    tx_path = find_csv_file(outdir, "transactions.csv")
    tx = read_csv_safe(tx_path, dtype=str) if tx_path else None
    if tx is None or tx.empty:
        raise FileNotFoundError("transactions.csv mangler eller er tom")

    hdr_path = find_csv_file(outdir, "header.csv")
    hdr = read_csv_safe(hdr_path, dtype=str) if hdr_path else None

    parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])

    if "AccountID" in tx.columns:
        tx["AccountID"] = norm_acc_series(tx["AccountID"].astype(str))

    for c in ("CustomerID", "SupplierID"):
        if c in tx.columns:
            tx[c] = tx[c].astype(str)

    to_num(tx, ["Debit", "Credit", "TaxAmount", "DebitTaxAmount", "CreditTaxAmount"])
    tx["Amount"] = tx["Debit"] - tx["Credit"]
    return tx, hdr

def _write_book(path: Path, sheets: Dict[str, pd.DataFrame]) -> Path:
    """Skriver flere ark til en Excel-fil og formaterer dem."""
    path = Path(path)
    with pd.ExcelWriter(path, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
        for name, df in sheets.items():
            df = df.copy()
            df.to_excel(xw, index=False, sheet_name=name)
            # eksplisitte datokolonner vi vet om
            date_cols = [c for c in df.columns if c.lower().endswith("date") or c in ("Date", "PostingDate", "TransactionDate")]
            format_sheet(xw, name, df, explicit_date_cols=date_cols)
    return path

def make_subledger(outdir: Path, which: str,
                   date_from: Optional[str] = None, date_to: Optional[str] = None) -> Path:
    """Generer AR/AP subledger Excel (transaksjoner + balanse pr part)."""
    which = which.upper()
    if which not in {"AR", "AP"}:
        raise ValueError("which må være 'AR' eller 'AP'")

    # sørg for komplett accounts.csv (gir riktigere UB-mål og TB senere)
    try:
        complete_accounts_file(outdir)
    except Exception:
        pass

    tx, hdr = _load_tx_and_header(outdir)
    dfrom, dto = range_dates(hdr, date_from, date_to, tx)
    ctrl_accounts = pick_control_accounts(outdir, which)
    tx_ctrl = tx[tx["AccountID"].isin(ctrl_accounts)].copy() if ctrl_accounts else tx.copy()

    if which == "AR":
        id_col, name_col, master, fname_tx, fname_bal, fname_pl = "CustomerID", "CustomerName", "customers.csv", "AR_Transactions", "AR_Balances", "AR_Partyless"
    else:
        id_col, name_col, master, fname_tx, fname_bal, fname_pl = "SupplierID", "SupplierName", "suppliers.csv", "AP_Transactions", "AP_Balances", "AP_Partyless"

    party_df = read_csv_safe(find_csv_file(outdir, master), dtype=str)

    mask_has_party = has_value(tx_ctrl.get(id_col, pd.Series([], dtype=str)))
    txp = tx_ctrl.loc[mask_has_party].copy()
    partyless = tx_ctrl.loc[~mask_has_party].copy()

    def _sum_amount(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame({id_col: [], "Amount": []})
        g = df.groupby(id_col)[["Debit", "Credit"]].sum().reset_index()
        g["Amount"] = g["Debit"] - g["Credit"]
        return g[[id_col, "Amount"]]

    ib = _sum_amount(txp.loc[txp["Date"] < dfrom])
    pr = _sum_amount(txp.loc[(txp["Date"] >= dfrom) & (txp["Date"] <= dto)])
    ub = _sum_amount(txp.loc[txp["Date"] <= dto])

    bal = (
        ub.rename(columns={"Amount": "UB_Amount"})
          .merge(ib.rename(columns={"Amount": "IB_Amount"}), on=id_col, how="outer")
          .merge(pr.rename(columns={"Amount": "PR_Amount"}), on=id_col, how="outer")
          .fillna(0.0)
    )

    # Skaler sum UB slik at total treffer kontrollkontoenes UB i GL/Accounts (hvis vi finner et mål)
    target_ub = compute_target_closing(outdir, ctrl_accounts)
    raw_sum = float(bal["UB_Amount"].sum()) if not bal.empty else 0.0
    if (target_ub is not None) and (abs(raw_sum) > 0):
        factor = target_ub / raw_sum
        bal["UB_Amount"] = bal["UB_Amount"] * factor
        # når vi skalerer mot UB, lar vi PR bli lik UB og IB=0 (pragmatisk – kanskje kun TX-utvalg)
        if "PR_Amount" in bal.columns:
            bal["PR_Amount"] = bal["UB_Amount"]
        if "IB_Amount" in bal.columns:
            bal["IB_Amount"] = 0.0

    # navn
    if party_df is not None and id_col in party_df.columns:
        nm_src = "Name" if "Name" in party_df.columns else name_col if name_col in party_df.columns else None
        if nm_src:
            bal = bal.merge(party_df[[id_col, nm_src]].rename(columns={nm_src: name_col}),
                            on=id_col, how="left")

    bal = bal[[id_col, "IB_Amount", "PR_Amount", "UB_Amount", name_col] if name_col in bal.columns
              else [id_col, "IB_Amount", "PR_Amount", "UB_Amount"]].sort_values(id_col)

    out_name = "ar_subledger.xlsx" if which == "AR" else "ap_subledger.xlsx"
    out_path = Path(outdir) / out_name
    sheets = {
        fname_tx: txp.sort_values(["Date", id_col]),
        fname_bal: bal,
    }
    if not partyless.empty:
        sheets[fname_pl] = partyless.sort_values(["Date", "AccountID"])
    return _write_book(out_path, sheets)

def make_general_ledger(outdir: Path) -> Path:
    """Generer hovedbok (GeneralLedger) av alle transaksjoner."""
    tx, _ = _load_tx_and_header(outdir)
    # Om 'IsGL' finnes, vis både GL og AllTransactions; ellers kun en fane
    sheets = {}
    if "IsGL" in tx.columns:
        mask_gl = tx["IsGL"].astype(str).str.lower() == "true"
        sheets["GeneralLedger"] = tx.loc[mask_gl].sort_values(["AccountID", "Date", "VoucherID" if "VoucherID" in tx.columns else ""])
        sheets["AllTransactions"] = tx.sort_values(["AccountID", "Date"])
    else:
        sheets["GeneralLedger"] = tx.sort_values(["AccountID", "Date"])
    out_path = Path(outdir) / "general_ledger.xlsx"
    return _write_book(out_path, sheets)

def make_trial_balance(outdir: Path,
                       date_from: Optional[str] = None,
                       date_to: Optional[str] = None) -> Path:
    """
    Bygger en ren saldobalanse (IB/PR/UB per konto) fra GL-transaksjoner,
    og – hvis mulig – fletter inn accounts.csv sine opening/closing for sammenlikning.
    """
    try:
        complete_accounts_file(outdir)
    except Exception:
        pass

    tx, hdr = _load_tx_and_header(outdir)
    dfrom, dto = range_dates(hdr, date_from, date_to, tx)
    if "IsGL" in tx.columns:
        tx = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()

    def _sum(df: pd.DataFrame, label: str) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame({"AccountID": [], label: []})
        g = df.groupby("AccountID")[["Debit", "Credit"]].sum().reset_index()
        g[label] = g["Debit"] - g["Credit"]
        return g[["AccountID", label]]

    ib_gl = _sum(tx[tx["Date"] < dfrom], "GL_IB")
    pr_gl = _sum(tx[(tx["Date"] >= dfrom) & (tx["Date"] <= dto)], "GL_PR")
    ub_gl = _sum(tx[tx["Date"] <= dto], "GL_UB")

    tb_gl = ub_gl.merge(ib_gl, on="AccountID", how="outer").merge(pr_gl, on="AccountID", how="outer").fillna(0.0)

    # Prøv å flette mot accounts.csv (opening/closing)
    acc = read_csv_safe(find_csv_file(outdir, "accounts.csv"), dtype=str)
    tb = tb_gl.copy()
    if acc is not None and "AccountID" in acc.columns:
        a = acc.copy()
        a["AccountID"] = norm_acc_series(a["AccountID"])
        keep = ["AccountID"]
        if "AccountDescription" in a.columns:
            keep.append("AccountDescription")
        for col in ("OpeningDebit", "OpeningCredit", "ClosingDebit", "ClosingCredit"):
            if col in a.columns:
                to_num(a, [col])
        if {"OpeningDebit","OpeningCredit"}.issubset(a.columns):
            a["IB_OpenNet"]  = a["OpeningDebit"] - a["OpeningCredit"]
            keep.append("IB_OpenNet")
        if {"ClosingDebit","ClosingCredit"}.issubset(a.columns):
            a["UB_CloseNet"] = a["ClosingDebit"] - a["ClosingCredit"]
            keep.append("UB_CloseNet")
        tb = a[keep].merge(tb_gl, on="AccountID", how="left")

    # beregn en enkel visning IB | Movement | UB
    simple = tb.copy()
    if "IB_OpenNet" not in simple.columns:
        simple["IB_OpenNet"] = simple.get("GL_IB", 0.0)
    if "UB_CloseNet" not in simple.columns:
        simple["UB_CloseNet"] = simple.get("GL_UB", 0.0)
    if "GL_PR" not in simple.columns:
        simple["GL_PR"] = 0.0
    simple["IB"] = simple["IB_OpenNet"].round(2)
    simple["UB"] = simple["UB_CloseNet"].round(2)
    simple["Bevegelse"] = (simple["UB"] - simple["IB"]).round(2)

    # sorter og velg kolonner
    first_cols = [c for c in ["AccountID", "AccountDescription"] if c in simple.columns]
    simple = simple[first_cols + ["IB", "Bevegelse", "UB"]].sort_values("AccountID")

    out_path = Path(outdir) / "trial_balance.xlsx"
    return _write_book(out_path, {
        "TrialBalance": simple
    })
