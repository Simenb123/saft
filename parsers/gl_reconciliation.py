# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
from .common import (
    read_csv_safe, find_csv_file, parse_dates, to_num, has_value, norm_acc,
    pick_control_accounts, compute_target_closing, AR_CONTROL_ACCOUNTS, AP_CONTROL_ACCOUNTS
)

def _sum_partyless(base: Path, which: str, dto: pd.Timestamp, ctrl_accounts) -> float:
    tx = read_csv_safe(find_csv_file(base, "transactions.csv"), dtype=str)
    if tx is None or tx.empty:
        return 0.0
    parse_dates(tx, ["TransactionDate","PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    tx["AccountID"] = tx["AccountID"].astype(str).map(norm_acc)
    to_num(tx, ["Debit","Credit"])
    tx = tx.loc[(~tx["Date"].isna()) & (tx["Date"] <= dto)]
    tx = tx.loc[tx["AccountID"].isin(ctrl_accounts)].copy()
    if which.upper() == "AR":
        mask = has_value(tx.get("CustomerID", pd.Series([], dtype=str)))
    else:
        mask = has_value(tx.get("SupplierID", pd.Series([], dtype=str)))
    partless = tx.loc[~mask]
    if partless.empty:
        return 0.0
    grp = partless.groupby("AccountID")[["Debit","Credit"]].sum().reset_index()
    grp["Amount"] = grp["Debit"] - grp["Credit"]
    return float(grp["Amount"].sum())

def _dto_from_data(base: Path) -> pd.Timestamp:
    tx = read_csv_safe(find_csv_file(base, "transactions.csv"), dtype=str)
    if tx is None or tx.empty:
        return pd.Timestamp.max
    parse_dates(tx, ["TransactionDate","PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    return tx["Date"].dropna().max() if "Date" in tx.columns else pd.Timestamp.max

def check_gl_vs_reskontro(base: Path) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """Sammenligner UB i kontoplan/trial balance mot reskontrosummer (AR/AP)."""
    details: Dict[str, pd.DataFrame] = {}
    dto = _dto_from_data(base)

    rows: List[dict] = []
    for typ, defaults in [("AR", AR_CONTROL_ACCOUNTS), ("AP", AP_CONTROL_ACCOUNTS)]:
        ctrl = pick_control_accounts(base, typ) or defaults
        # UB i kontoplan/trial balance for kontrollkonti
        closing = compute_target_closing(base, ctrl) or 0.0
        # Reskontro-UB (helst fra subledger; ellers aggreger direkte)
        sl = None
        x = {"bal": None, "partyless": None}
        try:
            from .common import load_subledger_xlsx
            x = load_subledger_xlsx(base, typ)
            sl = x["bal"]
        except Exception:
            sl = None
        if sl is not None and not sl.empty and "UB_Amount" in sl.columns:
            res_ub = float(pd.to_numeric(sl["UB_Amount"], errors="coerce").fillna(0.0).sum())
        else:
            # fallback: aggreger på CustomerID/SupplierID
            tx = read_csv_safe(find_csv_file(base, "transactions.csv"), dtype=str)
            if tx is None or tx.empty:
                res_ub = 0.0
            else:
                parse_dates(tx, ["TransactionDate","PostingDate"])
                tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
                tx["AccountID"] = tx["AccountID"].astype(str).map(norm_acc)
                to_num(tx, ["Debit","Credit"])
                tx = tx.loc[(~tx["Date"].isna()) & (tx["Date"] <= dto)]
                tx = tx.loc[tx["AccountID"].isin(ctrl)].copy()
                if typ == "AR":
                    mask = has_value(tx.get("CustomerID", pd.Series([], dtype=str)))
                    grp = tx.loc[mask].groupby("CustomerID")[["Debit","Credit"]].sum().reset_index()
                else:
                    mask = has_value(tx.get("SupplierID", pd.Series([], dtype=str)))
                    grp = tx.loc[mask].groupby("SupplierID")[["Debit","Credit"]].sum().reset_index()
                if grp.empty:
                    res_ub = 0.0
                else:
                    grp["Amount"] = grp["Debit"] - grp["Credit"]
                    res_ub = float(grp["Amount"].sum())
        partyless = _sum_partyless(base, typ, dto, ctrl)
        rows.append({
            "Type": typ,
            "ControlAccounts": ", ".join(sorted(ctrl)),
            "ClosingNet": closing,
            "ReskontroUB": res_ub,
            "Partyless": partyless,
            "Difference": closing - res_ub - partyless,
        })
        # details for partyless pr konto
        if x.get("partyless") is not None and not x["partyless"].empty:
            df = x["partyless"].copy()
            to_num(df, ["Debit","Credit"])
            df["Amount"] = df["Debit"] - df["Credit"]
            dsum = df.groupby("AccountID")["Amount"].sum().reset_index().sort_values("AccountID")
            details[f"{typ}_Partyless"] = dsum
    summary = pd.DataFrame(rows)
    return summary, details
