# app/parsers/saft_trial_balance.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Dict, Tuple
import re
import pandas as pd

from .saft_common import (
    _read_csv_safe, _find_csv_file, _find_accounts_file, _complete_accounts_file,
    _parse_dates, _to_num, _norm_acc_series, _range_dates, _sanitize_df_for_excel,
    _format_sheet_xlsxwriter, _has_value
)

# --------- NS/Mappings (enkle, robuste plukkere) ---------

def _normalize_colname(s: str) -> str:
    s = s.strip().lower()
    s = s.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")

def _pick_col(cols: List[str], patterns: List[str]) -> Optional[str]:
    norm = {c: _normalize_colname(c) for c in cols}
    for pat in patterns:
        p = _normalize_colname(pat)
        for c, cn in norm.items():
            if p == cn or p in cn:
                return c
    return None

def _read_ns_mapping_any(outdir: Path) -> Optional[pd.DataFrame]:
    # 1) csv/mapping_accounts.csv
    p = _find_csv_file(outdir, "mapping_accounts.csv")
    if p is not None and p.exists():
        df = _read_csv_safe(p, dtype=str)
        if df is not None and not df.empty:
            cols = df.columns.tolist()
            acc = _pick_col(cols, ["AccountID", "Konto"])
            ns_acc = _pick_col(cols, ["NOSpecAccountID","NSAccountID","NoSpecAccountID","NæringsspesifikasjonKonto","NaeringsspesifikasjonKonto"])
            ns_acc_name = _pick_col(cols, ["NOSpecAccountName","NSAccountName","NoSpecAccountName","NæringsspesifikasjonKontonavn","NaeringsspesifikasjonKontonavn"])
            ns_cat = _pick_col(cols, ["NOSpecCategory","NOSpecCategoryCode","NSCategory","NSCategoryCode"])
            ns_cat_name = _pick_col(cols, ["NOSpecCategoryName","NSCategoryName"])
            take = [c for c in [acc, ns_acc, ns_acc_name, ns_cat, ns_cat_name] if c]
            if acc and (ns_acc or ns_cat):
                out = df[take].copy()
                out.rename(columns={
                    acc: "AccountID",
                    ns_acc or "": "NS_AccountID",
                    ns_acc_name or "": "NS_AccountName",
                    ns_cat or "": "NS_CategoryCode",
                    ns_cat_name or "": "NS_CategoryName",
                }, inplace=True)
                out["Source"] = "mapping_accounts.csv"
                return out
    # 2) excel/mapping_overview.xlsx
    p2 = _find_csv_file(outdir, "mapping_overview.xlsx")
    if p2 is not None and p2.exists():
        try:
            xl = pd.ExcelFile(p2)
            for sn in xl.sheet_names:
                df = xl.parse(sn, dtype=str)
                if df is None or df.empty:
                    continue
                cols = df.columns.tolist()
                acc = _pick_col(cols, ["AccountID", "Konto"])
                ns_acc = _pick_col(cols, ["NOSpecAccountID","NSAccountID","NoSpecAccountID","NæringsspesifikasjonKonto"])
                ns_acc_name = _pick_col(cols, ["NOSpecAccountName","NSAccountName"])
                ns_cat = _pick_col(cols, ["NOSpecCategory","NOSpecCategoryCode","NSCategory","NSCategoryCode"])
                ns_cat_name = _pick_col(cols, ["NOSpecCategoryName","NSCategoryName"])
                take = [c for c in [acc, ns_acc, ns_acc_name, ns_cat, ns_cat_name] if c]
                if acc and (ns_acc or ns_cat):
                    out = df[take].copy()
                    out.rename(columns={
                        acc: "AccountID",
                        ns_acc or "": "NS_AccountID",
                        ns_acc_name or "": "NS_AccountName",
                        ns_cat or "": "NS_CategoryCode",
                        ns_cat_name or "": "NS_CategoryName",
                    }, inplace=True)
                    out["Source"] = f"mapping_overview.xlsx::{sn}"
                    return out
        except Exception:
            pass
    return None

def _build_ns_mapping_sheet(out_accounts: pd.DataFrame, outdir: Path) -> pd.DataFrame:
    ns = _read_ns_mapping_any(outdir)
    base = out_accounts[["AccountID"] + (["AccountDescription"] if "AccountDescription" in out_accounts.columns else [])].copy()
    if ns is None or ns.empty:
        base["Info"] = "Ingen næringsspesifikasjons-mapping funnet"
        return base
    ns["AccountID"] = ns["AccountID"].astype(str).map(str)
    base["AccountID"] = base["AccountID"].astype(str).map(str)
    merged = base.merge(ns, on="AccountID", how="left")
    cols = ["AccountID"]
    if "AccountDescription" in merged.columns: cols.append("AccountDescription")
    for c in ["NS_AccountID","NS_AccountName","NS_CategoryCode","NS_CategoryName","Source"]:
        if c in merged.columns: cols.append(c)
    return merged[cols].drop_duplicates()

# --------- GL-statistikk ---------

def _build_gl_stats(gl_df: pd.DataFrame, accounts: Optional[pd.DataFrame]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if gl_df is None or gl_df.empty:
        return pd.DataFrame(), pd.DataFrame([{"Metric": "Info", "Value": "Ingen GL-linjer i perioden"}])

    work = gl_df.copy()
    for col in ["Debit", "Credit", "Amount"]:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0.0)
    if "Amount" not in work.columns:
        work["Amount"] = work.get("Debit", 0.0) - work.get("Credit", 0.0)

    key_cols = [c for c in ["VoucherID","VoucherNo","DocumentNumber","JournalID"] if c in work.columns]
    work["VoucherKey"] = work[key_cols].astype(str).agg("|".join, axis=1) if key_cols else ""

    g = work.groupby("AccountID").agg(
        Lines=("Amount", "size"),
        TotalDebit=("Debit", "sum"),
        TotalCredit=("Credit", "sum"),
        Net=("Amount", "sum"),
        MeanAmount=("Amount", "mean"),
        MedianAmount=("Amount", "median"),
        StdAmount=("Amount", lambda s: float(s.std(ddof=0))),
        MaxAmount=("Amount", "max"),
        MinAmount=("Amount", "min"),
        FirstDate=("Date", "min"),
        LastDate=("Date", "max"),
        UniqueVouchers=("VoucherKey", "nunique"),
    )
    debit_cnt  = (work["Debit"]  > 0).groupby(work["AccountID"]).sum()
    credit_cnt = (work["Credit"] > 0).groupby(work["AccountID"]).sum()
    g["DebitCount"]  = debit_cnt
    g["CreditCount"] = credit_cnt
    g["AbsSum"]      = work.groupby("AccountID")["Amount"].apply(lambda s: s.abs().sum())
    g["MaxAbsAmount"]= work.groupby("AccountID")["Amount"].apply(lambda s: s.abs().max())
    g["ActiveDays"]  = (g["LastDate"] - g["FirstDate"]).dt.days.add(1).fillna(0).astype(int)
    g = g.reset_index()
    g["AccountID"] = _norm_acc_series(g["AccountID"].astype(str))

    if accounts is not None and not accounts.empty and {"AccountID","AccountDescription"}.issubset(accounts.columns):
        acc = accounts[["AccountID","AccountDescription"]].copy()
        acc["AccountID"] = _norm_acc_series(acc["AccountID"].astype(str))
        g = g.merge(acc, on="AccountID", how="left")

    g = g.sort_values(["AbsSum","Net"], ascending=False)

    ov: List[Dict[str, object]] = []
    ov.append({"Metric": "Period transactions (GL only)", "Value": int(len(work))})
    ov.append({"Metric": "Distinct accounts",             "Value": int(work["AccountID"].nunique())})
    ov.append({"Metric": "Distinct vouchers",             "Value": int(work["VoucherKey"].nunique()) if key_cols else 0})
    ov.append({"Metric": "Debit count",                   "Value": int((work["Debit"] > 0).sum())})
    ov.append({"Metric": "Credit count",                  "Value": int((work["Credit"] > 0).sum())})
    ov.append({"Metric": "Sum debit",                     "Value": float(work["Debit"].sum())})
    ov.append({"Metric": "Sum credit",                    "Value": float(work["Credit"].sum())})
    ov.append({"Metric": "Net sum (Debit−Credit)",        "Value": float(work["Amount"].sum())})
    ov.append({"Metric": "Mean amount",                   "Value": float(work["Amount"].mean())})
    ov.append({"Metric": "Median amount",                 "Value": float(work["Amount"].median())})
    ov.append({"Metric": "Std amount",                    "Value": float(work["Amount"].std(ddof=0))})
    ov.append({"Metric": "Max abs transaction",           "Value": float(work["Amount"].abs().max())})
    acc_abs = g["AbsSum"].sum(); top10_abs = g["AbsSum"].head(10).sum() if not g.empty else 0.0
    ov.append({"Metric": "Top10 accounts share of AbsMovement", "Value": float((top10_abs / acc_abs) if acc_abs else 0.0)})
    overview = pd.DataFrame(ov)

    return g, overview

# --------- General Ledger (fil) ---------

def make_general_ledger(outdir: Path, include_all: bool = False) -> Path:
    tx_path = _find_csv_file(outdir, "transactions.csv")
    tx = _read_csv_safe(tx_path, dtype=str) if tx_path else None
    if tx is None or tx.empty:
        raise FileNotFoundError("transactions.csv mangler")

    tx = _parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    if "AccountID" in tx.columns:
        tx["AccountID"] = _norm_acc_series(tx["AccountID"].astype(str))
    tx = _to_num(tx, ["Debit", "Credit", "TaxAmount"])
    tx["Amount"] = tx["Debit"] - tx["Credit"]

    if "IsGL" in tx.columns:
        gl_df = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()
    else:
        gl_df = tx.copy()
    gl_df = gl_df.sort_values(["AccountID", "Date", "VoucherID", "VoucherNo"], kind="mergesort")
    gl_df = _sanitize_df_for_excel(gl_df, stringify_dates=True)

    path = Path(outdir) / "general_ledger.xlsx"
    with pd.ExcelWriter(
        path,
        engine="xlsxwriter",
        datetime_format="yyyy-mm-dd",
        engine_kwargs={"options": {
            "strings_to_urls": False, "strings_to_numbers": False,
            "strings_to_formulas": False, "nan_inf_to_errors": True
        }},
    ) as xw:
        gl_df.to_excel(xw, index=False, sheet_name="GeneralLedger")
        _format_sheet_xlsxwriter(xw, "GeneralLedger", gl_df, freeze_cols=1)
        if include_all:
            all_df = _sanitize_df_for_excel(tx.sort_values(["AccountID","Date","VoucherID","VoucherNo"], kind="mergesort"), stringify_dates=True)
            all_df.to_excel(xw, index=False, sheet_name="AllTransactions")
            _format_sheet_xlsxwriter(xw, "AllTransactions", all_df, freeze_cols=1)
    return path

# --------- Trial Balance (m/ GL_Stats, NS_Mapping og AR/AP-rekon) ---------

def _agg_ib_pr_ub(df: pd.DataFrame, dfrom: pd.Timestamp, dto: pd.Timestamp) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["AccountID","IB","PR","UB"])
    ib = df.loc[df["Date"] < dfrom].groupby("AccountID")["Amount"].sum().rename("IB")
    pr = df.loc[(df["Date"] >= dfrom) & (df["Date"] <= dto)].groupby("AccountID")["Amount"].sum().rename("PR")
    ub = df.loc[df["Date"] <= dto].groupby("AccountID")["Amount"].sum().rename("UB")
    return pd.concat([ib, pr, ub], axis=1).fillna(0.0).reset_index()

def _make_recon(gl_tx: pd.DataFrame, accounts: Optional[pd.DataFrame],
                dfrom, dto, party_col: str, label: str) -> pd.DataFrame:
    # Party-linjer (reskontro)
    party_tx = gl_tx.loc[_has_value(gl_tx.get(party_col, pd.Series([], dtype=str)))].copy()
    party = _agg_ib_pr_ub(party_tx, dfrom, dto)
    party.rename(columns={"IB": "Party_IB", "PR": "Party_PR", "UB": "Party_UB"}, inplace=True)
    # Totale GL-linjer
    gl_sum = _agg_ib_pr_ub(gl_tx, dfrom, dto)
    gl_sum.rename(columns={"IB": "GL_IB", "PR": "GL_PR", "UB": "GL_UB"}, inplace=True)
    # Slå sammen på konti hvor det finnes part-linjer
    if party.empty and gl_sum.empty:
        return pd.DataFrame(columns=["AccountID","AccountDescription","Type",
                                     "Party_IB","Party_PR","Party_UB",
                                     "GL_IB","GL_PR","GL_UB",
                                     "Partyless_IB","Partyless_PR","Partyless_UB"])
    recon = party.merge(gl_sum, on="AccountID", how="outer").fillna(0.0)
    recon["Partyless_IB"] = recon["GL_IB"] - recon["Party_IB"]
    recon["Partyless_PR"] = recon["GL_PR"] - recon["Party_PR"]
    recon["Partyless_UB"] = recon["GL_UB"] - recon["Party_UB"]
    recon["Type"] = label
    # Kontonavn
    if accounts is not None and {"AccountID","AccountDescription"}.issubset(accounts.columns):
        acc = accounts[["AccountID","AccountDescription"]].copy()
        acc["AccountID"] = _norm_acc_series(acc["AccountID"].astype(str))
        recon = recon.merge(acc, on="AccountID", how="left")
    cols = ["AccountID","AccountDescription","Type",
            "Party_IB","Party_PR","Party_UB","GL_IB","GL_PR","GL_UB",
            "Partyless_IB","Partyless_PR","Partyless_UB"]
    for c in cols:
        if c not in recon.columns:
            recon[c] = 0.0 if c not in ("AccountID","AccountDescription","Type") else ""
    return recon[cols].sort_values(["Type","AccountID"])

def make_trial_balance(outdir: Path, date_from: Optional[str] = None, date_to: Optional[str] = None) -> Path:
    try:
        _complete_accounts_file(outdir)
    except Exception:
        pass

    tx_path = _find_csv_file(outdir, "transactions.csv")
    hdr_path = _find_csv_file(outdir, "header.csv")
    tx  = _read_csv_safe(tx_path, dtype=str) if tx_path else None
    hdr = _read_csv_safe(hdr_path, dtype=str) if hdr_path else None
    if tx is None or tx.empty:
        raise FileNotFoundError("transactions.csv mangler")

    tx = _parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    if "AccountID" in tx.columns:
        tx["AccountID"] = _norm_acc_series(tx["AccountID"].astype(str))
    tx = _to_num(tx, ["Debit", "Credit"])
    tx["Amount"] = tx["Debit"] - tx["Credit"]   # Sikker beløpskolonne
    if "IsGL" in tx.columns:
        tx = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()

    dfrom, dto = _range_dates(hdr, date_from, date_to, tx)

    def _sum(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame({"AccountID": [], "GL_Amount": []})
        g = df.groupby("AccountID")[["Debit", "Credit"]].sum().reset_index()
        g["GL_Amount"] = g["Debit"] - g["Credit"]
        return g[["AccountID", "GL_Amount"]]

    ib_gl = _sum(tx[tx["Date"] < dfrom]).rename(columns={"GL_Amount": "GL_IB"})
    pr_gl = _sum(tx[(tx["Date"] >= dfrom) & (tx["Date"] <= dto)]).rename(columns={"GL_Amount": "GL_PR"})
    ub_gl = _sum(tx[tx["Date"] <= dto]).rename(columns={"GL_Amount": "GL_UB"})
    tb_gl = ub_gl.merge(ib_gl, on="AccountID", how="outer").merge(pr_gl, on="AccountID", how="outer").fillna(0.0)
    tb = tb_gl.copy()

    acc = _find_accounts_file(outdir)
    if acc is not None and "AccountID" in acc.columns:
        acc = acc.copy()
        acc["AccountID"] = _norm_acc_series(acc["AccountID"])
        if {"OpeningDebit","OpeningCredit","ClosingDebit","ClosingCredit"}.issubset(acc.columns):
            acc = _to_num(acc, ["OpeningDebit","OpeningCredit","ClosingDebit","ClosingCredit"])
            acc["IB_OpenNet"] = acc["OpeningDebit"] - acc["OpeningCredit"]
            acc["UB_CloseNet"] = acc["ClosingDebit"] - acc["ClosingCredit"]
            acc["PR_Accounts"] = acc["UB_CloseNet"] - acc["IB_OpenNet"]
            cols = ["AccountID", *(["AccountDescription"] if "AccountDescription" in acc.columns else []),
                    "IB_OpenNet","PR_Accounts","UB_CloseNet","OpeningDebit","OpeningCredit","ClosingDebit","ClosingCredit"]
            tb = acc[cols].merge(tb_gl, on="AccountID", how="left")

    if {"IB_OpenNet","GL_IB"}.issubset(tb.columns):  tb["Diff_IB"] = tb["GL_IB"] - tb["IB_OpenNet"]
    if {"PR_Accounts","GL_PR"}.issubset(tb.columns):  tb["Diff_PR"] = tb["GL_PR"] - tb["PR_Accounts"]
    if {"UB_CloseNet","GL_UB"}.issubset(tb.columns):  tb["Diff_UB"] = tb["GL_UB"] - tb["UB_CloseNet"]

    first_cols = [c for c in ["AccountID","AccountDescription"] if c in tb.columns]
    value_cols: List[str] = []
    for pair in [("IB_OpenNet","GL_IB","Diff_IB"),
                 ("PR_Accounts","GL_PR","Diff_PR"),
                 ("UB_CloseNet","GL_UB","Diff_UB")]:
        for c in pair:
            if c in tb.columns and c not in value_cols:
                value_cols.append(c)
    optional_cols = [c for c in ["OpeningDebit","OpeningCredit","ClosingDebit","ClosingCredit"] if c in tb.columns]
    out_cols = first_cols + value_cols + optional_cols
    out = tb[out_cols].copy()

    # GL-statistikk for valgt periode
    gl_period = tx[(tx["Date"] >= dfrom) & (tx["Date"] <= dto)].copy()
    gl_stats, gl_overview = _build_gl_stats(gl_period, acc)

    # AR/AP‑reconciliation (kontrollkonti)
    ar_recon = _make_recon(gl_period, acc, dfrom, dto, "CustomerID", "AR")
    ap_recon = _make_recon(gl_period, acc, dfrom, dto, "SupplierID", "AP")

    path = Path(outdir) / "trial_balance.xlsx"
    with pd.ExcelWriter(
        path,
        engine="xlsxwriter",
        datetime_format="yyyy-mm-dd",
        engine_kwargs={"options": {
            "strings_to_urls": False, "strings_to_numbers": False,
            "strings_to_formulas": False, "nan_inf_to_errors": True
        }},
    ) as xw:
        out_sorted = out.sort_values("AccountID").copy()
        for col in out_sorted.select_dtypes(include=["float","float64"]).columns:
            out_sorted[col] = out_sorted[col].round(2)
        out_sorted = _sanitize_df_for_excel(out_sorted)
        out_sorted.to_excel(xw, index=False, sheet_name="TrialBalance")
        _format_sheet_xlsxwriter(xw, "TrialBalance", out_sorted, freeze_cols=1)

        simple_cols = [c for c in ["AccountID","AccountDescription"] if c in out.columns]
        simple_df = out[simple_cols].copy() if simple_cols else pd.DataFrame()
        has_open_close = {"OpeningDebit","OpeningCredit","ClosingDebit","ClosingCredit"}.issubset(out.columns)
        if has_open_close:
            odeb = pd.to_numeric(out.get("OpeningDebit", 0), errors="coerce").fillna(0.0)
            ocred= pd.to_numeric(out.get("OpeningCredit",0), errors="coerce").fillna(0.0)
            cdeb = pd.to_numeric(out.get("ClosingDebit", 0), errors="coerce").fillna(0.0)
            ccred= pd.to_numeric(out.get("ClosingCredit",0), errors="coerce").fillna(0.0)
            simple_df["IB"] = (odeb - ocred).round(2)
            simple_df["UB"] = (cdeb - ccred).round(2)
            simple_df["Movement"] = (simple_df["UB"] - simple_df["IB"]).round(2)
        else:
            ib_col = "IB_OpenNet" if "IB_OpenNet" in out.columns else "GL_IB"
            pr_col = "PR_Accounts" if "PR_Accounts" in out.columns else "GL_PR"
            ub_col = "UB_CloseNet" if "UB_CloseNet" in out.columns else "GL_UB"
            simple_df["IB"]       = pd.to_numeric(out.get(ib_col, 0), errors="coerce").round(2)
            simple_df["Movement"] = pd.to_numeric(out.get(pr_col, 0), errors="coerce").round(2)
            simple_df["UB"]       = pd.to_numeric(out.get(ub_col, 0), errors="coerce").round(2)
        if "AccountID" in simple_df.columns:
            simple_df = simple_df.sort_values("AccountID")
        simple_df = _sanitize_df_for_excel(simple_df)
        simple_df.to_excel(xw, index=False, sheet_name="SimpleTrialBalance")
        _format_sheet_xlsxwriter(xw, "SimpleTrialBalance", simple_df, freeze_cols=1)

        ns_map = _build_ns_mapping_sheet(out_sorted, outdir)
        ns_map = _sanitize_df_for_excel(ns_map)
        ns_map.to_excel(xw, index=False, sheet_name="NS_Mapping")
        _format_sheet_xlsxwriter(xw, "NS_Mapping", ns_map, freeze_cols=1)

        gl_stats_out = _sanitize_df_for_excel(gl_stats, stringify_dates=True)
        gl_stats_out.to_excel(xw, index=False, sheet_name="GL_Stats")
        _format_sheet_xlsxwriter(xw, "GL_Stats", gl_stats_out, freeze_cols=1)

        gl_over_out = _sanitize_df_for_excel(gl_overview)
        gl_over_out.to_excel(xw, index=False, sheet_name="GL_Overview")
        _format_sheet_xlsxwriter(xw, "GL_Overview", gl_over_out, freeze_cols=0)

        # NYTT: AR/AP‑Reconciliation
        if not ar_recon.empty:
            ar_out = _sanitize_df_for_excel(ar_recon)
            ar_out.to_excel(xw, index=False, sheet_name="AR_Recon")
            _format_sheet_xlsxwriter(xw, "AR_Recon", ar_out, freeze_cols=1)
        if not ap_recon.empty:
            ap_out = _sanitize_df_for_excel(ap_recon)
            ap_out.to_excel(xw, index=False, sheet_name="AP_Recon")
            _format_sheet_xlsxwriter(xw, "AP_Recon", ap_out, freeze_cols=1)

    print(f"[excel] TB: la til 'NS_Mapping', 'GL_Stats', 'GL_Overview' og AR/AP‑Recon -> {path}")
    return path
