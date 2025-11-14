# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Optional, Tuple, Set, Dict
import pandas as pd
from .common import (
    read_csv_any, to_num, parse_dates, norm_acc_series, period_ym, year_term,
    find_in_outdir, pick_ar_ap_controls, load_vat_gl_config, NOK_TOL
)

# --------- GL og bilag ----------
def global_and_voucher(tx: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    t = tx.copy()
    to_num(t, ["Debit","Credit"])
    tot = pd.DataFrame([{
        "Delta": float(t["Debit"].sum() - t["Credit"].sum()),
        "OK": abs(float(t["Debit"].sum() - t["Credit"].sum())) <= 0.01
    }])
    if "VoucherID" in t.columns:
        g = t.groupby("VoucherID")[["Debit","Credit"]].sum().reset_index()
        g["Delta"] = (g["Debit"] - g["Credit"]).round(2)
        g["OK"] = g["Delta"].abs() <= 0.01
        unb = g.loc[~g["OK"]].copy()
    else:
        unb = pd.DataFrame(columns=["VoucherID","Debit","Credit","Delta","OK"])
    return tot, unb

# --------- TB vs accounts ----------
def tb_vs_accounts(tx: pd.DataFrame, acc: Optional[pd.DataFrame]) -> pd.DataFrame:
    t = tx.copy()
    t["AccountID"] = norm_acc_series(t.get("AccountID", pd.Series([], dtype=str)))
    to_num(t, ["Debit","Credit"])
    tb = t.groupby("AccountID")[["Debit","Credit"]].sum().reset_index()
    tb["GL_UB"] = tb["Debit"] - tb["Credit"]
    if acc is None or not {"ClosingDebit","ClosingCredit"}.issubset(acc.columns):
        return tb[["AccountID","GL_UB"]].sort_values("AccountID")
    a = acc.copy()
    a["AccountID"] = norm_acc_series(a["AccountID"])
    to_num(a, ["ClosingDebit","ClosingCredit"])
    a["Acc_UB"] = a["ClosingDebit"] - a["ClosingCredit"]
    out = tb.merge(a[["AccountID","AccountDescription","Acc_UB"]], on="AccountID", how="left")
    out["Diff_UB"] = (out["GL_UB"] - out["Acc_UB"]).round(2)
    out["OK"] = out["Diff_UB"].abs() <= NOK_TOL
    return out.sort_values("AccountID")

# --------- periode-kompletthet ----------
def period_completeness(tx: pd.DataFrame, dfrom, dto) -> pd.DataFrame:
    t = tx.copy()
    d = pd.to_datetime(t.get("PostingDate")).fillna(pd.to_datetime(t.get("TransactionDate")))
    t["YM"] = period_ym(d)
    months = pd.period_range(dfrom, dto, freq="M").astype(str).tolist()
    have = set(t["YM"].dropna().unique())
    rows = [{"PeriodYM": m, "HasTx": m in have, "Missing": m not in have} for m in months]
    return pd.DataFrame(rows)

# --------- duplikatkandidater ----------
def dup_candidates(tx: pd.DataFrame) -> pd.DataFrame:
    t = tx.copy()
    to_num(t, ["Debit","Credit"])
    if {"VoucherNo","JournalID","PostingDate"}.issubset(t.columns):
        grp = t.groupby(["VoucherNo","JournalID","PostingDate"])[["Debit","Credit"]].sum().reset_index()
        grp["Net"] = (grp["Debit"] - grp["Credit"]).round(2)
        cnt = t.groupby(["VoucherNo","JournalID","PostingDate"]).size().reset_index(name="Lines")
        return (grp.merge(cnt, on=["VoucherNo","JournalID","PostingDate"], how="left")
                  .query("Lines>1").sort_values(["PostingDate","JournalID","VoucherNo"]))
    return pd.DataFrame(columns=["VoucherNo","JournalID","PostingDate","Debit","Credit","Net","Lines"])

# --------- AR/AP recon ----------
def _tb_ub_and_accounts_ub(outdir: Path, ctrl: Set[str]) -> Tuple[Optional[float], Optional[float]]:
    ub_gl = None
    ub_acc = None
    tbp = Path(outdir) / "trial_balance.xlsx"
    if tbp.exists():
        try:
            tb = pd.read_excel(tbp, sheet_name="TrialBalance")
            if {"AccountID","UB"}.issubset(tb.columns):
                tb["AccountID"] = norm_acc_series(tb["AccountID"])
                to_num(tb, ["UB"])
                mask = tb["AccountID"].isin(ctrl)
                if mask.any():
                    ub_gl = float(tb.loc[mask,"UB"].sum())
        except Exception:
            pass
    acc = read_csv_any(Path(outdir) / "accounts.csv", dtype=str)
    if acc is not None and {"AccountID","ClosingDebit","ClosingCredit"}.issubset(acc.columns):
        acc["AccountID"] = norm_acc_series(acc["AccountID"])
        to_num(acc, ["ClosingDebit","ClosingCredit"])
        mask = acc["AccountID"].isin(ctrl)
        if mask.any():
            ub_acc = float((acc.loc[mask,"ClosingDebit"] - acc.loc[mask,"ClosingCredit"]).sum())
    return ub_gl, ub_acc

def _subledger_ub(excel_path: Path, sheet: str) -> Optional[float]:
    try:
        if excel_path.exists():
            df = pd.read_excel(excel_path, sheet_name=sheet)
            return float(df["UB_Amount"].sum())
    except Exception:
        pass
    return None

def ar_ap_recon(outdir: Path, ar_ctrl: Set[str], ap_ctrl: Set[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ar_sub = _subledger_ub(Path(outdir) / "ar_subledger.xlsx", "AR_Balances")
    ap_sub = _subledger_ub(Path(outdir) / "ap_subledger.xlsx", "AP_Balances")
    ar_gl, ar_acc = _tb_ub_and_accounts_ub(outdir, ar_ctrl)
    ap_gl, ap_acc = _tb_ub_and_accounts_ub(outdir, ap_ctrl)

    def row(typ, ctrl, ub_gl, ub_acc, sub):
        d = {"Type":typ, "Kontrollkonti":", ".join(sorted(ctrl)) if ctrl else "",
             "UB_GL":ub_gl, "UB_Accounts":ub_acc, "Subledger_UB":sub,
             "Avvik_GL_mot_Sub":None, "Avvik_Acc_mot_Sub":None, "Avvik_GL_mot_Acc":None}
        if (ub_gl is not None) and (sub is not None): d["Avvik_GL_mot_Sub"] = round(ub_gl - sub, 2)
        if (ub_acc is not None) and (sub is not None): d["Avvik_Acc_mot_Sub"] = round(ub_acc - sub, 2)
        if (ub_gl is not None) and (ub_acc is not None): d["Avvik_GL_mot_Acc"] = round(ub_gl - ub_acc, 2)
        return d
    return pd.DataFrame([row("AR", ar_ctrl, ar_gl, ar_acc, ar_sub)]), \
           pd.DataFrame([row("AP", ap_ctrl, ap_gl, ap_acc, ap_sub)])

# --------- MVA (måneds- og terminvis) ----------
def vat_views(tx: pd.DataFrame, tax: Optional[pd.DataFrame], acc: Optional[pd.DataFrame], outdir: Path) -> Dict[str,pd.DataFrame]:
    t = tx.copy()
    to_num(t, ["Debit","Credit","DebitTaxAmount","CreditTaxAmount","TaxAmount","TaxPercentage"])
    t["Date"] = pd.to_datetime(t.get("PostingDate")).fillna(pd.to_datetime(t.get("TransactionDate")))
    t["VAT"]  = t.get("DebitTaxAmount", 0.0) - t.get("CreditTaxAmount", 0.0)
    if ("DebitTaxAmount" not in t.columns) and ("CreditTaxAmount" not in t.columns):
        t["VAT"] = t.get("TaxAmount", 0.0)
    if "TaxType" in t.columns:
        t = t.loc[t["TaxType"].str.upper()=="MVA"].copy()
    if tax is not None and {"TaxCode","StandardTaxCode"}.issubset(tax.columns) and "TaxCode" in t.columns:
        t = t.merge(tax[["TaxCode","StandardTaxCode"]].drop_duplicates(), on="TaxCode", how="left")

    t["Month"] = period_ym(t["Date"]); t["Term"] = year_term(t["Date"])
    by_code_m = (t.groupby(["Month","TaxCode","StandardTaxCode"])["VAT"].sum()
                   .reset_index().sort_values(["Month","TaxCode"]))
    by_code_t = (t.groupby(["Term","TaxCode","StandardTaxCode"])["VAT"].sum()
                   .reset_index().sort_values(["Term","TaxCode"]))

    all27, taxonly, cfg = load_vat_gl_config(outdir, acc)
    g = tx.copy()
    to_num(g, ["Debit","Credit"])
    g["AccountID"] = norm_acc_series(g.get("AccountID", pd.Series([], dtype=str)))
    g["Date"] = pd.to_datetime(g.get("PostingDate")).fillna(pd.to_datetime(g.get("TransactionDate")))
    g["Month"] = period_ym(g["Date"]); g["Term"] = year_term(g["Date"])
    g["GL_Amount"] = g["Debit"] - g["Credit"]

    gl_all_m = (g.loc[g["AccountID"].isin(all27)].groupby(["Month"])["GL_Amount"].sum()
                  .reset_index().rename(columns={"GL_Amount":"GL_All27xx"}))
    gl_all_t = (g.loc[g["AccountID"].isin(all27)].groupby(["Term"])["GL_Amount"].sum()
                  .reset_index().rename(columns={"GL_Amount":"GL_All27xx"}))
    gl_tax_m = (g.loc[g["AccountID"].isin(taxonly)].groupby(["Month"])["GL_Amount"].sum()
                  .reset_index().rename(columns={"GL_Amount":"GL_TaxOnly"}))
    gl_tax_t = (g.loc[g["AccountID"].isin(taxonly)].groupby(["Term"])["GL_Amount"].sum()
                  .reset_index().rename(columns={"GL_Amount":"GL_TaxOnly"}))

    mvat = by_code_m.groupby("Month")["VAT"].sum().reset_index().rename(columns={"VAT":"VAT_TaxLines"})
    tvat = by_code_t.groupby("Term")["VAT"].sum().reset_index().rename(columns={"VAT":"VAT_TaxLines"})

    chk_m = mvat.merge(gl_all_m, on="Month", how="outer").fillna(0.0)
    chk_m["Diff"] = (chk_m["VAT_TaxLines"] - chk_m["GL_All27xx"]).round(2)
    chk_m["OK"]   = chk_m["Diff"].abs() <= NOK_TOL

    chk_t = tvat.merge(gl_all_t, on="Term", how="outer").fillna(0.0)
    chk_t["Diff"] = (chk_t["VAT_TaxLines"] - chk_t["GL_All27xx"]).round(2)
    chk_t["OK"]   = chk_t["Diff"].abs() <= NOK_TOL

    recon_m = (mvat.merge(gl_all_m, on="Month", how="outer")
                    .merge(gl_tax_m, on="Month", how="outer")).fillna(0.0)
    recon_m["Diff_All"]     = (recon_m["VAT_TaxLines"] - recon_m["GL_All27xx"]).round(2)
    recon_m["OK_All"]       = recon_m["Diff_All"].abs() <= NOK_TOL
    recon_m["Diff_TaxOnly"] = (recon_m["VAT_TaxLines"] - recon_m["GL_TaxOnly"]).round(2)
    recon_m["OK_TaxOnly"]   = recon_m["Diff_TaxOnly"].abs() <= NOK_TOL

    recon_t = (tvat.merge(gl_all_t, on="Term", how="outer")
                    .merge(gl_tax_t, on="Term", how="outer")).fillna(0.0)
    recon_t["Diff_All"]     = (recon_t["VAT_TaxLines"] - recon_t["GL_All27xx"]).round(2)
    recon_t["OK_All"]       = recon_t["Diff_All"].abs() <= NOK_TOL
    recon_t["Diff_TaxOnly"] = (recon_t["VAT_TaxLines"] - recon_t["GL_TaxOnly"]).round(2)
    recon_t["OK_TaxOnly"]   = recon_t["Diff_TaxOnly"].abs() <= NOK_TOL

    return {
        "VAT_ByCode_Month": by_code_m,
        "VAT_ByCode_Term":  by_code_t,
        "VAT_GL_Check_Month": chk_m,
        "VAT_GL_Check_Term":  chk_t,
        "VAT_Recon_Month":  recon_m,
        "VAT_Recon_Term":   recon_t,
        "VAT_GL_Config":    cfg,
    }

# --------- Terminvis MVA (melding-stil) ----------
def mva_term_report(outdir: Path) -> Dict[str, pd.DataFrame]:
    tx = read_csv_any((find_in_outdir(outdir, "transactions.csv") or Path(outdir)/"transactions.csv"))
    tt = read_csv_any((find_in_outdir(outdir, "tax_table.csv")   or Path(outdir)/"tax_table.csv"))
    acc= read_csv_any((find_in_outdir(outdir, "accounts.csv")    or Path(outdir)/"accounts.csv"))
    if tx is None or tx.empty: raise FileNotFoundError("transactions.csv mangler/tom")
    parse_dates(tx, ["PostingDate","TransactionDate"])
    tx["Date"] = pd.to_datetime(tx.get("PostingDate")).fillna(pd.to_datetime(tx.get("TransactionDate")))
    to_num(tx, ["DebitTaxAmount","CreditTaxAmount","TaxAmount","Debit","Credit"])
    tx["VAT"] = tx.get("DebitTaxAmount", 0.0) - tx.get("CreditTaxAmount", 0.0)
    if ("DebitTaxAmount" not in tx.columns) and ("CreditTaxAmount" not in tx.columns):
        tx["VAT"] = tx.get("TaxAmount", 0.0)
    if "TaxType" in tx.columns:
        tx = tx.loc[tx["TaxType"].str.upper()=="MVA"].copy()
    if tt is not None and "TaxCode" in tx.columns and "TaxCode" in tt.columns:
        tt2 = tt[["TaxCode","StandardTaxCode"]].drop_duplicates()
        m = tx.merge(tt2, on="TaxCode", how="left")
    else:
        m = tx.copy()
        if "StandardTaxCode" not in m.columns and "TaxCode" in m.columns:
            m["StandardTaxCode"] = m["TaxCode"]
    m["Term"] = year_term(m["Date"])
    # summer per term og StandardTaxCode
    by_code = (m.groupby(["Term","StandardTaxCode","TaxCode"])["VAT"]
                 .sum().reset_index().rename(columns={"VAT":"VAT_TaxLines"}))
    # GL tax-only per term
    all27, taxonly, cfg_view = load_vat_gl_config(outdir, acc)
    g = tx.copy(); g["AccountID"] = norm_acc_series(g.get("AccountID", pd.Series([], dtype=str)))
    g["Date"] = pd.to_datetime(g.get("PostingDate")).fillna(pd.to_datetime(g.get("TransactionDate")))
    g["Term"] = year_term(g["Date"]); g["GL_Amount"] = g["Debit"] - g["Credit"]
    gl_tax = (g.loc[g["AccountID"].isin(taxonly)].groupby("Term")["GL_Amount"].sum()
                .reset_index().rename(columns={"GL_Amount":"GL_TaxOnly"}))
    net_tax = (by_code.groupby("Term")["VAT_TaxLines"].sum().reset_index()
                 .rename(columns={"VAT_TaxLines":"VAT_TaxLines_Net"}))
    # del opp i utgående/inngående (enkel – summer basert på standardkode)
    def _toi(v):
        try: return int(str(v).rstrip(".0"))
        except: return None
    bc = by_code.copy(); bc["std_i"] = bc["StandardTaxCode"].map(_toi)
    OUT = {3,31,32,33}; IN = {1,11,12,13,21,22}
    out_s = (bc.loc[bc["std_i"].isin(OUT)].groupby("Term")["VAT_TaxLines"].sum()
               .reset_index().rename(columns={"VAT_TaxLines":"VAT_Out"}))
    in_s  = (bc.loc[bc["std_i"].isin(IN)].groupby("Term")["VAT_TaxLines"].sum()
               .reset_index().rename(columns={"VAT_TaxLines":"VAT_In"}))
    summary = net_tax.merge(out_s, on="Term", how="left").merge(in_s, on="Term", how="left").merge(gl_tax, on="Term", how="left").fillna(0.0)
    summary["Diff_vs_GL_TaxOnly"] = (summary["VAT_TaxLines_Net"] - summary["GL_TaxOnly"]).round(2)
    summary["OK"] = summary["Diff_vs_GL_TaxOnly"].abs() <= NOK_TOL
    return {
        "MVA_Term_ByCode": by_code.sort_values(["Term","StandardTaxCode","TaxCode"]),
        "MVA_Term_Summary": summary.sort_values("Term"),
        "VAT_GL_Config": cfg_view
    }
