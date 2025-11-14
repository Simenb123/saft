# -*- coding: utf-8 -*-
"""
controls/gl_checks.py – GL- og hovedboksrelaterte kontroller.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from .common import to_num, norm_acc_series, CENT_TOL, NOK_TOL, \
                    load_tb_ub_and_accounts_ub, read_subledger_ub

def global_and_voucher(tx: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Global debet=kredit + ubalanserte bilag."""
    t = tx.copy(); to_num(t, ["Debit","Credit"])
    delta = float(t["Debit"].sum() - t["Credit"].sum())
    tot = pd.DataFrame([{"Delta": delta, "OK": abs(delta) <= CENT_TOL}])
    if "VoucherID" in t.columns:
        g = t.groupby("VoucherID")[["Debit","Credit"]].sum().reset_index()
        g["Delta"] = (g["Debit"] - g["Credit"]).round(2)
        g["OK"] = g["Delta"].abs() <= CENT_TOL
        unb = g.loc[~g["OK"]].copy()
    else:
        unb = pd.DataFrame(columns=["VoucherID","Debit","Credit","Delta","OK"])
    return tot, unb

def tb_vs_accounts(tx: pd.DataFrame, acc: pd.DataFrame|None) -> pd.DataFrame:
    """Sammenlign TB (GL-summer) med Accounts Closing UB."""
    t = tx.copy(); t["AccountID"] = norm_acc_series(t.get("AccountID", pd.Series([], dtype=str)))
    to_num(t, ["Debit","Credit"])
    tb = t.groupby("AccountID")[["Debit","Credit"]].sum().reset_index()
    tb["GL_UB"] = tb["Debit"] - tb["Credit"]
    if acc is None or not {"ClosingDebit","ClosingCredit"}.issubset(acc.columns):
        return tb[["AccountID","GL_UB"]].sort_values("AccountID")
    a = acc.copy(); a["AccountID"] = norm_acc_series(a["AccountID"]); to_num(a, ["ClosingDebit","ClosingCredit"])
    a["Acc_UB"] = a["ClosingDebit"] - a["ClosingCredit"]
    out = tb.merge(a[["AccountID","AccountDescription","Acc_UB"]], on="AccountID", how="left")
    out["Diff_UB"] = (out["GL_UB"] - out["Acc_UB"]).round(2)
    out["OK"] = out["Diff_UB"].abs() <= NOK_TOL
    return out.sort_values("AccountID")

def ar_ap_recon(outdir: Path, ar_ctrl: set[str], ap_ctrl: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Avstemming: Subledger UB mot UB_GL (TB) og mot UB_Accounts (accounts.csv)."""
    ar_sub = read_subledger_ub(outdir / "ar_subledger.xlsx", "AR_Balances")
    ap_sub = read_subledger_ub(outdir / "ap_subledger.xlsx", "AP_Balances")
    ar_gl, ar_acc = load_tb_ub_and_accounts_ub(outdir, set(ar_ctrl))
    ap_gl, ap_acc = load_tb_ub_and_accounts_ub(outdir, set(ap_ctrl))

    def row(tt, ctrl, ub_gl, ub_acc, sub):
        d = {"Type":tt,"Kontrollkonti":", ".join(sorted(ctrl)) if ctrl else "",
             "UB_GL":ub_gl,"UB_Accounts":ub_acc,"Subledger_UB":sub,
             "Avvik_GL_mot_Sub":None,"Avvik_Acc_mot_Sub":None,"Avvik_GL_mot_Acc":None}
        if (ub_gl is not None) and (sub is not None): d["Avvik_GL_mot_Sub"]=round(ub_gl-sub,2)
        if (ub_acc is not None) and (sub is not None): d["Avvik_Acc_mot_Sub"]=round(ub_acc-sub,2)
        if (ub_gl is not None) and (ub_acc is not None): d["Avvik_GL_mot_Acc"]=round(ub_gl-ub_acc,2)
        return d

    return pd.DataFrame([row("AR", ar_ctrl, ar_gl, ar_acc, ar_sub)]), \
           pd.DataFrame([row("AP", ap_ctrl, ap_gl, ap_acc, ap_sub)])
