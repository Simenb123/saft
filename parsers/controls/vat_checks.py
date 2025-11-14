# -*- coding: utf-8 -*-
"""
controls/vat_checks.py – MVA-oversikter (kode, måned/termin) og avstemming mot GL.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import Dict, Set, List, Tuple
from .common import to_num, period_ym, year_term, norm_acc_series, VAT_PREFIXES, NOK_TOL, read_csv_any

def _load_vat_gl_config(outdir: Path, accounts: pd.DataFrame|None) -> Tuple[Set[str], Set[str], pd.DataFrame]:
    """Les valgfri 'vat_gl_accounts.csv' (AccountID + Category {'tax','settlement','exclude'})."""
    all27: Set[str] = set(); tax_only: Set[str] = set(); rows: List[dict] = []
    if accounts is not None and "AccountID" in accounts.columns:
        a = accounts.copy(); a["AccountID"] = norm_acc_series(a["AccountID"])
        if "AccountDescription" not in a.columns: a["AccountDescription"] = ""
        all27 |= set(a.loc[a["AccountID"].str.startswith(VAT_PREFIXES), "AccountID"])
        desc=a["AccountDescription"].str.lower()
        settlement=(desc.str.contains("oppgj",na=False)|desc.str.contains("oppgjor",na=False)|
                    desc.str.contains("oppgjør",na=False)|desc.str.contains("interim",na=False))
        tax_only |= set(a.loc[a["AccountID"].isin(all27) & ~settlement, "AccountID"])
        for _,r in a.loc[a["AccountID"].isin(all27),["AccountID","AccountDescription"]].iterrows():
            rows.append({"AccountID":r["AccountID"],"AccountDescription":r["AccountDescription"],
                         "Category":("tax" if r["AccountID"] in tax_only else "settlement/other")})
    p = outdir / "vat_gl_accounts.csv"
    if p.exists():
        cfg = read_csv_any(p, dtype=str)
        if cfg is not None and "AccountID" in cfg.columns:
            cfg["AccountID"] = norm_acc_series(cfg["AccountID"])
            catcol=None
            for c in cfg.columns:
                if c.strip().lower() in {"category","role","type"}:
                    catcol=c; break
            if catcol:
                for _,r in cfg.iterrows():
                    acc=str(r["AccountID"])
                    cat=str(r[catcol]).strip().lower()
                    all27.add(acc)
                    if cat in {"tax","mva","calc"}:          tax_only.add(acc)
                    elif cat in {"settlement","oppgjør","oppgjor","interim"}: tax_only.discard(acc)
                    elif cat in {"exclude"}:                 all27.discard(acc); tax_only.discard(acc)
                    rows.append({"AccountID":acc,"AccountDescription":"","Category":cat})
            else:
                for acc in cfg["AccountID"].tolist():
                    acc=str(acc); all27.add(acc); tax_only.add(acc)
                    rows.append({"AccountID":acc,"AccountDescription":"","Category":"tax"})
    view = pd.DataFrame(rows).drop_duplicates().sort_values("AccountID") if rows else \
           pd.DataFrame([{"Info":"Ingen vat_gl_accounts.csv – heuristikk brukt (all27xx, ekskl. oppgjør/interim)."}])
    return all27, tax_only, view

def build_vat_views(tx: pd.DataFrame, tax: pd.DataFrame|None, acc: pd.DataFrame|None, outdir: Path) -> Dict[str, pd.DataFrame]:
    """Returnerer alle MVA-ark: VAT_ByCode_*, VAT_GL_Check_*, VAT_Recon_*, VAT_GL_Config."""
    # lag MVA-tx-serie
    t = tx.copy()
    to_num(t, ["Debit","Credit","DebitTaxAmount","CreditTaxAmount","TaxAmount","TaxPercentage"])
    t["Date"] = pd.to_datetime(t.get("PostingDate")).fillna(pd.to_datetime(t.get("TransactionDate")))
    t["VAT"]  = t.get("DebitTaxAmount",0.0) - t.get("CreditTaxAmount",0.0)
    if ("DebitTaxAmount" not in t.columns) and ("CreditTaxAmount" not in t.columns):
        t["VAT"] = t.get("TaxAmount", 0.0)
    if "TaxType" in t.columns:
        t = t.loc[t["TaxType"].str.upper()=="MVA"].copy()
    else:
        t = t.loc[t["VAT"].abs()>0].copy()
    if tax is not None and {"TaxCode","StandardTaxCode"}.issubset(tax.columns) and "TaxCode" in t.columns:
        t = t.merge(tax[["TaxCode","StandardTaxCode"]].drop_duplicates(), on="TaxCode", how="left")

    t["Month"] = period_ym(t["Date"])
    t["Term"]  = year_term(t["Date"])
    by_code_m = (t.groupby(["Month","TaxCode","StandardTaxCode"])["VAT"].sum()
                   .reset_index().sort_values(["Month","TaxCode"]))
    by_code_t = (t.groupby(["Term","TaxCode","StandardTaxCode"])["VAT"].sum()
                   .reset_index().sort_values(["Term","TaxCode"]))

    # GL-serier
    all27, taxonly, cfg = _load_vat_gl_config(outdir, acc)

    g = tx.copy(); to_num(g, ["Debit","Credit"])
    g["AccountID"] = norm_acc_series(g.get("AccountID", pd.Series([], dtype=str)))
    g["Date"] = pd.to_datetime(g.get("PostingDate")).fillna(pd.to_datetime(g.get("TransactionDate")))
    g["Month"] = period_ym(g["Date"]); g["Term"] = year_term(g["Date"])
    g["GL_Amount"] = g["Debit"] - g["Credit"]

    gl_all_m = (g.loc[g["AccountID"].isin(all27)]
                  .groupby(["Month"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount":"GL_All27xx"}))
    gl_all_t = (g.loc[g["AccountID"].isin(all27)]
                  .groupby(["Term"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount":"GL_All27xx"}))
    gl_tax_m = (g.loc[g["AccountID"].isin(taxonly)]
                  .groupby(["Month"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount":"GL_TaxOnly"}))
    gl_tax_t = (g.loc[g["AccountID"].isin(taxonly)]
                  .groupby(["Term"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount":"GL_TaxOnly"}))

    # aggreger VAT-taxlines
    mvat = by_code_m.groupby("Month")["VAT"].sum().reset_index().rename(columns={"VAT":"VAT_TaxLines"})
    tvat = by_code_t.groupby("Term")["VAT"].sum().reset_index().rename(columns={"VAT":"VAT_TaxLines"})

    # bakoverkompatibel sjekk (All 27xx)
    chk_m = mvat.merge(gl_all_m, on="Month", how="outer").fillna(0.0)
    chk_m["Diff"] = (chk_m["VAT_TaxLines"] - chk_m["GL_All27xx"]).round(2)
    chk_m["OK"]   = chk_m["Diff"].abs() <= NOK_TOL

    chk_t = tvat.merge(gl_all_t, on="Term", how="outer").fillna(0.0)
    chk_t["Diff"] = (chk_t["VAT_TaxLines"] - chk_t["GL_All27xx"]).round(2)
    chk_t["OK"]   = chk_t["Diff"].abs() <= NOK_TOL

    # ny recon-tabell med BEGGE serier
    recon_m = (mvat.merge(gl_all_m,on="Month",how="outer")
                    .merge(gl_tax_m,on="Month",how="outer")).fillna(0.0)
    recon_m["Diff_All"]     = (recon_m["VAT_TaxLines"] - recon_m["GL_All27xx"]).round(2)
    recon_m["OK_All"]       = recon_m["Diff_All"].abs() <= NOK_TOL
    recon_m["Diff_TaxOnly"] = (recon_m["VAT_TaxLines"] - recon_m["GL_TaxOnly"]).round(2)
    recon_m["OK_TaxOnly"]   = recon_m["Diff_TaxOnly"].abs() <= NOK_TOL

    recon_t = (tvat.merge(gl_all_t,on="Term",how="outer")
                    .merge(gl_tax_t,on="Term",how="outer")).fillna(0.0)
    recon_t["Diff_All"]     = (recon_t["VAT_TaxLines"] - recon_t["GL_All27xx"]).round(2)
    recon_t["OK_All"]       = recon_t["Diff_All"].abs() <= NOK_TOL
    recon_t["Diff_TaxOnly"] = (recon_t["VAT_TaxLines"] - recon_t["GL_TaxOnly"]).round(2)
    recon_t["OK_TaxOnly"]   = recon_t["Diff_TaxOnly"].abs() <= NOK_TOL

    return {
        "VAT_ByCode_Month":    by_code_m,
        "VAT_ByCode_Term":     by_code_t,
        "VAT_GL_Check_Month":  chk_m,      # All 27xx
        "VAT_GL_Check_Term":   chk_t,      # All 27xx
        "VAT_Recon_Month":     recon_m,    # All + Tax-only
        "VAT_Recon_Term":      recon_t,    # All + Tax-only
        "VAT_GL_Config":       cfg,
    }
