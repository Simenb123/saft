# -*- coding: utf-8 -*-
"""
controls/mva_term_recon.py – Terminvis mva-avstemming fra SAF‑T.

Produserer tre datasett:
  - MVA_Term_ByCode:  Sum mva pr Termin x StandardTaxCode (og TaxCode)
  - MVA_Term_Summary: Summert per Termin (Utgående, Inngående, Netto, GL_TaxOnly, Avvik)
  - MVA_Settlement:   Oppgjørsposteringer rundt terminslutt (kontrollinformasjon)

Skrives til control_report.xlsx av run_all_checks.py.
"""
from __future__ import annotations
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple, List
import pandas as pd
import numpy as np

NOK_TOL = 1.00
VAT_PREFIXES = ("27",)  # GL 27xx ~ mva-konti

# ---- små hjelpere ----
def _read_csv(p: Path, dtype=str) -> Optional[pd.DataFrame]:
    if not p or not p.exists():
        return None
    for sep in (",", ";", "\t"):
        try:
            return pd.read_csv(p, dtype=dtype, keep_default_na=False, sep=sep)
        except Exception:
            continue
    try:
        return pd.read_csv(p, dtype=dtype, keep_default_na=False)
    except Exception:
        return None

def _to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = (df[c].astype(str)
                        .str.replace("\u00A0", "", regex=False)
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
    if t.endswith(".0"): t = t[:-2]
    t = t.lstrip("0") or "0"
    return t

def _norm_acc_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(_norm_acc)

def _period_month(d: pd.Series) -> pd.Series:
    return pd.to_datetime(d, errors="coerce").dt.to_period("M").astype(str)

def _period_term(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d, errors="coerce")
    def lab(x):
        if pd.isna(x): return ""
        t = (x.month + 1) // 2  # T1..T6
        return f"{x.year}-T{t}"
    return d.apply(lab)

def _find(outdir: Path, name: str) -> Optional[Path]:
    # søk i outdir, outdir/csv og rekursivt
    for p in [outdir / name, outdir / "csv" / name]:
        if p.exists():
            return p
    for base in [outdir, outdir.parent, Path.cwd()]:
        try:
            for fp in base.rglob(name):
                return fp
        except Exception:
            pass
    return None

# ---- les GL-kontokonfig for MVA ----
def _load_vat_gl_config(outdir: Path, accounts: Optional[pd.DataFrame]) -> Tuple[Set[str], Set[str], pd.DataFrame]:
    """
    Returnerer (all27xx_ids, tax_only_ids, config_view).
    Heuristikk + valgfri vat_gl_accounts.csv:
      - Category: 'tax' (med i TaxOnly), 'settlement' (utelates fra TaxOnly), 'exclude'
    """
    all27: Set[str] = set()
    tax_only: Set[str] = set()
    cfg_rows: List[Dict[str, str]] = []

    if accounts is not None and "AccountID" in accounts.columns:
        a = accounts.copy()
        a["AccountID"] = _norm_acc_series(a["AccountID"])
        if "AccountDescription" not in a.columns:
            a["AccountDescription"] = ""
        # alle 27xx
        all27 |= set(a.loc[a["AccountID"].str.startswith(VAT_PREFIXES), "AccountID"])
        # heuristisk tax_only = 27xx minus oppgjør/interim
        desc = a["AccountDescription"].str.lower()
        settlement_mask = (
            desc.str.contains("oppgj", na=False) |
            desc.str.contains("oppgjor", na=False) |
            desc.str.contains("oppgjør", na=False) |
            desc.str.contains("interim", na=False)
        )
        tax_only |= set(a.loc[a["AccountID"].isin(all27) & ~settlement_mask, "AccountID"])
        for _, r in a.loc[a["AccountID"].isin(all27), ["AccountID","AccountDescription"]].iterrows():
            cat = "tax" if r["AccountID"] in tax_only else "settlement/other"
            cfg_rows.append({"AccountID": r["AccountID"], "AccountDescription": r["AccountDescription"], "Category": cat})

    p = outdir / "vat_gl_accounts.csv"
    if p.exists():
        cfg = _read_csv(p, dtype=str)
        if cfg is not None and "AccountID" in cfg.columns:
            cfg["AccountID"] = _norm_acc_series(cfg["AccountID"])
            catcol = None
            for c in cfg.columns:
                if c.strip().lower() in {"category","role","type"}:
                    catcol = c; break
            for _, r in cfg.iterrows():
                acc = _norm_acc(r["AccountID"])
                cat = (str(r[catcol]).strip().lower() if catcol else "tax")
                all27.add(acc)
                if cat in {"tax","mva","calc"}:
                    tax_only.add(acc)
                elif cat in {"settlement","oppgjør","oppgjor","interim"}:
                    tax_only.discard(acc)
                elif cat in {"exclude"}:
                    all27.discard(acc); tax_only.discard(acc)
                cfg_rows.append({"AccountID": acc, "AccountDescription": "", "Category": cat})

    cfg_view = pd.DataFrame(cfg_rows).drop_duplicates().sort_values("AccountID") if cfg_rows else \
               pd.DataFrame([{"Info": "Ingen vat_gl_accounts.csv – brukte heuristikk (all 27xx, ekskl. oppgjør/interim i TaxOnly)."}])
    return all27, tax_only, cfg_view

# ---- mapping grupper (for oppsummering) ----
# Vi aggregerer per StandardTaxCode; i tillegg grupperer vi i blokker (utgående, inngående, import, mv.)
OUT_CODES  = {3, 31, 32, 33}
IN_CODES   = {1, 11, 12, 13, 21, 22}
IMPORT_BASE_CODES = {81, 82, 83, 84}
OTHER_CODES = {5, 52, 6, 51, 86, 87, 88, 89, 91, 92, 85}

def _code_to_int(x) -> Optional[int]:
    try:
        # StandardTaxCode kan være streng – forsøk parse int
        return int(str(x).strip().rstrip(".0"))
    except Exception:
        return None

def build_mva_term_report(outdir: Path) -> Dict[str, pd.DataFrame]:
    outdir = Path(outdir)
    tx = _read_csv(_find(outdir, "transactions.csv") or outdir/"transactions.csv")
    tt = _read_csv(_find(outdir, "tax_table.csv") or outdir/"tax_table.csv")
    acc = _read_csv(_find(outdir, "accounts.csv") or outdir/"accounts.csv")
    hdr = _read_csv(_find(outdir, "header.csv") or outdir/"header.csv")

    if tx is None or tx.empty:
        raise FileNotFoundError("transactions.csv mangler/tom")
    _parse_dates(tx, ["PostingDate","TransactionDate"])
    tx["Date"] = pd.to_datetime(tx.get("PostingDate")).fillna(pd.to_datetime(tx.get("TransactionDate")))

    # beregn VAT-beløp på linjene
    _to_num(tx, ["DebitTaxAmount","CreditTaxAmount","TaxAmount","Debit","Credit"])
    if "DebitTaxAmount" in tx.columns or "CreditTaxAmount" in tx.columns:
        tx["VAT"] = tx.get("DebitTaxAmount", 0.0) - tx.get("CreditTaxAmount", 0.0)
    else:
        tx["VAT"] = tx.get("TaxAmount", 0.0)

    # hold oss til mva-linjer
    if "TaxType" in tx.columns:
        tx = tx.loc[tx["TaxType"].str.upper() == "MVA"].copy()

    # join med tax_table for StandardTaxCode
    if tt is not None and not tt.empty and "TaxCode" in tx.columns and "TaxCode" in tt.columns:
        cols = ["TaxCode","StandardTaxCode"]
        extra = [c for c in ["Description","Name"] if c in tt.columns]
        tt2 = tt[cols + extra].drop_duplicates()
        m = tx.merge(tt2, on="TaxCode", how="left")
    else:
        m = tx.copy()
        if "StandardTaxCode" not in m.columns and "TaxCode" in m.columns:
            m["StandardTaxCode"] = m["TaxCode"]  # beste vi kan gjøre

    # periodisering
    m["Month"] = _period_month(m["Date"])
    m["Term"]  = _period_term(m["Date"])

    # konverter StandardTaxCode til heltall (der det lar seg gjøre)
    m["StdCode"] = m["StandardTaxCode"].apply(_code_to_int)

    # ---- 1) ByCode per Term ----
    by_code = (m.groupby(["Term","StdCode","TaxCode"])["VAT"]
                 .sum().reset_index().sort_values(["Term","StdCode","TaxCode"]))
    by_code = by_code.rename(columns={"StdCode": "StandardTaxCode",
                                      "VAT": "VAT_TaxLines"})

    # ---- 2) GL mva-serier per Term (TaxOnly) ----
    all27, taxonly, cfg_view = _load_vat_gl_config(outdir, acc)
    g = tx.copy()
    _to_num(g, ["Debit","Credit"])
    g["AccountID"] = _norm_acc_series(g.get("AccountID", pd.Series([], dtype=str)))
    g["Date"] = pd.to_datetime(g.get("PostingDate")).fillna(pd.to_datetime(g.get("TransactionDate")))
    g["Term"] = _period_term(g["Date"])
    g["GL_Amount"] = g["Debit"] - g["Credit"]
    gl_taxonly = (g.loc[g["AccountID"].isin(taxonly)]
                    .groupby("Term")["GL_Amount"].sum().reset_index()
                    .rename(columns={"GL_Amount":"GL_TaxOnly"}))

    # netto mva fra taxlines per term (utgående - inngående)
    # (retning ligger i signen på VAT)
    net_taxlines = (by_code.groupby("Term")["VAT_TaxLines"].sum()
                        .reset_index().rename(columns={"VAT_TaxLines":"VAT_TaxLines_Net"}))

    # ---- 3) Sammendrag per Term ----
    # splitt utgående/inngående basert på standardkodene
    by_code["is_out"] = by_code["StandardTaxCode"].isin(list(OUT_CODES))
    by_code["is_in"]  = by_code["StandardTaxCode"].isin(list(IN_CODES))
    by_term_out = (by_code.loc[by_code["is_out"]]
                        .groupby("Term")["VAT_TaxLines"].sum()
                        .reset_index().rename(columns={"VAT_TaxLines":"VAT_Out"}))
    by_term_in  = (by_code.loc[by_code["is_in"]]
                        .groupby("Term")["VAT_TaxLines"].sum()
                        .reset_index().rename(columns={"VAT_TaxLines":"VAT_In"}))

    summary = net_taxlines.merge(by_term_out, on="Term", how="left") \
                          .merge(by_term_in,  on="Term", how="left") \
                          .merge(gl_taxonly,  on="Term", how="left") \
                          .fillna(0.0)
    summary["Diff_vs_GL_TaxOnly"] = (summary["VAT_TaxLines_Net"] - summary["GL_TaxOnly"]).round(2)
    summary["OK"] = summary["Diff_vs_GL_TaxOnly"].abs() <= NOK_TOL

    # ---- 4) Oppgjør – finn posteringer på oppgjørskonti rundt terminslutt ----
    # heuristikk: konti med "oppgjør" i teksten eller spesifikke 274x/275x
    settlement_ids: Set[str] = set()
    if acc is not None and "AccountID" in acc.columns:
        a = acc.copy()
        a["AccountID"] = _norm_acc_series(a["AccountID"])
        desc = a.get("AccountDescription","").astype(str).str.lower()
        mask = (a["AccountID"].str.match(r"^27(4|5)\d$")) | \
               desc.str.contains("oppgj", na=False) | desc.str.contains("oppgjor", na=False) | desc.str.contains("oppgjør", na=False)
        settlement_ids = set(a.loc[mask, "AccountID"])
    # trekk ut GL på disse kontoene, oppsummert per Term (posteringsmåned)
    gl_settlement = (g.loc[g["AccountID"].isin(settlement_ids)]
                       .groupby("Term")["GL_Amount"].sum()
                       .reset_index().rename(columns={"GL_Amount":"GL_Settlement"}))
    # slå på summary
    summary = summary.merge(gl_settlement, on="Term", how="left").fillna(0.0)

    # oppgjørskontroll: netto mva for en termin avspeiles ofte i neste periodes oppgjør
    # (enkel visning – vi viser både Term og GL_Settlement i samme Term for oversikt)
    settlement_view = gl_settlement.copy()
    return {
        "MVA_Term_ByCode": by_code,
        "MVA_Term_Summary": summary,
        "MVA_Settlement": settlement_view,
        "VAT_GL_Config": cfg_view,
    }
