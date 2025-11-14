# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import numpy as np

from .common import (
    read_csv_any, find_csv, to_num, to_date, month_key, term_label, detect_vat_accounts
)

def build_vat_summary(outdir: Path, frequency: str = "bimonthly") -> Dict[str, pd.DataFrame]:
    """
    Lager MVA-oversikter:
      - VAT_by_code_month: MVA pr. måned og TaxCode/StandardTaxCode (nettobeløp + estimert grunnlag)
      - VAT_terms: MVA pr. termin (2-mnd default)
      - VAT_GL_27xx_by_month: GL-bevegelse på antatte MVA-konti (27xx) pr. måned
      - VAT_Reconciliation: Terminvis sammenstilling TaxLines vs GL 27xx (diff)
      - VAT_Settings: parametre brukt
    Returnerer et dict {sheetnavn: DataFrame}.
    """
    outdir = Path(outdir)

    # --- Les grunnlagstabeller ---
    tx_p = find_csv(outdir, "transactions.csv")
    acc_p = find_csv(outdir, "accounts.csv")
    tax_p = find_csv(outdir, "tax_table.csv")

    tx = read_csv_any(tx_p, dtype=str) if tx_p else None
    acc = read_csv_any(acc_p, dtype=str) if acc_p else None
    tax = read_csv_any(tax_p, dtype=str) if tax_p else None

    if tx is None or tx.empty:
        # tomt sett for alle faner
        empty = pd.DataFrame()
        return {
            "VAT_by_code_month": empty,
            "VAT_terms": empty,
            "VAT_GL_27xx_by_month": empty,
            "VAT_Reconciliation": empty,
            "VAT_Settings": pd.DataFrame([{"Error": "transactions.csv mangler/er tom"}]),
        }

    # --- Forbered data ---
    tx = to_date(tx, ["PostingDate", "TransactionDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    tx = to_num(tx, ["Debit", "Credit", "DebitTaxAmount", "CreditTaxAmount", "TaxAmount", "TaxPercentage"])
    # Nettobeløp MVA
    tx["TaxNet"] = np.where(
        tx[["DebitTaxAmount", "CreditTaxAmount"]].notna().any(axis=1),
        tx.get("DebitTaxAmount", 0.0) - tx.get("CreditTaxAmount", 0.0),
        tx.get("TaxAmount", 0.0),
    )
    # Vi bruker bare de linjene der vi faktisk har MVA-informasjon
    # (TaxType=MVA eller TaxCode/TaxPercentage er fylt).
    cond_mva = (
        (tx.get("TaxType", "").astype(str).str.upper() == "MVA")
        | tx.get("TaxCode", "").astype(str).ne("")
        | tx.get("TaxPercentage", 0.0).astype(float).ne(0.0)
    )
    mva = tx.loc[cond_mva].copy()

    # Estimert grunnlag (der vi har både TaxNet og TaxPercentage>0)
    pct = mva.get("TaxPercentage", 0.0).astype(float)
    mva["Base_est"] = np.where(
        (mva["TaxNet"].abs() > 0) & (pct.abs() > 1e-9),
        mva["TaxNet"] / (pct / 100.0),
        0.0,
    )

    # Koble StandardTaxCode hvis mulig (v1.3)
    if tax is not None and not tax.empty and "TaxCode" in tax.columns:
        use_cols = ["TaxCode", "StandardTaxCode"]
        mva = mva.merge(
            tax[use_cols].drop_duplicates(), on="TaxCode", how="left"
        )

    # Periodisering
    mva["Month"] = mva["Date"].apply(month_key)
    mva["Term"] = mva["Date"].apply(lambda d: term_label(d, frequency=frequency))

    by_code_month = (
        mva.groupby(["Month", "TaxCode", "StandardTaxCode"], dropna=False)[["Base_est", "TaxNet"]]
        .sum()
        .reset_index()
        .sort_values(["Month", "TaxCode"])
    )

    # Terminvis (uavhengig av TaxCode)
    by_term = (
        mva.groupby(["Term"], dropna=False)[["Base_est", "TaxNet"]]
        .sum()
        .reset_index()
        .sort_values(["Term"])
    )

    # --- GL-sjekk: 27xx-bevegelser pr. måned ---
    vat_accounts = detect_vat_accounts(acc)
    tx["AccountID"] = tx.get("AccountID", "").astype(str)
    tx["Amount"] = tx.get("Debit", 0.0) - tx.get("Credit", 0.0)
    is_vat_gl = tx["AccountID"].isin(vat_accounts) if vat_accounts else False
    gl_vat = tx.loc[is_vat_gl].copy()
    gl_vat["Month"] = gl_vat["Date"].apply(month_key)
    gl_vat["Term"] = gl_vat["Date"].apply(lambda d: term_label(d, frequency=frequency))

    gl_by_month = (
        gl_vat.groupby(["Month"], dropna=False)[["Amount"]]
        .sum()
        .reset_index()
        .rename(columns={"Amount": "GL_27xx"})
        .sort_values(["Month"])
    )
    gl_by_term = (
        gl_vat.groupby(["Term"], dropna=False)[["Amount"]]
        .sum()
        .reset_index()
        .rename(columns={"Amount": "GL_27xx"})
        .sort_values(["Term"])
    )

    # --- Avstemming pr. termin: TaxLines vs 27xx ---
    recon = by_term.merge(gl_by_term, on="Term", how="outer").fillna(0.0)
    recon["Diff_Tax_vs_GL"] = recon["TaxNet"] - recon["GL_27xx"]

    settings = pd.DataFrame(
        [
            {
                "VAT_frequency": frequency,
                "Tolerance": 0.01,
                "VAT_accounts_heuristic": ", ".join(vat_accounts) if vat_accounts else "(ingen funnet – 27xx/‘MVA’ i navn)",
            }
        ]
    )

    return {
        "VAT_by_code_month": by_code_month,
        "VAT_terms": by_term,
        "VAT_GL_27xx_by_month": gl_by_month,
        "VAT_Reconciliation": recon,
        "VAT_Settings": settings,
    }
