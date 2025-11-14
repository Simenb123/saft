# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
from .common import read_csv_safe, find_csv_file, to_num

def check_unknown_tax_codes(base: Path) -> pd.DataFrame:
    tx = read_csv_safe(find_csv_file(base, "transactions.csv"), dtype=str)
    tt = read_csv_safe(find_csv_file(base, "tax_table.csv"), dtype=str)
    if tx is None or tt is None or tx.empty:
        return pd.DataFrame(columns=["TaxCode","Count"])
    used = tx["TaxCode"].astype(str).str.strip()
    used = used[used != ""]
    known = set(tt["TaxCode"].astype(str).str.strip().tolist()) if "TaxCode" in tt.columns else set()
    unk = used[~used.isin(known)]
    grp = unk.value_counts().rename_axis("TaxCode").reset_index(name="Count")
    return grp.sort_values(["Count","TaxCode"], ascending=[False, True])

def build_vat_summary(base: Path) -> pd.DataFrame:
    tx = read_csv_safe(find_csv_file(base, "transactions.csv"), dtype=str)
    if tx is None or tx.empty:
        return pd.DataFrame(columns=["TaxType","TaxCode","TaxPercentage","TaxAmount","NetBaseImplied"])
    to_num(tx, ["DebitTaxAmount","CreditTaxAmount","TaxAmount","Debit","Credit"])
    tx["TA"] = tx.get("TaxAmount", 0.0)
    # Deriver netto mva-beløp (eller DebitTaxAmount - CreditTaxAmount hvis tilgjengelig)
    tx["VAT"] = (tx.get("DebitTaxAmount", 0.0) - tx.get("CreditTaxAmount", 0.0))
    tx.loc[tx["VAT"] == 0.0, "VAT"] = tx["TA"]
    # Estimer grunnlag = VAT / (rate/100) dersom rate>0
    def implied(perc, vat):
        try:
            p = float(perc)
            if p == 0.0:
                return 0.0
            return float(vat) / (p/100.0)
        except Exception:
            return 0.0
    tx["NetBaseImplied"] = [implied(p, v) for p, v in zip(tx.get("TaxPercentage",""), tx["VAT"])]
    grp = tx.groupby(["TaxType","TaxCode","TaxPercentage"])[["VAT","NetBaseImplied"]].sum().reset_index()
    grp.rename(columns={"VAT":"TaxAmount"}, inplace=True)
    return grp.sort_values(["TaxType","TaxCode","TaxPercentage"])
