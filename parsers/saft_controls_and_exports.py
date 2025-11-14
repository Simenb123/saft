# -*- coding: utf-8 -*-
"""
Bruk:
    python saft_controls_and_exports.py <outdir> [--asof YYYY-MM-DD]

Lager:
- controls_summary.csv
- trial_balance.xlsx
- general_ledger.xlsx
- ar_ap_transactions.xlsx
- ar_ap_balances.xlsx
- ar_ap_aging.xlsx
"""
import argparse, sys
from pathlib import Path
import pandas as pd
import numpy as np

TOL = 0.01  # kr 0,01 toleranse

def read_csv_safe(path: Path, **kwargs):
    if not path.exists():
        return None
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        try:
            return pd.read_csv(path, sep=';', **kwargs)
        except Exception as e:
            raise e

def parse_dates(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce')
    return df

def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = (
                df[c].astype(str)
                     .str.replace('\u00A0','', regex=False)
                     .str.replace(' ','', regex=False)
                     .str.replace(',','.', regex=False)
            )
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("outdir", help="Mappe med parser-CSV")
    ap.add_argument("--asof", help="Skjæringsdato for aldersanalyse (YYYY-MM-DD). Default = seneste Posting/TransactionDate.", default=None)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    if not outdir.exists():
        print(f"Fant ikke mappe: {outdir}")
        sys.exit(2)

    tx = read_csv_safe(outdir/"transactions.csv", dtype=str)
    acc = read_csv_safe(outdir/"accounts.csv", dtype=str)
    vou = read_csv_safe(outdir/"vouchers.csv", dtype=str)
    cus = read_csv_safe(outdir/"customers.csv", dtype=str)
    sup = read_csv_safe(outdir/"suppliers.csv", dtype=str)
    gtot = read_csv_safe(outdir/"gl_totals.csv", dtype=str)
    unk = read_csv_safe(outdir/"unknown_nodes.csv", dtype=str)

    if tx is None or acc is None or vou is None:
        print("Mangler nødvendige filer (transactions.csv, accounts.csv, vouchers.csv). Avbryter.")
        sys.exit(2)

    tx = to_num(tx, ["Debit", "Credit", "TaxAmount"])
    tx = parse_dates(tx, ["TransactionDate", "PostingDate"])
    if "VoucherID" not in tx.columns: tx["VoucherID"] = ""
    if "AccountID" not in tx.columns: tx["AccountID"] = ""

    # Kontroller
    controls = []
    controls.append({"control":"Linjetelling (transactions)", "result": len(tx)})

    total_debit = tx["Debit"].sum()
    total_credit = tx["Credit"].sum()
    ok_global = abs(total_debit - total_credit) <= TOL
    controls.append({"control":"Global debet= kredit", "result": "OK" if ok_global else f"AVVIK {total_debit-total_credit:.2f}"})

    per_voucher = tx.groupby("VoucherID")[["Debit","Credit"]].sum().fillna(0.0)
    per_voucher["balanced"] = (per_voucher["Debit"] - per_voucher["Credit"]).abs() <= TOL
    unbalanced_vouchers = per_voucher[~per_voucher["balanced"]].index.tolist()
    controls.append({"control":"Bilag balansert", "result": "OK" if not unbalanced_vouchers else f"Ubalanserte bilag: {len(unbalanced_vouchers)}"})

    if gtot is not None:
        gtot2 = to_num(gtot.copy(), ["TotalDebit","TotalCredit"])
        if "JournalID" in tx.columns:
            tj = tx.groupby("JournalID")[["Debit","Credit"]].sum().reset_index()
            m = tj.merge(gtot2, on="JournalID", how="left", suffixes=("_calc",""))
            def comp(r):
                okd = abs(r["Debit"] - r.get("TotalDebit",0.0)) <= 1.0
                okc = abs(r["Credit"] - r.get("TotalCredit",0.0)) <= 1.0
                return okd and okc
            m["match"] = m.apply(comp, axis=1)
            n_bad = (~m["match"]).sum()
            controls.append({"control":"Journal totals = gl_totals.csv", "result": "OK" if n_bad==0 else f"Avvik i {int(n_bad)} journal(er)"})
        else:
            controls.append({"control":"Journal totals = gl_totals.csv", "result": "SKIPPET (mangler JournalID)"})
    else:
        controls.append({"control":"Journal totals = gl_totals.csv", "result": "SKIPPET (fil finnes ikke)"})

    tb = tx.groupby("AccountID")[["Debit","Credit"]].sum().reset_index()
    tb["Net"] = tb["Debit"] - tb["Credit"]
    if acc is not None and "AccountID" in acc.columns:
        join_cols = ["AccountID","AccountDescription"]
        if set(["ClosingDebit","ClosingCredit"]).issubset(acc.columns):
            join_cols += ["ClosingDebit","ClosingCredit"]
        tb = tb.merge(acc[join_cols].copy(), on="AccountID", how="left")
        if "ClosingDebit" in tb.columns and "ClosingCredit" in tb.columns:
            tb = to_num(tb, ["ClosingDebit","ClosingCredit"])
            tb["ClosingNet"] = tb["ClosingDebit"] - tb["ClosingCredit"]
            tb["MatchClosing?"] = (tb["Net"] - tb["ClosingNet"]).abs() <= 1.0
            controls.append({"control":"Saldobalanse vs accounts closing", "result": "OK" if tb["MatchClosing?"].all() else f"Avvik på {(~tb['MatchClosing?']).sum()} konto(er)"})
        else:
            controls.append({"control":"Saldobalanse vs accounts closing", "result": "SKIPPET (Closing-felter mangler i accounts.csv)"})
    else:
        controls.append({"control":"Saldobalanse vs accounts closing", "result": "SKIPPET (accounts.csv mangler eller mangler AccountID)"})

    n_missing_acc = (tx["AccountID"]=="").sum()
    controls.append({"control":"Transaksjoner uten AccountID", "result": 0 if n_missing_acc==0 else f"{n_missing_acc} linjer mangler AccountID"})
    if cus is not None and "CustomerID" in tx.columns:
        n_cust_orphan = tx["CustomerID"].notna().sum() - tx["CustomerID"].isin(set(cus["CustomerID"])).sum()
        controls.append({"control":"CustomerID uten master", "result": 0 if n_cust_orphan==0 else f"{n_cust_orphan} linjer har ukjent CustomerID"})
    if sup is not None and "SupplierID" in tx.columns:
        n_sup_orphan = tx["SupplierID"].notna().sum() - tx["SupplierID"].isin(set(sup["SupplierID"])).sum()
        controls.append({"control":"SupplierID uten master", "result": 0 if n_sup_orphan==0 else f"{n_sup_orphan} linjer har ukjent SupplierID"})

    anl = read_csv_safe(outdir/"analysis_lines.csv", dtype=str)
    if anl is not None and "RecordID" in anl.columns:
        miss = ~anl["RecordID"].isin(set(tx["RecordID"].dropna()))
        controls.append({"control":"Analysis uten record i transactions", "result": 0 if miss.sum()==0 else f"{miss.sum()} analysis-linjer mangler transaksjons-RecordID"})
    else:
        controls.append({"control":"Analysis uten record i transactions", "result": "SKIPPET (analysis_lines.csv mangler)"})

    if unk is not None:
        controls.append({"control":"Unknown nodes (rådump)", "result": len(unk)})
    else:
        controls.append({"control":"Unknown nodes (rådump)", "result": "SKIPPET (unknown_nodes.csv mangler)"})

    pd.DataFrame(controls).to_csv(outdir/"controls_summary.csv", index=False)

    # Excel-rapporter
    asof = args.asof
    if asof is None:
        cand = pd.concat([tx["PostingDate"], tx["TransactionDate"]], axis=0)
        asof_dt = pd.to_datetime(cand.max())
    else:
        asof_dt = pd.to_datetime(asof)

    tb_export = tb.copy().sort_values(["AccountID"])
    with pd.ExcelWriter(outdir/"trial_balance.xlsx") as xw:
        tb_export.to_excel(xw, index=False, sheet_name="TrialBalance")

    gl = tx.copy()
    gl["Date"] = gl["PostingDate"].fillna(gl["TransactionDate"])
    gl = gl.sort_values(["AccountID","Date","RecordID"])
    with pd.ExcelWriter(outdir/"general_ledger.xlsx") as xw:
        gl.to_excel(xw, index=False, sheet_name="GeneralLedger")

    ar = tx.loc[(tx.get("CustomerID","").astype(str)!="")].copy()
    ap = tx.loc[(tx.get("SupplierID","").astype(str)!="")].copy()
    with pd.ExcelWriter(outdir/"ar_ap_transactions.xlsx") as xw:
        if not ar.empty: ar.to_excel(xw, index=False, sheet_name="AR_Transactions")
        if not ap.empty: ap.to_excel(xw, index=False, sheet_name="AP_Transactions")

    def balance_by(df, key):
        g = df.copy()
        g["Date"] = g["PostingDate"].fillna(g["TransactionDate"])
        g = g[g["Date"]<=asof_dt]
        grp = g.groupby(key)[["Debit","Credit"]].sum().reset_index()
        grp["Balance"] = grp["Debit"] - grp["Credit"]
        return grp

    def optional_merge(df, dim, key, namecol):
        if dim is None or key not in dim.columns:
            return df
        rename = {namecol:"Name"} if namecol!="Name" else {}
        return df.merge(dim[[key, namecol]].rename(columns=rename), on=key, how="left")

    ar_bal = balance_by(ar, "CustomerID")
    ar_bal = optional_merge(ar_bal, cus, "CustomerID", "Name")
    ap_bal = balance_by(ap, "SupplierID")
    ap_bal = optional_merge(ap_bal, sup, "SupplierID", "Name")

    with pd.ExcelWriter(outdir/"ar_ap_balances.xlsx") as xw:
        if not ar_bal.empty: ar_bal.sort_values("CustomerID").to_excel(xw, index=False, sheet_name="AR_Balances")
        if not ap_bal.empty: ap_bal.sort_values("SupplierID").to_excel(xw, index=False, sheet_name="AP_Balances")

    def age_buckets(df, idcol, namecol=None):
        d = df.copy()
        d["Date"] = d["PostingDate"].fillna(d["TransactionDate"])
        d = d[d["Date"]<=asof_dt]
        d["Balance"] = d["Debit"] - d["Credit"]
        d["Days"] = (asof_dt - d["Date"]).dt.days
        def bucket(days):
            if pd.isna(days): return ">90"
            days = int(days)
            if days <= 30: return "0-30"
            if days <= 60: return "31-60"
            if days <= 90: return "61-90"
            return ">90"
        d["Bucket"] = d["Days"].apply(bucket)
        grp = d.groupby([idcol,"Bucket"])["Balance"].sum().unstack(fill_value=0).reset_index()
        if namecol and namecol in d.columns:
            names = d.groupby(idcol)[namecol].agg(lambda s: next((x for x in s if pd.notna(x) and x!=''), '')).reset_index()
            grp = names.merge(grp, on=idcol, how="right")
        grp["Total"] = grp[[c for c in ["0-30","31-60","61-90",">90"] if c in grp.columns]].sum(axis=1)
        return grp

    ar_aging = age_buckets(ar.merge(cus[["CustomerID","Name"]], on="CustomerID", how="left") if cus is not None else ar, "CustomerID", "Name" if cus is not None else None)
    ap_aging = age_buckets(ap.merge(sup[["SupplierID","Name"]], on="SupplierID", how="left") if sup is not None else ap, "SupplierID", "Name" if sup is not None else None)

    with pd.ExcelWriter(outdir/"ar_ap_aging.xlsx") as xw:
        if not ar_aging.empty: ar_aging.sort_values("CustomerID").to_excel(xw, index=False, sheet_name="AR_Aging")
        if not ap_aging.empty: ap_aging.sort_values("SupplierID").to_excel(xw, index=False, sheet_name="AP_Aging")

    print("Kontroller skrevet til:", outdir/"controls_summary.csv")
    print("Excel generert: trial_balance.xlsx, general_ledger.xlsx, ar_ap_transactions.xlsx, ar_ap_balances.xlsx, ar_ap_aging.xlsx")

if __name__ == "__main__":
    main()
