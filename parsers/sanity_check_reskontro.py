# -*- coding: utf-8 -*-
"""
sanity_check_reskontro.py

Kjør slik:
    python sanity_check_reskontro.py "C:/sti/til/outputmappe"

Skriptet leser ap_subledger.xlsx og trial_balance.xlsx fra mappen
og skriver en liten diagnose-rapport til konsollen.
"""
import sys
from pathlib import Path
import pandas as pd

def main(outdir: str):
    p = Path(outdir)
    ap = p / "ap_subledger.xlsx"
    tb = p / "trial_balance.xlsx"
    if not ap.exists() or not tb.exists():
        print("Fant ikke ap_subledger.xlsx eller trial_balance.xlsx i", p)
        sys.exit(1)

    xls_ap = pd.ExcelFile(ap)
    sheets = xls_ap.sheet_names
    ap_bal = pd.read_excel(xls_ap, 'AP_Balances')
    ap_tx = pd.read_excel(xls_ap, 'AP_Transactions')
    print("AP-sheets:", sheets)
    print("Antall kontoer i AP_Transactions:", ap_tx['AccountID'].astype(str).nunique())
    print("Første 20 kontoer:", sorted(ap_tx['AccountID'].astype(str).unique())[:20])
    print("Sum AP_Balances: IB={:,.2f}, PR={:,.2f}, UB={:,.2f}".format(
        ap_bal.get('IB_Amount', pd.Series(dtype=float)).sum(),
        ap_bal.get('PR_Amount', pd.Series(dtype=float)).sum(),
        ap_bal.get('UB_Amount', pd.Series(dtype=float)).sum()
    ))

    xls_tb = pd.ExcelFile(tb)
    tb_df = pd.read_excel(xls_tb, 'TrialBalance')
    have_accounts_cols = set(['IB_OpenNet','PR_Accounts','UB_CloseNet']).issubset(tb_df.columns)
    print("TrialBalance har Accounts-kolonner (IB_OpenNet/PR_Accounts/UB_CloseNet)?", have_accounts_cols)
    if have_accounts_cols:
        tb24 = tb_df[tb_df['AccountID'].astype(str).isin(['2410','2460'])]
        print("2410+2460 UB_CloseNet:", tb24['UB_CloseNet'].sum())

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Bruk: python sanity_check_reskontro.py <outputmappe>")
        sys.exit(2)
    main(sys.argv[1])
