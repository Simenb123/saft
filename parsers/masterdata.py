# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import pandas as pd
from .common import read_csv_safe, find_csv_file

def check_master_references(base: Path):
    out = {}
    tx = read_csv_safe(find_csv_file(base, "transactions.csv"), dtype=str)
    acc = read_csv_safe(find_csv_file(base, "accounts.csv"), dtype=str)
    cus = read_csv_safe(find_csv_file(base, "customers.csv"), dtype=str)
    sup = read_csv_safe(find_csv_file(base, "suppliers.csv"), dtype=str)
    if tx is None or tx.empty:
        return out
    # Accounts
    if acc is not None and not acc.empty and "AccountID" in acc.columns and "AccountID" in tx.columns:
        miss = tx.loc[~tx["AccountID"].isin(set(acc["AccountID"].astype(str))) & tx["AccountID"].astype(str).ne("")]
        out["Unknown_Accounts"] = miss[["RecordID","VoucherID","AccountID","Debit","Credit","Description"]]
    # Customers
    if cus is not None and "CustomerID" in tx.columns:
        mask = tx["CustomerID"].astype(str).ne("")
        miss = tx.loc[mask & ~tx["CustomerID"].isin(set(cus["CustomerID"].astype(str)))]
        out["Unknown_Customers"] = miss[["RecordID","VoucherID","CustomerID","Amount","Description"]]
    # Suppliers
    if sup is not None and "SupplierID" in tx.columns:
        mask = tx["SupplierID"].astype(str).ne("")
        miss = tx.loc[mask & ~tx["SupplierID"].isin(set(sup["SupplierID"].astype(str)))]
        out["Unknown_Suppliers"] = miss[["RecordID","VoucherID","SupplierID","Amount","Description"]]
    return out
