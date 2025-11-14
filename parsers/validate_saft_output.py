# -*- coding: utf-8 -*-
"""
Validator for SAF-T parse-resultater (CSV).
Bruk:
  python -m app.parsers.validate_saft_output --csv "path/to/job_folder/csv"

Den sjekker at forventede CSV-er finnes, summerer rader, og gjør noen grunnleggende kontroller:
- Amount == Debit - Credit pr. transaksjonslinje
- AccountID i transactions finnes i accounts
- CustomerID/SupplierID i transactions finnes i customers/suppliers
- Enkle sjekker av faktura-duplikater
Skriver en kort rapport til konsoll og 'validation_report.txt' i csv-mappa.
"""
from __future__ import annotations
from pathlib import Path
import argparse
import sys
import pandas as pd

EXPECTED = [
    "header.csv",
    "accounts.csv",
    "customers.csv",
    "suppliers.csv",
    "tax_table.csv",
    "transactions.csv",
    # opsjonelle:
    "analysis_lines.csv",
    "sales_invoices.csv",
    "purchase_invoices.csv",
]

def _load(csv_dir: Path, name: str, usecols=None):
    p = csv_dir / name
    if not p.exists():
        return None
    try:
        return pd.read_csv(p, dtype=str, low_memory=False, usecols=usecols)
    except Exception as e:
        print(f"[WARN] Kunne ikke lese {name}: {e}")
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Sti til csv-mappe (jobbmappens 'csv/').")
    args = ap.parse_args()
    csv_dir = Path(args.csv)
    if not csv_dir.exists():
        print(f"Fant ikke csv-mappen: {csv_dir}")
        sys.exit(2)

    report_lines = []
    def add(line=""):
        print(line)
        report_lines.append(line)

    add(f"SAF-T CSV-validering: {csv_dir}")
    add("=" * 72)

    # Tilstedeværelse
    present = []
    for name in EXPECTED:
        exists = (csv_dir / name).exists()
        present.append((name, exists))
    add("Filer (forventet):")
    for name, ok in present:
        add(f"  {'[OK ]' if ok else '[MISS]'} {name}")
    add()

    # Les grunnlagsfiler
    header = _load(csv_dir, "header.csv")
    accounts = _load(csv_dir, "accounts.csv")
    customers = _load(csv_dir, "customers.csv")
    suppliers = _load(csv_dir, "suppliers.csv")
    tax = _load(csv_dir, "tax_table.csv")
    tx_cols = ["RecordID","VoucherID","VoucherNo","JournalID","TransactionDate","PostingDate",
               "SystemID","BatchID","DocumentNumber","SourceDocumentID",
               "AccountID","CustomerID","SupplierID","Debit","Credit","Amount"]
    tx = _load(csv_dir, "transactions.csv", usecols=[c for c in tx_cols if (csv_dir/"transactions.csv").exists()])

    # Radtall
    add("Radtall:")
    for nm, df in [("header.csv", header), ("accounts.csv", accounts), ("customers.csv", customers),
                   ("suppliers.csv", suppliers), ("tax_table.csv", tax), ("transactions.csv", tx)]:
        if df is not None:
            add(f"  {nm:18s}  {len(df):>10,d} rader".replace(",", " "))
        else:
            add(f"  {nm:18s}  (mangler)")
    add()

    # header: forvent ~1 rad
    if header is not None and len(header) != 1:
        add(f"[WARN] header.csv har {len(header)} rader (forventet ~1).")

    # Kontroller mot accounts/customers/suppliers
    if tx is not None:
        # Normaliser
        for c in ("Debit","Credit","Amount"):
            if c in tx.columns:
                tx[c] = (tx[c].astype(str).str.replace(" ", "").str.replace("\u00A0","").str.replace(",", ".", regex=False))
                # behold string; jämför tall via float (for rask validering)
                try:
                    tx[c] = tx[c].astype(float)
                except Exception:
                    pass

        # Amount = Debit - Credit
        if {"Debit","Credit","Amount"}.issubset(tx.columns):
            diff = (tx["Debit"].fillna(0) - tx["Credit"].fillna(0)) - tx["Amount"].fillna(0)
            bad = diff.abs() > 1e-9
            n_bad = int(bad.sum())
            add(f"Kontroll: Amount == Debit - Credit: {'OK' if n_bad==0 else 'FEIL'} ({n_bad} avvik)")
            if n_bad:
                sample = tx.loc[bad, ["RecordID","VoucherID","AccountID","Debit","Credit","Amount"]].head(5)
                add("  Eksempelavvik:")
                add(sample.to_string(index=False))
        else:
            add("[INFO] Hopper over Amount-kontroll (mangler kolonner).")

        # Accounts
        if accounts is not None and "AccountID" in accounts.columns and "AccountID" in tx.columns:
            missing_acc = set(tx["AccountID"].dropna().unique()) - set(accounts["AccountID"].dropna().unique())
            add(f"Kontroll: AccountID fra transaksjoner i accounts: "
                f"{'OK' if len(missing_acc)==0 else f'MANGLER {len(missing_acc)}'}")
            if missing_acc:
                add(f"  Eksempel: {list(sorted(missing_acc))[:10]}")

        # Customers
        if customers is not None and "CustomerID" in customers.columns and "CustomerID" in tx.columns:
            cust_ids = set(customers["CustomerID"].dropna().unique())
            used_cust = set(tx["CustomerID"].dropna().unique())
            missing_cust = used_cust - cust_ids
            add(f"Kontroll: CustomerID fra transaksjoner i customers: "
                f"{'OK' if len(missing_cust)==0 else f'MANGLER {len(missing_cust)}'}")
            if missing_cust:
                add(f"  Eksempel: {list(sorted(missing_cust))[:10]}")

        # Suppliers
        if suppliers is not None and "SupplierID" in suppliers.columns and "SupplierID" in tx.columns:
            supp_ids = set(suppliers["SupplierID"].dropna().unique())
            used_supp = set(tx["SupplierID"].dropna().unique())
            missing_supp = used_supp - supp_ids
            add(f"Kontroll: SupplierID fra transaksjoner i suppliers: "
                f"{'OK' if len(missing_supp)==0 else f'MANGLER {len(missing_supp)}'}")
            if missing_supp:
                add(f"  Eksempel: {list(sorted(missing_supp))[:10]}")

    # Faktura-duplikater
    for inv_name, id_col in (("sales_invoices.csv","InvoiceNo"), ("purchase_invoices.csv","InvoiceNo")):
        inv = _load(csv_dir, inv_name)
        if inv is None:
            continue
        if id_col in inv.columns:
            dup = inv[inv.duplicated([id_col], keep=False)]
            add(f"Kontroll: Duplikate {inv_name}/{id_col}: {'OK' if dup.empty else f'{len(dup)} duplikater'}")
            if not dup.empty:
                add(dup.sort_values(id_col).head(10).to_string(index=False))

    # Skriv rapport til fil
    out = csv_dir / "validation_report.txt"
    try:
        with open(out, "w", encoding="utf-8") as f:
            for line in report_lines:
                f.write(line + "\n")
        add()
        add(f"[OK] Skrev rapport: {out}")
    except Exception as e:
        add(f"[WARN] Klarte ikke å skrive rapport: {e}")

if __name__ == "__main__":
    main()
