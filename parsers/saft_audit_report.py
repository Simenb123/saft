# -*- coding: utf-8 -*-
"""
Audit-rapport for SAF-T CSV-output (etter parsing).
Bruk:
  python -m app.parsers.saft_audit_report --csv "path/to/job_folder/csv"

Skriver:
  - audit_report.md   (lesbar oppsummering)
  - audit_summary.json (maskinlesbar)
Viser også kort oppsummering i konsollen.

Hva rapporteres:
  - Tilstedeværelse av forventede CSV-er og radtall
  - Sum Debet/Kredit/Beløp (transaksjoner) + avvik
  - Andel manglende felt i transactions (AccountID, CustomerID, SupplierID, DocumentNumber, osv.)
  - Konto-hierarki-nivåer ut fra ParentID-lenker
  - MVA-koder: oversikt per TaxType/TaxCode
  - Enkle kvalitetskontroller (AccountID brukt i transaksjoner finnes i accounts, osv.)
"""
from __future__ import annotations
from pathlib import Path
import argparse, json, math
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

def _read_csv(path: Path, usecols=None, chunksize=None):
    if not path.exists():
        return None
    try:
        if chunksize:
            return pd.read_csv(path, dtype=str, low_memory=False, usecols=usecols, chunksize=chunksize)
        return pd.read_csv(path, dtype=str, low_memory=False, usecols=usecols)
    except Exception:
        return None

def _norm_num(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(" ", "", regex=False)
         .str.replace("\u00A0", "", regex=False)
         .str.replace(",", ".", regex=False)
    )

def _account_levels(df_accounts: pd.DataFrame) -> pd.DataFrame:
    # beregn nivå ut fra ParentID-lenker (root=1)
    parent = dict(zip(df_accounts["AccountID"], df_accounts["ParentID"].fillna("")))
    levels = {}
    def lvl(aid, seen=None):
        if aid in levels: return levels[aid]
        if seen is None: seen = set()
        if not aid or aid in seen: return 1
        seen.add(aid)
        p = parent.get(aid, "")
        if not p or p == aid:  # root eller syklus
            levels[aid] = 1
            return 1
        L = 1 + lvl(p, seen)
        levels[aid] = L
        return L
    for aid in df_accounts["AccountID"].dropna().unique():
        lvl(aid)
    return pd.DataFrame({"AccountID": list(levels.keys()), "Level": list(levels.values())})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Sti til csv-mappe")
    args = ap.parse_args()
    csv_dir = Path(args.csv)
    if not csv_dir.exists():
        print(f"[ERROR] Fant ikke mappe: {csv_dir}")
        return

    # tilstedeværelse
    present = {nm: (csv_dir / nm).exists() for nm in EXPECTED}

    header = _read_csv(csv_dir/"header.csv")
    accounts = _read_csv(csv_dir/"accounts.csv")
    customers = _read_csv(csv_dir/"customers.csv")
    suppliers = _read_csv(csv_dir/"suppliers.csv")
    tax = _read_csv(csv_dir/"tax_table.csv")

    # transactions: strøm gjennom i chunks for minne
    tx_path = csv_dir/"transactions.csv"
    tx_cols = ["RecordID","VoucherID","VoucherNo","JournalID","TransactionDate","PostingDate",
               "SystemID","BatchID","DocumentNumber","SourceDocumentID",
               "AccountID","CustomerID","SupplierID","Debit","Credit","Amount"]
    tx_chunks = _read_csv(tx_path, usecols=[c for c in tx_cols if tx_path.exists()], chunksize=250_000)

    # aggregering for tx
    tx_rows = 0
    sums = {"Debit": 0.0, "Credit": 0.0, "Amount": 0.0}
    missing_counts = {c: 0 for c in tx_cols}
    sample_missing = []

    if tx_chunks is not None:
        for ch in tx_chunks:
            tx_rows += len(ch)
            for c in ("Debit","Credit","Amount"):
                if c in ch.columns:
                    v = _norm_num(ch[c])
                    try:
                        sums[c] += pd.to_numeric(v, errors="coerce").fillna(0.0).sum()
                    except Exception:
                        pass
            # missing
            for c in tx_cols:
                if c in ch.columns:
                    missing_counts[c] += ch[c].isna().sum() + (ch[c].astype(str).str.len() == 0).sum()
            # sample første 5 med manglende AccountID/CustomerID/SupplierID
            if len(sample_missing) < 5 and "AccountID" in ch.columns:
                sm = ch[ch["AccountID"].isna() | (ch["AccountID"].astype(str) == "")]
                if not sm.empty:
                    sample_missing.extend(sm.head(5).to_dict("records"))

    # konto-nivåer
    level_stats = None
    if accounts is not None and "AccountID" in accounts.columns and "ParentID" in accounts.columns:
        levels_df = _account_levels(accounts.fillna(""))
        level_stats = levels_df["Level"].value_counts().sort_index().to_dict()

    # MVA-koder
    tax_summary = None
    if tax is not None:
        cols = [c for c in ("TaxType","TaxCode","Description","TaxPercentage") if c in tax.columns]
        tax_summary = tax[cols].fillna("").drop_duplicates().to_dict("records")

    # kvalitetskontroller
    checks = {}
    if accounts is not None and tx_path.exists():
        # accounts mapping
        acc_set = set(accounts["AccountID"].dropna().astype(str))
        missing_acc = 0
        used_acc = 0
        for ch in _read_csv(tx_path, usecols=["AccountID"], chunksize=250_000):
            s = ch["AccountID"].dropna().astype(str)
            used_acc += s.size
            missing_acc += (~s.isin(list(acc_set))).sum()
        checks["AccountID_present_in_accounts"] = {"total_used": int(used_acc), "missing": int(missing_acc)}
    if customers is not None and tx_path.exists():
        cust_set = set(customers["CustomerID"].dropna().astype(str))
        total = miss = 0
        for ch in _read_csv(tx_path, usecols=["CustomerID"], chunksize=250_000):
            s = ch["CustomerID"].dropna().astype(str)
            total += s.size
            miss += (~s.isin(list(cust_set))).sum()
        checks["CustomerID_present_in_customers"] = {"total_used": int(total), "missing": int(miss)}
    if suppliers is not None and tx_path.exists():
        sup_set = set(suppliers["SupplierID"].dropna().astype(str))
        total = miss = 0
        for ch in _read_csv(tx_path, usecols=["SupplierID"], chunksize=250_000):
            s = ch["SupplierID"].dropna().astype(str)
            total += s.size
            miss += (~s.isin(list(sup_set))).sum()
        checks["SupplierID_present_in_suppliers"] = {"total_used": int(total), "missing": int(miss)}

    # bygg rapport
    summary = {
        "present_files": present,
        "rows": {
            "header": int(len(header)) if header is not None else 0,
            "accounts": int(len(accounts)) if accounts is not None else 0,
            "customers": int(len(customers)) if customers is not None else 0,
            "suppliers": int(len(suppliers)) if suppliers is not None else 0,
            "tax_table": int(len(tax)) if tax is not None else 0,
            "transactions": int(tx_rows),
        },
        "transaction_sums": {k: float(round(v, 2)) for k,v in sums.items()},
        "transaction_missing_fraction": {k: (float(missing_counts[k]) / tx_rows if tx_rows else None) for k in tx_cols},
        "account_level_counts": level_stats,
        "tax_codes": tax_summary,
        "checks": checks,
        "sample_missing_records": sample_missing[:5],
    }

    # skriv filer
    with open(csv_dir/"audit_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    with open(csv_dir/"audit_report.md", "w", encoding="utf-8") as f:
        f.write(f"# SAF-T Audit-rapport\n\nKatalog: `{csv_dir}`\n\n")
        f.write("## Filer og radtall\n")
        for nm in EXPECTED:
            status = "OK" if present.get(nm) else "Mangler"
            f.write(f"- {nm:18s}: {status}\n")
        f.write("\n")
        f.write("## Radtall\n")
        for k, v in summary["rows"].items():
            f.write(f"- {k:12s}: {v:,}\n".replace(",", " "))
        f.write("\n## Transaksjonssummer\n")
        f.write(f"- Debit : {summary['transaction_sums']['Debit']}\n")
        f.write(f"- Credit: {summary['transaction_sums']['Credit']}\n")
        f.write(f"- Amount: {summary['transaction_sums']['Amount']}\n")
        f.write(f"- Diff (Debit-Credit-Amount): {round(summary['transaction_sums']['Debit'] - summary['transaction_sums']['Credit'] - summary['transaction_sums']['Amount'], 2)}\n")
        f.write("\n## Manglende felt (andel av rader)\n")
        for k, frac in summary["transaction_missing_fraction"].items():
            if frac is None: continue
            f.write(f"- {k:16s}: {frac:.2%}\n")
        if summary["account_level_counts"]:
            f.write("\n## Konto-hierarki (antall kontoer per nivå)\n")
            for lvl, cnt in sorted(summary["account_level_counts"].items()):
                f.write(f"- Nivå {lvl}: {cnt}\n")
        if summary["tax_codes"]:
            f.write("\n## MVA-koder (unik kombinasjon)\n")
            for row in summary["tax_codes"][:50]:
                f.write(f"- {row}\n")
        if summary["checks"]:
            f.write("\n## Kryssjekker\n")
            for name, info in summary["checks"].items():
                f.write(f"- {name}: {info}\n")
        if summary["sample_missing_records"]:
            f.write("\n## Eksempel (rader med manglende AccountID)\n")
            for r in summary["sample_missing_records"]:
                f.write(f"- {r}\n")

    print("[OK] Skrev audit_summary.json og audit_report.md i", csv_dir)

if __name__ == "__main__":
    main()
