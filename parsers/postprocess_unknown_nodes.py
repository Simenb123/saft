# -*- coding: utf-8 -*-
"""
Bruk:
    python postprocess_unknown_nodes.py <outdir>

Forventer:
    <outdir>/raw_elements.csv  (laget av parseren)

Skriver:
    <outdir>/unknown_nodes.csv
    <outdir>/unknown_summary.csv

Endringer:
- Normaliserer XPath bedre: fjerner namespace-prefiks og indeks-segmenter som [1], [2] osv.
- Dette gjør matching mot kjente containere mer robust (lxml.getpath() bruker som regel [1]/[2]).
"""
import csv, re, sys
from pathlib import Path
from collections import defaultdict

_NS_SEG = re.compile(r'(?<=/)[^/]*?:')   # fjern "ns:"-prefiks etter "/"
_IDX_SEG = re.compile(r'\[\d+\]')        # fjern [1], [2], ...

def strip_ns_and_idx(xp: str) -> str:
    return _IDX_SEG.sub('', _NS_SEG.sub('', xp or ''))

KNOWN_CONTAINERS = [
    "/AuditFile/Header",
    "/AuditFile/MasterFiles/GeneralLedgerAccounts/Account",
    "/AuditFile/MasterFiles/GeneralLedgerAccounts/GeneralLedgerAccount",
    "/AuditFile/MasterFiles/Customers/Customer",
    "/AuditFile/MasterFiles/Suppliers/Supplier",
    "/AuditFile/MasterFiles/Products/Product",
    "/AuditFile/MasterFiles/TaxTable/TaxTableEntry",
    "/AuditFile/MasterFiles/Employees/Employee",
    "/AuditFile/MasterFiles/Employees/Staff",
    "/AuditFile/MasterFiles/AnalysisTypeTable/AnalysisType",
    "/AuditFile/MasterFiles/BankAccount",
    "/AuditFile/MasterFiles/Currency",
    "/AuditFile/GeneralLedgerEntries/Journal",
    "/AuditFile/GeneralLedgerEntries/Journal/Transaction",
    "/AuditFile/GeneralLedgerEntries/Journal/Transaction/Line",
    "/AuditFile/GeneralLedgerEntries/Journal/Transaction/TransactionLine",
    "/AuditFile/GeneralLedgerEntries/Journal/Transaction/JournalLine",
    "/AuditFile/SourceDocuments/SalesInvoices/Invoice",
    "/AuditFile/SourceDocuments/PurchaseInvoices/Invoice",
    "/AuditFile/SourceDocuments/WorkingDocuments/WorkDocument",
    "/AuditFile/SourceDocuments/WorkingDocuments/WorkingDocument",
    "/AuditFile/SourceDocuments/Receipts/Receipt",
    "/AuditFile/SourceDocuments/Payments/Payment",
    "/AuditFile/MovementOfGoods/Movement",
    "/AuditFile/FixedAssets/Asset",
    "/AuditFile/FixedAssets/FixedAsset",
]
ROOT_GROUPERS = [
    "/AuditFile/MasterFiles/",
    "/AuditFile/SourceDocuments/",
    "/AuditFile/GeneralLedgerEntries/",
    "/AuditFile/MovementOfGoods/",
    "/AuditFile/FixedAssets/",
]

def is_known(xp: str) -> bool:
    xps = strip_ns_and_idx(xp)
    for anchor in KNOWN_CONTAINERS:
        if anchor in xps:
            return True
    if "/Analysis" in xps:
        return True
    return False

def root_group(xp: str) -> str:
    xps = strip_ns_and_idx(xp)
    for base in ROOT_GROUPERS:
        if base in xps:
            tail = xps.split(base, 1)[1]
            seg = tail.split("/", 1)[0]
            return f"{base}{seg}"
    return "/(annet)"

def main():
    if len(sys.argv) != 2:
        print("Bruk: python postprocess_unknown_nodes.py <outdir>")
        sys.exit(2)

    outdir = Path(sys.argv[1])
    raw_path = outdir / "raw_elements.csv"
    if not raw_path.exists():
        print(f"Fant ikke {raw_path}. Sjekk at du bruker riktig <outdir>.")
        sys.exit(1)

    unk_path = outdir / "unknown_nodes.csv"
    sum_path = outdir / "unknown_summary.csv"

    unknown_rows = []
    counts = defaultdict(int)
    examples = {}

    with raw_path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            xp = (row.get("XPath") or "").strip()
            if not xp:
                continue
            if is_known(xp):
                continue
            unknown_rows.append({
                "XPath": strip_ns_and_idx(row.get("XPath","")),
                "Tag": row.get("Tag",""),
                "Text": row.get("Text",""),
                "Attributes": row.get("Attributes",""),
            })
            grp = root_group(xp)
            counts[grp] += 1
            if grp not in examples:
                examples[grp] = row

    with unk_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["XPath","Tag","Text","Attributes"])
        w.writeheader()
        w.writerows(unknown_rows)

    with sum_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["UnknownRoot","Count","ExampleXPath","ExampleTag","ExampleText"])
        w.writeheader()
        for grp, cnt in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            ex = examples.get(grp, {})
            w.writerow({
                "UnknownRoot": grp,
                "Count": cnt,
                "ExampleXPath": strip_ns_and_idx(ex.get("XPath","")),
                "ExampleTag": ex.get("Tag",""),
                "ExampleText": ex.get("Text",""),
            })

    print(f"Skrev:\n - {unk_path}\n - {sum_path}")

if __name__ == "__main__":
    main()
