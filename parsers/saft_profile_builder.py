# -*- coding: utf-8 -*-
"""
saft_profile_builder.py
-----------------------
Bygger en *beskrivende* profil av en SAF‑T‑kjøring basert på CSV‑ene produsert av parseren.
Ingen heuristikk, ingen endring av data – kun observasjoner.
Produserer:
- SAFT_PROFILE.json (maskinlesbar)
- SAFT_SUMMARY.md  (for mennesker)
"""

from __future__ import annotations
import csv, json, hashlib
from pathlib import Path
from typing import Dict, Any

def _read_one_row(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            return {k: (v or "").strip() for k, v in row.items()}
    return {}

def _count_rows(path: Path) -> int:
    if not path.exists(): return 0
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        try:
            next(r)  # header
        except StopIteration:
            return 0
        return sum(1 for _ in r)

def _count_nonempty(path: Path, field: str) -> int:
    if not path.exists(): return 0
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        n = 0
        for row in r:
            if (row.get(field) or "").strip():
                n += 1
        return n

def _distinct_nonempty(path: Path, field: str) -> int:
    if not path.exists(): return 0
    s = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            v = (row.get(field) or "").strip()
            if v:
                s.add(v)
    return len(s)

def _sha256_file(path: Path, limit_mb: int = 4) -> str:
    # Hasher kun først limit_mb for å unngå treghet på gigantfiler; nok til fingeravtrykk
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            left = limit_mb * 1024 * 1024
            while left > 0:
                chunk = f.read(min(1024 * 1024, left))
                if not chunk: break
                h.update(chunk)
                left -= len(chunk)
    except Exception:
        return ""
    return h.hexdigest()

def build_profile(csv_dir: Path, *, input_file: Path | None = None) -> Path:
    """
    Leser CSV-utdata og bygger en profil:
    - SAFT_PROFILE.json i csv_dir
    - SAFT_SUMMARY.md i csv_dir
    Returnerer stien til SAFT_PROFILE.json
    """
    csv_dir = Path(csv_dir)
    header = _read_one_row(csv_dir / "header.csv")
    audit_ver = (header.get("AuditFileVersion") or "").strip()
    product_version = (header.get("ProductVersion") or "").strip()
    certificate = (header.get("SoftwareCertificateNumber") or "").strip()

    profile: Dict[str, Any] = {
        "file_info": {
            "file_name": input_file.name if input_file else "",
            "sha256_prefix": _sha256_file(input_file) if input_file else "",
            "audit_file_version": audit_ver,
            "product_version": product_version,
            "software_certificate_number": certificate,
        },
        "presence": {},
        "counts": {},
        "journal_type_distribution": {},
        "classification": {},
        "compliance_findings": [],
        "unknown_nodes_top": []
    }

    # Presence + counts
    tx = csv_dir / "transactions.csv"
    cust = csv_dir / "customers.csv"
    supp = csv_dir / "suppliers.csv"
    accs = csv_dir / "accounts.csv"
    tax  = csv_dir / "tax_table.csv"
    gl_tot = csv_dir / "gl_totals.csv"
    journals = csv_dir / "journals.csv"  # skrives av nyere parser; kan mangle
    unk = csv_dir / "unknown_summary.csv"
    parse_stats = csv_dir / "parse_stats.json"

    def has(p: Path) -> bool: return p.exists() and p.stat().st_size > 0

    presence = {
        "has_general_ledger_entries": has(tx),
        "has_source_documents": False,  # norsk SAF‑T bruker normalt ikke disse
        "has_customers": has(cust),
        "has_suppliers": has(supp),
        "has_tax_table": has(tax),
        "has_gl_accounts": has(accs),
    }

    # AR/AP signaler (normativt): CustomerID / SupplierID på LINE‑nivå
    n_ar_lines = _count_nonempty(tx, "CustomerID") if presence["has_general_ledger_entries"] else 0
    n_ap_lines = _count_nonempty(tx, "SupplierID") if presence["has_general_ledger_entries"] else 0
    presence["has_ar_lines"] = n_ar_lines > 0
    presence["has_ap_lines"] = n_ap_lines > 0

    # Counts
    counts = {
        "transactions": _count_rows(tx),
        "customers": _count_rows(cust),
        "suppliers": _count_rows(supp),
        "accounts": _count_rows(accs),
        "tax_table": _count_rows(tax),
        "gl_totals": _count_rows(gl_tot),
        "journals": _count_rows(journals),
        "ar_line_count": n_ar_lines,
        "ap_line_count": n_ap_lines,
        "distinct_customers_in_lines": _distinct_nonempty(tx, "CustomerID"),
        "distinct_suppliers_in_lines": _distinct_nonempty(tx, "SupplierID"),
    }

    # Journal type distribution hvis journals.csv finnes
    jdist: Dict[str, int] = {}
    if journals.exists():
        with journals.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                t = (row.get("Type") or "").strip() or "UNSPECIFIED"
                jdist[t] = jdist.get(t, 0) + 1

    profile["presence"] = presence
    profile["counts"] = counts
    profile["journal_type_distribution"] = jdist or {"not_available": True}

    # Klassifisering (beskrivende, ikke tolkende)
    if presence["has_general_ledger_entries"]:
        if presence["has_ar_lines"] and presence["has_ap_lines"]:
            pid = "GL_WITH_AR_AP"
            evidence = ["CustomerID present on lines", "SupplierID present on lines"]
        elif presence["has_ar_lines"]:
            pid = "GL_WITH_AR"
            evidence = ["CustomerID present on lines"]
        elif presence["has_ap_lines"]:
            pid = "GL_WITH_AP"
            evidence = ["SupplierID present on lines"]
        else:
            pid = "GL_ONLY"
            evidence = ["No CustomerID/SupplierID on lines"]
    else:
        pid = "EMPTY"
        evidence = ["No GeneralLedgerEntries (transactions.csv missing/empty)"]
    profile["classification"] = {
        "profile_id": pid,
        "evidence": evidence
    }

    # Enkle versjonskontroller (forsiktige – ingen antakelser)
    findings = []

    # 1.30: krever StandardTaxCode på TaxTable og GroupingCategory/GroupingCode på Account
    if audit_ver.startswith("1.3"):
        # a) TaxTable.StandardTaxCode
        if presence["has_tax_table"]:
            missing = 0
            with tax.open("r", encoding="utf-8", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    if not (row.get("StandardTaxCode") or "").strip():
                        missing += 1
            if missing > 0:
                findings.append({
                    "rule_id": "STD_130_TAXTABLE_STANDARDCODE",
                    "severity": "ERROR",
                    "message": f"TaxTable.StandardTaxCode mangler på {missing} rader (krav i 1.30)."
                })
        # b) Account.GroupingCategory/GroupingCode
        if presence["has_gl_accounts"]:
            miss_cat = miss_code = 0
            with accs.open("r", encoding="utf-8", newline="") as f:
                r = csv.DictReader(f)
                for row in r:
                    if not (row.get("GroupingCategory") or "").strip():
                        miss_cat += 1
                    if not (row.get("GroupingCode") or "").strip():
                        miss_code += 1
            if miss_cat > 0 or miss_code > 0:
                findings.append({
                    "rule_id": "STD_130_ACCOUNT_GROUPING",
                    "severity": "ERROR",
                    "message": f"Account mangler GroupingCategory ({miss_cat})/GroupingCode ({miss_code}) (krav i 1.30)."
                })

    # AR/AP signaler
    if counts["ar_line_count"] == 0 and counts["ap_line_count"] == 0 and presence["has_general_ledger_entries"]:
        findings.append({
            "rule_id": "NO_AR_AP_EXPOSED",
            "severity": "INFO",
            "message": "Ingen reskontrolinjer (CustomerID/SupplierID) er eksponert i filen."
        })

    # Unknown summary
    unk_top = []
    if unk.exists():
        with unk.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            for i, row in enumerate(r):
                if i >= 10: break
                unk_top.append({"tag": (row.get("Tag") or "").strip(),
                                "count": int((row.get("Count") or "0").strip() or 0)})
    profile["unknown_nodes_top"] = unk_top

    # Skriv profil + MD
    prof_path = csv_dir / "SAFT_PROFILE.json"
    prof_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")

    md_lines = []
    md_lines.append("# SAF‑T profil (automatisk)")
    md_lines.append("")
    md_lines.append(f"- AuditFileVersion: **{audit_ver or '(ukjent)'}**")
    md_lines.append(f"- Produktversjon: **{product_version or '(ukjent)'}**")
    md_lines.append(f"- Sertifikatnr.: **{certificate or '(ukjent)'}**")
    md_lines.append("")
    md_lines.append("## Observasjoner")
    md_lines.append(f"- Transactions: **{counts['transactions']:,}**")
    md_lines.append(f"- Customers (master): **{counts['customers']:,}**  |  Suppliers (master): **{counts['suppliers']:,}**")
    md_lines.append(f"- AR‑linjer (CustomerID satt): **{counts['ar_line_count']:,}**  |  AP‑linjer (SupplierID satt): **{counts['ap_line_count']:,}**")
    if jdist:
        md_lines.append(f"- Journal‑typer: `{jdist}`")
    else:
        md_lines.append("- Journal‑typer: *(ikke tilgjengelig)*")
    md_lines.append("")
    md_lines.append("## Klassifisering (beskrivende)")
    md_lines.append(f"- **{pid}**  –  begrunnelse: {', '.join(evidence)}")
    md_lines.append("")
    if findings:
        md_lines.append("## Funn")
        for f in findings:
            md_lines.append(f"- **{f['severity']}** `{f['rule_id']}` – {f['message']}")
        md_lines.append("")
    if unk_top:
        md_lines.append("## Ukjente noder (topp 10)")
        for u in unk_top:
            md_lines.append(f"- `{u['tag']}`: {u['count']}")
        md_lines.append("")

    (csv_dir / "SAFT_SUMMARY.md").write_text("\n".join(md_lines), encoding="utf-8")
    return prof_path