# -*- coding: utf-8 -*-
"""
saft_mapping_report.py
----------------------
Bygger *alltid* CSV-rapporter (rådata) for mapping:
  - Konto -> Næringsspesifikasjon (GroupingCategory/GroupingCode)
  - MVA-koder -> StandardTaxCode

I tillegg:
  - mapping_findings.csv           (kun avvik)
  - mapping_findings_summary.csv   (tellelinjer)
Valgfritt: excel/mapping_overview.xlsx for analyse (hvis make_excel=True og xlsxwriter finnes).

Fallback:
  Hvis accounts.csv mangler 1.3-feltene eller de er tomme, forsøker vi å lese mapping_probe_accounts.csv.
  Finnes ikke den, forsøker vi å lese SAFT_PROFILE.json for å finne original input-fil og kjøre proben,
  med output i csv/-mappen.
"""
from __future__ import annotations

from pathlib import Path
import csv
import json
from typing import List, Dict, Tuple, Optional

try:
    import xlsxwriter  # type: ignore
    _HAS_XLSX = True
except Exception:
    _HAS_XLSX = False


def _read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append({k: (v or "").strip() for k, v in row.items()})
    return rows


def _safe_float(s: str) -> Optional[float]:
    try:
        return float((s or "").strip())
    except Exception:
        return None


# ------------------- mapping-kilder -------------------

def _accounts_from_accounts_csv(csv_dir: Path) -> Tuple[List[Dict[str,str]], str]:
    acc = _read_csv(csv_dir / "accounts.csv")
    if not acc:
        return [], "missing_accounts_csv"
    cols = {k.lower() for k in acc[0].keys()}
    has_cols = ("groupingcategory" in cols) and ("groupingcode" in cols)
    if not has_cols:
        return [], "accounts_no_grouping_columns"
    any_val = any((r.get("GroupingCategory") or r.get("GroupingCode")) for r in acc)
    if not any_val:
        return [], "accounts_grouping_empty"
    # return bare feltene vi trenger
    out = []
    for r in acc:
        out.append({
            "AccountID": r.get("AccountID",""),
            "AccountDescription": r.get("AccountDescription",""),
            "GroupingCategory": r.get("GroupingCategory",""),
            "GroupingCode": r.get("GroupingCode",""),
        })
    return out, "accounts_csv"


def _accounts_from_probe_csv(csv_dir: Path) -> Tuple[List[Dict[str,str]], bool]:
    p = csv_dir / "mapping_probe_accounts.csv"
    if not p.exists():
        return [], False
    rows = _read_csv(p)
    out = []
    for r in rows:
        out.append({
            "AccountID": r.get("AccountID",""),
            "AccountDescription": r.get("AccountDescription",""),
            "GroupingCategory": r.get("GroupingCategory",""),
            "GroupingCode": r.get("GroupingCode",""),
        })
    return out, True


def _maybe_run_probe_via_profile(csv_dir: Path) -> bool:
    prof = csv_dir / "SAFT_PROFILE.json"
    if not prof.exists():
        return False
    try:
        d = json.loads(prof.read_text(encoding="utf-8"))
        # forsøk flere vanlige plasseringer for input-path
        inp = (d.get("file_info", {}) or {}).get("input_file") \
              or (d.get("file_info", {}) or {}).get("source_file") \
              or d.get("input_file")
        if not inp:
            return False
        in_path = Path(inp)
        # import probe
        try:
            from . import saft_mapping_probe as probe  # type: ignore
        except Exception:
            import importlib
            probe = importlib.import_module("app.parsers.saft_mapping_probe")  # type: ignore
        # kjør proben slik at mapping_probe_accounts.csv legges i csv/
        probe._probe(in_path, csv_dir)  # type: ignore
        return True
    except Exception:
        return False


def _choose_account_mapping_source(csv_dir: Path) -> Tuple[List[Dict[str,str]], str]:
    # 1) forsøk fra accounts.csv
    rows, src = _accounts_from_accounts_csv(csv_dir)
    if rows:
        return rows, src
    # 2) forsøk fra mapping_probe_accounts.csv
    rows, ok = _accounts_from_probe_csv(csv_dir)
    if ok and rows:
        return rows, "probe_csv"
    # 3) hvis mulig, kjør probe nå (via profil) og les på nytt
    if _maybe_run_probe_via_profile(csv_dir):
        rows, ok = _accounts_from_probe_csv(csv_dir)
        if ok and rows:
            return rows, "probe_csv"
    # 4) ellers tomt
    return [], src


# ------------------- CSV/Excel bygging -------------------

def _write_accounts_csv(accounts: List[Dict[str, str]], out_csv: Path) -> Tuple[int, int]:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["AccountID","AccountDescription","GroupingCategory","GroupingCode"])
        missing = 0
        for r in accounts:
            cat = r.get("GroupingCategory","")
            code = r.get("GroupingCode","")
            if not cat or not code:
                missing += 1
            w.writerow([r.get("AccountID",""), r.get("AccountDescription",""), cat, code])
    return len(accounts), missing


def _write_tax_csv(tax_rows: List[Dict[str, str]], out_csv: Path) -> Tuple[int, int]:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["TaxCode","StandardTaxCode","TaxType","TaxPercentage","TaxCountryRegion","Description"])
        missing = 0
        for r in tax_rows:
            std = r.get("StandardTaxCode","")
            if not std:
                missing += 1
            w.writerow([
                r.get("TaxCode",""), std, r.get("TaxType",""), r.get("TaxPercentage",""),
                r.get("TaxCountryRegion",""), r.get("Description",""),
            ])
    return len(tax_rows), missing


def _collect_findings(accounts: List[Dict[str, str]], tax_rows: List[Dict[str, str]]):
    findings: List[Dict[str, str]] = []
    acc_total = len(accounts); acc_missing = 0
    tax_total = len(tax_rows); tax_missing = 0

    for r in accounts:
        cat = (r.get("GroupingCategory") or "").strip()
        code = (r.get("GroupingCode") or "").strip()
        issue = None
        if not cat and not code:
            issue = "Missing GroupingCategory and GroupingCode"
        elif not cat:
            issue = "Missing GroupingCategory"
        elif not code:
            issue = "Missing GroupingCode"
        if issue:
            acc_missing += 1
            findings.append({
                "Type": "ACCOUNT",
                "AccountID": r.get("AccountID",""),
                "AccountDescription": r.get("AccountDescription",""),
                "GroupingCategory": cat,
                "GroupingCode": code,
                "TaxCode": "",
                "StandardTaxCode": "",
                "TaxType": "",
                "TaxPercentage": "",
                "TaxCountryRegion": "",
                "Description": "",
                "Issue": issue,
            })

    for r in tax_rows:
        std = (r.get("StandardTaxCode") or "").strip()
        if not std:
            tax_missing += 1
            findings.append({
                "Type": "TAX",
                "AccountID": "",
                "AccountDescription": "",
                "GroupingCategory": "",
                "GroupingCode": "",
                "TaxCode": r.get("TaxCode",""),
                "StandardTaxCode": "",
                "TaxType": r.get("TaxType",""),
                "TaxPercentage": r.get("TaxPercentage",""),
                "TaxCountryRegion": r.get("TaxCountryRegion",""),
                "Description": r.get("Description",""),
                "Issue": "Missing StandardTaxCode",
            })

    stats = {
        "accounts_total": acc_total, "accounts_missing": acc_missing,
        "tax_total": tax_total, "tax_missing": tax_missing
    }
    return findings, stats


def _write_findings_csv(findings: List[Dict[str,str]], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "Type",
        "AccountID","AccountDescription","GroupingCategory","GroupingCode",
        "TaxCode","StandardTaxCode","TaxType","TaxPercentage","TaxCountryRegion","Description",
        "Issue"
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        w.writeheader()
        for row in findings:
            w.writerow(row)


def _write_findings_summary_csv(stats: Dict[str,int], out_csv: Path) -> None:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["Section","Total","Missing","MissingPercent"])
        for name, total_key, miss_key in [
            ("Accounts", "accounts_total", "accounts_missing"),
            ("Tax", "tax_total", "tax_missing"),
        ]:
            total = int(stats.get(total_key, 0))
            missing = int(stats.get(miss_key, 0))
            pct = (missing/total*100.0) if total else 0.0
            w.writerow([name, total, missing, f"{pct:.2f}"])


# ------------------- Excel (analyse) -------------------

def _write_account_sheet(wb, accounts: List[Dict[str, str]]):
    ws = wb.add_worksheet("Konto–Næringsspesifikasjon")
    fmt_head = wb.add_format({"bold": True})
    fmt_ok   = wb.add_format({"font_color": "green"})
    fmt_err  = wb.add_format({"font_color": "white", "bg_color": "#D9534F"})
    fmt_num  = wb.add_format({"num_format": "0"})
    headers = [
        ("AccountID", "Konto"),
        ("AccountDescription", "Kontonavn"),
        ("GroupingCategory", "Næringsspesifikasjon (kategori)"),
        ("GroupingCode", "Næringsspesifikasjon (kode 4-siffer)"),
        ("_VAL", "Status (1.3‑krav)"),
    ]
    for c, (_, title) in enumerate(headers):
        ws.write(0, c, title, fmt_head)

    missing = 0
    for r, row in enumerate(accounts, start=1):
        acc = row.get("AccountID",""); name = row.get("AccountDescription","")
        cat = row.get("GroupingCategory",""); code = row.get("GroupingCode","")
        ws.write(r, 0, acc); ws.write(r, 1, name); ws.write(r, 2, cat)
        if code.isdigit(): ws.write_number(r, 3, int(code), fmt_num)
        else: ws.write(r, 3, code)
        ok = bool(cat) and bool(code)
        ws.write(r, 4, "OK" if ok else "Mangler", fmt_ok if ok else fmt_err)
        if not ok: missing += 1

    ws.freeze_panes(1, 0); ws.autofilter(0, 0, len(accounts), len(headers)-1)
    for c, w in enumerate([12, 38, 26, 18, 18]):
        ws.set_column(c, c, w)
    return {"total": len(accounts), "missing": missing}


def _write_tax_sheet(wb, tax_rows: List[Dict[str, str]]):
    ws = wb.add_worksheet("MVA‑Mapping")
    fmt_head = wb.add_format({"bold": True})
    fmt_ok   = wb.add_format({"font_color": "green"})
    fmt_err  = wb.add_format({"font_color": "white", "bg_color": "#D9534F"})
    fmt_pct  = wb.add_format({"num_format": "0.####"})
    headers = [
        ("TaxCode", "TaxCode"),
        ("StandardTaxCode", "StandardTaxCode"),
        ("TaxType", "TaxType"),
        ("TaxPercentage", "TaxPercentage"),
        ("TaxCountryRegion", "TaxCountryRegion"),
        ("Description", "Description"),
        ("_VAL", "Status (1.3‑krav)"),
    ]
    for c, (_, title) in enumerate(headers):
        ws.write(0, c, title, fmt_head)

    missing = 0
    for r, row in enumerate(tax_rows, start=1):
        tcode = row.get("TaxCode",""); std = row.get("StandardTaxCode","")
        ttype = row.get("TaxType",""); tperc = row.get("TaxPercentage","")
        reg = row.get("TaxCountryRegion",""); desc = row.get("Description","")
        ws.write(r, 0, tcode); ws.write(r, 1, std); ws.write(r, 2, ttype)
        val = _safe_float(tperc)
        if val is not None: ws.write_number(r, 3, val, fmt_pct)
        else: ws.write(r, 3, tperc)
        ws.write(r, 4, reg); ws.write(r, 5, desc)
        ok = bool(std)
        ws.write(r, 6, "OK" if ok else "Mangler", fmt_ok if ok else fmt_err)
        if not ok: missing += 1

    ws.freeze_panes(1, 0); ws.autofilter(0, 0, len(tax_rows), len(headers)-1)
    for c, w in enumerate([20, 22, 14, 14, 18, 40, 18]):
        ws.set_column(c, c, w)
    return {"total": len(tax_rows), "missing": missing}


def _write_summary_sheet(wb, acc_stats: Dict[str,int], tax_stats: Dict[str,int]):
    ws = wb.add_worksheet("Oppsummering")
    head = wb.add_format({"bold": True})
    ws.write(0, 0, "Rapport", head); ws.write(0, 1, "Totalt", head); ws.write(0, 2, "Mangler mapping", head)
    ws.write(1, 0, "Konto–Næringsspesifikasjon"); ws.write_number(1, 1, acc_stats.get("total",0)); ws.write_number(1, 2, acc_stats.get("missing",0))
    ws.write(2, 0, "MVA‑Mapping"); ws.write_number(2, 1, tax_stats.get("total",0)); ws.write_number(2, 2, tax_stats.get("missing",0))
    ws.set_column(0, 0, 34); ws.set_column(1, 2, 18)


# ------------------- hoved-API -------------------

def generate(csv_dir: Path, excel_dir: Path, *, make_excel: bool = True):
    """
    Lager:
      - csv/mapping_accounts.csv
      - csv/mapping_tax.csv
      - csv/mapping_findings.csv
      - csv/mapping_findings_summary.csv
    og (valgfritt) excel/mapping_overview.xlsx

    Returnerer:
      (acc_csv, tax_csv, findings_csv, findings_summary_csv, xlsx_path|None, stats_dict)
      der stats_dict også inneholder 'accounts_source': 'accounts_csv' | 'probe_csv' | <forklaring>
    """
    csv_dir = Path(csv_dir); excel_dir = Path(excel_dir)

    # Account-mapping: prioritet accounts.csv -> probe_csv -> (kjør probe) -> tomt
    accounts, acc_src = _choose_account_mapping_source(csv_dir)

    # MVA-tabel
    tax_rows = _read_csv(csv_dir / "tax_table.csv")

    # Fulle mapping-CSV
    acc_csv = csv_dir / "mapping_accounts.csv"
    tax_csv = csv_dir / "mapping_tax.csv"
    acc_total, acc_missing = _write_accounts_csv(accounts, acc_csv)
    tax_total, tax_missing = _write_tax_csv(tax_rows, tax_csv)

    # Compliance-funn
    findings, stats = _collect_findings(accounts, tax_rows)
    findings_csv = csv_dir / "mapping_findings.csv"
    _write_findings_csv(findings, findings_csv)
    findings_summary_csv = csv_dir / "mapping_findings_summary.csv"
    _write_findings_summary_csv(stats, findings_summary_csv)

    # Excel (analyse)
    xlsx_path: Optional[Path] = None
    if make_excel and _HAS_XLSX:
        excel_dir.mkdir(parents=True, exist_ok=True)
        xlsx_path = excel_dir / "mapping_overview.xlsx"
        wb = xlsxwriter.Workbook(str(xlsx_path))
        try:
            _write_account_sheet(wb, accounts)
            _write_tax_sheet(wb, tax_rows)
            _write_summary_sheet(wb, {"total": acc_total, "missing": acc_missing},
                                    {"total": tax_total, "missing": tax_missing})
        finally:
            wb.close()

    stats.update({
        "accounts_source": acc_src,
        "accounts_total": acc_total,
        "tax_total": tax_total
    })
    return acc_csv, tax_csv, findings_csv, findings_summary_csv, xlsx_path, stats
