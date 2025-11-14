# -*- coding: utf-8 -*-
"""
saft_mapping_probe.py
---------------------
Formål: Uten antakelser verifisere om SAF‑T-filen faktisk inneholder konto‑mapping til næringsspesifikasjonen.

Leser:
  - AuditFileVersion
  - GeneralLedgerAccounts/Account/*
    - AccountID, AccountDescription
    - GroupingCategory, GroupingCode (SAF‑T 1.3)
    - StandardAccountID (eldre mapping; kun til info)

Output (i valgt --outdir, typisk csv/):
  - mapping_probe_accounts.csv
  - mapping_probe.json (oppsummering)

Bruk:
  python -m app.parsers.saft_mapping_probe "C:\\sti\\Tripletex.zip" --outdir "C:\\...\\csv"
"""
from __future__ import annotations

import csv
import json
import sys
import zipfile
from pathlib import Path
from typing import Dict, Tuple, IO, Optional
import xml.etree.ElementTree as ET


def _local(tag: str) -> str:
    """Stripp namespace: '{ns}Tag' -> 'Tag'."""
    if '}' in tag:
        return tag.split('}', 1)[1]
    return tag

def _open_saft(input_path: Path) -> Tuple[IO[bytes], Optional[zipfile.ZipFile], str]:
    """
    Returner (fileobj, zip_handle|None, desc) slik at fileobj kan brukes av ET.iterparse.
    Caller må .close() både fileobj og zip om zip ikke er None.
    """
    if input_path.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(str(input_path), "r")
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xml_names:
            raise RuntimeError("ZIP inneholder ingen .xml")
        name = xml_names[0]
        return zf.open(name, "r"), zf, f"{input_path.name}:{name}"
    return open(str(input_path), "rb"), None, input_path.name


def _probe(input_path: Path, outdir: Optional[Path] = None) -> Dict:
    fobj, zf, desc = _open_saft(input_path)
    try:
        outdir = Path(outdir) if outdir else input_path.parent / f"{input_path.stem}_PROBE"
        outdir.mkdir(parents=True, exist_ok=True)

        header: Dict[str, str] = {}
        total_accounts = 0
        with_grouping = 0
        legacy_only = 0
        categories, codes = set(), set()

        csv_path = outdir / "mapping_probe_accounts.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as cf:
            w = csv.writer(cf, lineterminator="\n")
            w.writerow(["AccountID", "AccountDescription", "GroupingCategory", "GroupingCode", "StandardAccountID"])

            ctxt = ET.iterparse(fobj, events=("start", "end"))
            _, root = next(ctxt)

            in_header = False
            in_accounts = False
            cur = {}

            for ev, el in ctxt:
                tag = _local(el.tag)

                # Header
                if ev == "start" and tag == "Header":
                    in_header = True
                elif ev == "end" and tag == "Header":
                    in_header = False
                elif in_header and ev == "end":
                    if tag in ("AuditFileVersion", "CompanyName", "RegistrationNumber"):
                        header[tag] = (el.text or "").strip()

                # Accounts
                if ev == "start" and tag == "GeneralLedgerAccounts":
                    in_accounts = True
                elif ev == "end" and tag == "GeneralLedgerAccounts":
                    in_accounts = False

                if in_accounts:
                    if ev == "start" and tag == "Account":
                        cur = {"AccountID": "", "AccountDescription": "",
                               "GroupingCategory": "", "GroupingCode": "", "StandardAccountID": ""}
                    elif ev == "end" and tag == "Account":
                        total_accounts += 1
                        cat = cur.get("GroupingCategory", ""); code = cur.get("GroupingCode", ""); std = cur.get("StandardAccountID", "")
                        if cat: categories.add(cat)
                        if code: codes.add(code)
                        if cat and code:
                            with_grouping += 1
                        elif (not cat and not code) and std:
                            legacy_only += 1

                        w.writerow([cur.get("AccountID",""), cur.get("AccountDescription",""), cat, code, std])
                        root.clear()
                    elif ev == "end":
                        if tag in cur:
                            cur[tag] = (el.text or "").strip()

        summary = {
            "file": desc,
            "header": header,
            "counts": {
                "accounts_total": total_accounts,
                "accounts_with_grouping": with_grouping,
                "accounts_with_only_legacy_standard_account": legacy_only,
                "accounts_without_any_mapping": total_accounts - with_grouping - legacy_only,
            },
            "distinct_grouping_categories": sorted(categories),
            "distinct_grouping_codes_sample": sorted(list(codes))[:20],
            "output": {
                "accounts_csv": str(csv_path),
                "summary_json": str(outdir / "mapping_probe.json")
            }
        }
        (outdir / "mapping_probe.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        return summary
    finally:
        try: fobj.close()
        except Exception: pass
        if zf is not None:
            try: zf.close()
            except Exception: pass


def main(argv=None) -> int:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("input", help="SAF‑T .xml eller .zip")
    p.add_argument("--outdir", help="Output-mappe for probe (default: <filnavn>_PROBE)", default=None)
    args = p.parse_args(argv)
    summary = _probe(Path(args.input), Path(args.outdir) if args.outdir else None)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
