# -*- coding: utf-8 -*-
"""
saft_full_run.py – Slank orkestrator som kjører HELE flyten:
  SAF‑T (XML/ZIP) -> CSV -> AR/AP/GL/TB -> kontrolldokument (control_report.xlsx)
  og legger alt pent i en outputmappe navngitt lik SAF‑T‑filen, med undermapper
  "csv" og "excel".

Denne filen lar deg beholde den eksisterende, store `run_saft_pro_gui.py` som et
bibliotek (vi gjenbruker make_subledger/make_general_ledger/make_trial_balance),
samtidig som vi tilpasser mappestruktur og kobler på kontrollmotoren.

Kjøring (CLI):
    python -m src.app.parsers.saft_full_run --input "Vitamail AS 2024 SAF-T ...zip" --outroot "./out"
    # -> ./out/Vitamail AS 2024 SAF-T .../csv  (CSV)  og  ./out/.../excel  (Excel)
"""
from __future__ import annotations
from pathlib import Path
import argparse
import os
import re
import shutil

# 1) SAF‑T parser (din eksisterende)
from saft_parser_pro import parse_saft  # type: ignore

# 2) Rapportgeneratorer (gjenbruker funksjoner i din store run_saft_pro_gui.py)
from run_saft_pro_gui import (
    make_subledger,
    make_general_ledger,
    make_trial_balance,
)

# 3) Kontrollmotor (robust import – enten pakket eller frittstående modul)
def _import_controls():
    try:
        # pakkeplassering
        from src.app.parsers.controls.run_all_checks import run_all_checks  # type: ignore
        return run_all_checks
    except Exception:
        try:
            from controls.run_all_checks import run_all_checks  # type: ignore
            return run_all_checks
        except Exception:
            try:
                from run_all_checks import run_all_checks  # type: ignore
                return run_all_checks
            except Exception as e:
                raise ImportError("Fant ikke controls.run_all_checks. Plasser run_all_checks.py i 'src/app/parsers/controls/' eller ved roten.") from e

# ---------- Utilities ----------
def _safe_folder_name(p: Path) -> str:
    """
    Lag en trygg mappenavnvariant av SAF‑T‑filens navn (uten endelse).
    Beholder norske tegn, mellomrom, bindestrek og parenteser.
    """
    base = p.name
    # fjern .zip/.xml
    base = re.sub(r'\.[Zz][Ii][Pp]$|\.[Xx][Mm][Ll]$', '', base)
    # bytt ut ulovlige tegn
    base = re.sub(r'[^0-9A-Za-zæøåÆØÅ_ .()\-\u2013\u2014]+', '_', base)
    base = re.sub(r'\s+', ' ', base).strip()
    return base[:150] if len(base) > 0 else "SAF-T_Output"

def _derive_outdirs(input_path: Path, outroot: Path) -> tuple[Path, Path, Path]:
    job_root = outroot / _safe_folder_name(input_path)
    csv_dir = job_root / "csv"
    excel_dir = job_root / "excel"
    csv_dir.mkdir(parents=True, exist_ok=True)
    excel_dir.mkdir(parents=True, exist_ok=True)
    return job_root, csv_dir, excel_dir

def _move_to(dest_dir: Path, *paths: Path) -> None:
    for p in paths:
        if p is None:
            continue
        p = Path(p)
        if not p.exists():
            continue
        dest = dest_dir / p.name
        try:
            if dest.exists():
                dest.unlink()
            os.replace(str(p), str(dest))
        except Exception:
            try:
                shutil.copy2(str(p), str(dest))
                p.unlink(missing_ok=True)
            except Exception:
                pass

# ---------- Main pipeline ----------
def run_full(input_file: Path, outroot: Path, asof: str | None = None) -> Path:
    """
    Kjør full pipeline og returner job_root (\".../<SAF-T-navn>/\").
    """
    run_all_checks = _import_controls()
    job_root, csv_dir, excel_dir = _derive_outdirs(input_file, outroot)

    # 1) Parse SAF-T -> CSV
    parse_saft(input_file, csv_dir)

    # 2) Rapporter (Excel) – skrives først i csv_dir, flyttes så til excel_dir
    ar_path = make_subledger(csv_dir, "AR")
    ap_path = make_subledger(csv_dir, "AP")
    gl_path = make_general_ledger(csv_dir)
    tb_path = make_trial_balance(csv_dir)

    # 3) Kontroller
    ctrl_path = run_all_checks(csv_dir, asof=asof)  # skriver control_report.xlsx i csv_dir

    # 4) Flytt Excel til excel/
    _move_to(excel_dir, ar_path, ap_path, gl_path, tb_path, ctrl_path)

    print(f"✅ Ferdig – CSV i:   {csv_dir}")
    print(f"✅ Ferdig – Excel i: {excel_dir}")
    return job_root

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Full SAF‑T pipeline: parse -> rapporter -> kontroller")
    ap.add_argument("--input", required=True, help="SAF‑T .zip eller .xml")
    ap.add_argument("--outroot", default=".", help="Rotmappe for output (det lages en undermappe etter SAF‑T‑filens navn)")
    ap.add_argument("--asof", default=None, help="Skjæringsdato for kontrollene (YYYY-MM-DD). Valgfri.")
    args = ap.parse_args()

    job_root = run_full(Path(args.input), Path(args.outroot), asof=args.asof)
    print(f"📁 Outputmappe: {job_root}")
    print(f"   ├─ csv/   (grunnlagsfiler)")
    print(f"   └─ excel/ (rapporter, inkl. control_report.xlsx)")

if __name__ == "__main__":
    main()
