# src/app/parsers/saft_cli.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import os
import argparse
import sys

# Importer kjerne og kontroller
from .saft_reports import make_subledger, make_general_ledger, make_trial_balance
from .saft_reports import _complete_accounts_file  # valgfritt; nyttig ved kun-CSV
# Kontrollmotor
from .controls.run_all_checks import run_all_checks

# Parser (din eksisterende)
try:
    from saft_parser_pro import parse_saft  # SAF-T parser v1.3 klar :contentReference[oaicite:14]{index=14}
except Exception as e:
    parse_saft = None

# ----- intern util -----
def _ensure_dirs(root: Path) -> tuple[Path, Path]:
    csv_dir = root / "csv"
    excel_dir = root / "excel"
    csv_dir.mkdir(parents=True, exist_ok=True)
    excel_dir.mkdir(parents=True, exist_ok=True)
    return csv_dir, excel_dir

def _move_to_excel(excel_dir: Path, *paths: Path) -> None:
    for p in paths:
        if p is None:
            continue
        try:
            dest = excel_dir / p.name
            if dest.exists():
                dest.unlink()
            os.replace(p, dest)
        except Exception:
            # la fil ligge hvor den ble skrevet
            pass

def run_full_process(input_path: Path, outdir: Path) -> None:
    """
    Full pipeline:
      1) parse_saft -> csv/
      2) generer rapporter (AR/AP/GL/TB)
      3) kjør kontroller -> control_report.xlsx
      4) flytt alle Excel til excel/
    """
    csv_dir, excel_dir = _ensure_dirs(outdir)
    if parse_saft is None:
        raise RuntimeError("parse_saft ikke tilgjengelig (saft_parser_pro mangler)")
    # 1) SAF‑T → CSV
    parse_saft(input_path, csv_dir)
    # 2) Rapporter (bruk CSV som kilde)
    ar_path = make_subledger(csv_dir, "AR")
    ap_path = make_subledger(csv_dir, "AP")
    gl_path = make_general_ledger(csv_dir)
    tb_path = make_trial_balance(csv_dir)
    # 3) Kontroller
    ctrl_path = run_all_checks(csv_dir)
    # 4) Flytt til excel/
    _move_to_excel(excel_dir, ar_path, ap_path, gl_path, tb_path, ctrl_path)
    print(f"Ferdig! Excel‑rapporter i: {excel_dir}  | CSV: {csv_dir}")

# ----- GUI (valgfritt, enkel) -----
def _launch_gui() -> None:
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except Exception:
        print("tkinter ikke tilgjengelig. Bruk CLI.")
        return

    root = tk.Tk()
    root.title("SAF‑T – Parser + Rapporter + Kontroller")

    def run_full():
        p = filedialog.askopenfilename(title="Velg SAF‑T (.xml/.zip)", filetypes=[("SAF‑T/XML/ZIP","*.xml *.zip")])
        if not p:
            return
        out = filedialog.askdirectory(title="Velg output‑rotmappe")
        if not out:
            return
        try:
            run_full_process(Path(p), Path(out))
            messagebox.showinfo("OK", f"Ferdig! Se {Path(out)/'excel'}")
        except Exception as e:
            messagebox.showerror("Feil", str(e))

    def run_subledger(which: str):
        d = filedialog.askdirectory(title="Velg mappe med SAF‑T CSV‑filer")
        if not d:
            return
        try:
            # sikrer kontoplan først (robust mot mangler)
            _complete_accounts_file(Path(d))
            out = make_subledger(Path(d), which)
            messagebox.showinfo("OK", f"{which} subledger: {out}")
        except Exception as e:
            messagebox.showerror("Feil", str(e))

    tk.Button(root, text="Kjør full prosess (parse + rapporter + kontroller)", width=40, command=run_full).pack(padx=20, pady=8)
    tk.Button(root, text="Generer AR (fra CSV)", width=40, command=lambda: run_subledger("AR")).pack(padx=20, pady=4)
    tk.Button(root, text="Generer AP (fra CSV)", width=40, command=lambda: run_subledger("AP")).pack(padx=20, pady=4)
    tk.Button(root, text="Lukk", width=40, command=root.destroy).pack(padx=20, pady=12)
    root.mainloop()

# ----- CLI -----
def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="SAF‑T pipeline: parser → CSV → rapporter → kontroller (control_report.xlsx)"
    )
    p.add_argument("--input", type=str, default=None, help="SAF‑T .xml/.zip (brukes med --full)")
    p.add_argument("--outdir", type=str, default=".", help="Output rotmappe (lager csv/ og excel/)")
    p.add_argument("--which", type=str, choices=["AR","AP"], default="AR", help="Kun subledger fra eksisterende CSV")
    p.add_argument("--date_from", type=str, default=None)
    p.add_argument("--date_to", type=str, default=None)
    p.add_argument("--full", action="store_true", help="Kjør full pipeline (krever --input)")
    p.add_argument("--gui", action="store_true", help="Start enkel GUI")
    args = p.parse_args(argv)

    if args.gui or (len(sys.argv) == 1):
        _launch_gui()
        return 0

    outdir = Path(args.outdir)
    if args.full:
        if not args.input:
            p.error("--full krever --input")
        run_full_process(Path(args.input), outdir)
        return 0

    # Kun subledger fra CSV
    try:
        _complete_accounts_file(outdir)
    except Exception:
        pass
    path = make_subledger(outdir, args.which, args.date_from, args.date_to)
    print(f"Ferdig! Genererte {args.which} subledger: {path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
