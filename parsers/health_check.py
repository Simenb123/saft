# app/parsers/health_check.py
# -*- coding: utf-8 -*-
"""
En enkel helsesjekk for å verifisere at subledger/TB/GL finnes og har nødvendige faner.
Kjør:
    python -m app.parsers.health_check --out ".../csv"
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import sys

def _sheets(path: Path):
    if not path.exists():
        return []
    try:
        return pd.ExcelFile(path).sheet_names
    except Exception:
        return []

def check_outputs(out_dir: Path) -> int:
    out_dir = Path(out_dir)
    excel = out_dir.parent / "excel"
    ar = excel / "ar_subledger.xlsx"
    ap = excel / "ap_subledger.xlsx"
    tb = excel / "trial_balance.xlsx"
    gl = excel / "general_ledger.xlsx"

    ok = True

    want = {"Overview","Summary","Balances","Transactions","Top10","Partyless","Partyless_Details","MissingDate"}
    for name, p in (("AR", ar), ("AP", ap)):
        sh = set(_sheets(p))
        missing = sorted(list(want - sh))
        print(f"[check] {name}: {p.name} -> sheets={sorted(sh)}")
        if missing:
            ok = False
            print(f"[check] {name}: mangler faner: {', '.join(missing)}")

    for lbl, p in (("TB", tb), ("GL", gl)):
        print(f"[check] {lbl}: {p.name} -> exists={p.exists()}")

    return 0 if ok else 1

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", dest="out_dir", required=True)
    args = ap.parse_args()
    sys.exit(check_outputs(Path(args.out_dir)))
