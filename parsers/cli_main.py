# cli_main.py
# -*- coding: utf-8 -*-
"""
CLI-kjøring uten Tkinter (for raskere oppstart).
Bruk:
    python cli_main.py "<sti til SAF-T.zip|.xml>" "<output-mappe>"
"""
from __future__ import annotations
import sys
from pathlib import Path

# Tillat både top-level `parsers` og `app.parsers`
import types
import importlib
if "parsers" not in sys.modules:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    importlib.import_module("parsers")
parsers = sys.modules["parsers"]
app_pkg = types.ModuleType("app")
app_pkg.parsers = parsers
sys.modules["app"] = app_pkg
sys.modules["app.parsers"] = parsers

def main(inp: str, outdir: str):
    from parsers.saft_reports import make_general_ledger, make_trial_balance, make_subledger
    from pathlib import Path
    csv_dir = Path(outdir) / "csv"
    csv_dir.mkdir(parents=True, exist_ok=True)
    # Merk: her forutsetter vi at upstream parser allerede har laget CSV.
    # Hvis ikke, må du kalle parse først. Dette er et minimalt eksempel.
    make_general_ledger(csv_dir)
    make_trial_balance(csv_dir)
    make_subledger(csv_dir, "AR")
    make_subledger(csv_dir, "AP")
    print("[ok] Ferdig – se 'excel/'-mappen ved siden av outdir")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Bruk: python cli_main.py <SAF-T-fil> <outdir>")
        sys.exit(2)
    main(sys.argv[1], sys.argv[2])
