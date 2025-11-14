# -*- coding: utf-8 -*-
"""
Fail-fast kontroller. Stopper (exit code 1) hvis nøkkelfelt er under terskel.
Bruk: python -m app.parsers.saft_selftest --csv JOBB/csv --min-fill 0.5
"""
from __future__ import annotations
from pathlib import Path
import argparse, json, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--min-fill", type=float, default=0.5, help="Minste akseptable funnrate for AccountID og beløp (default 0.5)")
    args = ap.parse_args()
    stats_path = Path(args.csv)/"debug"/"parse_stats.json"
    if not stats_path.exists():
        print("Mangler parse_stats.json", file=sys.stderr); sys.exit(1)
    s = json.loads(stats_path.read_text(encoding="utf-8"))
    lines = s.get("lines_total",0) or 0
    if not lines:
        print("Ingen linjer i GL.", file=sys.stderr); sys.exit(1)

    def fill_of(key):
        r = s.get("fields",{}).get(key,{})
        return (r.get("found_text",0)+r.get("found_attr",0))/float(lines)

    ok = True
    for k in ("AccountID","DebitAmount","CreditAmount","Amount"):
        if fill_of(k) < args.min_fill:
            print(f"[FAIL] Fyllgrad for {k} < {args.min_fill:.0%}", file=sys.stderr)
            ok=False
    if not ok: sys.exit(1)
    print("[OK] Selvtest bestått.")

if __name__ == "__main__":
    main()
