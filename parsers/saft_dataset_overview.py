# -*- coding: utf-8 -*-
# Bruk: python saft_dataset_overview.py "<sti til Saft output>"
import sys, csv, os
from pathlib import Path

def count_rows(p: Path):
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            # -1 for å trekke fra header hvis fila har innhold
            n = sum(1 for _ in f)
            return max(0, n-1)
    except Exception:
        return None

def first_cols(p: Path, n=8):
    try:
        with p.open("r", encoding="utf-8", newline="") as f:
            r = csv.reader(f)
            header = next(r, [])
            return ", ".join(header[:n])
    except Exception:
        return ""

def main():
    if len(sys.argv)!=2:
        print("Bruk: python saft_dataset_overview.py <outdir>")
        sys.exit(2)
    outdir = Path(sys.argv[1])
    rows = []
    for p in sorted(outdir.glob("*.csv")):
        rows.append((p.name, count_rows(p), first_cols(p)))
    # skriv til skjerm
    width = max(len(n) for n,_,_ in rows) if rows else 12
    print(f"{'Filnavn'.ljust(width)} | Rader | Kolonner (første 8)")
    print("-"*(width+40))
    for name, cnt, cols in rows:
        print(f"{name.ljust(width)} | {cnt if cnt is not None else '-':>5} | {cols}")
    # lagre til overview.csv også
    with (outdir/"dataset_overview.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["file","rows","first_columns"])
        for name, cnt, cols in rows:
            w.writerow([name, cnt, cols])

if __name__ == "__main__":
    main()
