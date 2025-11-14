# -*- coding: utf-8 -*-
"""
XML-probe for SAF-T: teller tags, måler dybder og sjekker om viktige felter finnes i Line-subtrær.
Bruk:
  python -m app.parsers.saft_xml_probe --input path/to/SAF-T.xml|zip --out path/to/outdir

Skriver:
  - tag_stats.json: {tag: {count, min_depth, max_depth, sample_parents: {...}}}
  - tag_stats.txt : menneskelig lesbar oversikt
  - line_field_presence.json: funn av AccountID/CustomerID/SupplierID/Amount under Line (dybde 0..3)
"""
from __future__ import annotations
from pathlib import Path
import argparse, zipfile, json
from collections import defaultdict

try:
    from lxml import etree
except Exception as e:
    raise RuntimeError("saft_xml_probe krever lxml. Installer med: pip install lxml") from e

def _lname(tag: str) -> str:
    if not tag:
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag

def _open_source(p: Path):
    if p.suffix.lower() == ".zip":
        zf = zipfile.ZipFile(p, "r")
        names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not names:
            zf.close()
            raise RuntimeError("Ingen .xml i zip")
        st = zf.open(names[0], "r")
        def _closer():
            try: st.close()
            finally: zf.close()
        return st, _closer
    else:
        fh = open(p, "rb")
        def _closer():
            fh.close()
        return fh, _closer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="SAF-T .xml eller .zip")
    ap.add_argument("--out", required=True, help="Output-mappe for rapporter")
    args = ap.parse_args()

    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)
    src, closer = _open_source(Path(args.input))

    # stats
    count = defaultdict(int)
    min_depth = defaultdict(lambda: 10**9)
    max_depth = defaultdict(int)
    parent_rel = defaultdict(lambda: defaultdict(int))

    # line-field presence per depth
    fields = ("AccountID","CustomerID","SupplierID","Amount")
    line_presence = {f: defaultdict(int) for f in fields}

    stack = []  # stack of tag names

    try:
        for evt, el in etree.iterparse(src, events=("start","end")):
            tag = _lname(getattr(el, "tag", ""))
            if evt == "start":
                stack.append(tag)
                continue

            # end
            depth = len(stack)-1  # 0-based depth
            count[tag] += 1
            if depth < min_depth[tag]: min_depth[tag] = depth
            if depth > max_depth[tag]: max_depth[tag] = depth
            if depth >= 1:
                parent = stack[-2]
                parent_rel[parent][tag] += 1

            # Line field checks up to depth 3
            if tag in ("Line","TransactionLine","JournalLine"):
                # walk subtree quickly (children only, shallow)
                for d1 in el:
                    nm1 = _lname(getattr(d1, "tag", ""))
                    if nm1 in fields and (d1.text or "").strip():
                        line_presence[nm1][1] += 1
                    for d2 in d1:
                        nm2 = _lname(getattr(d2, "tag", ""))
                        if nm2 in fields and (d2.text or "").strip():
                            line_presence[nm2][2] += 1
                        for d3 in d2:
                            nm3 = _lname(getattr(d3, "tag", ""))
                            if nm3 in fields and (d3.text or "").strip():
                                line_presence[nm3][3] += 1

            # GC
            el.clear()
            if stack:
                stack.pop()
    finally:
        closer()

    # skriv ut
    tag_stats = {}
    for t in sorted(count.keys()):
        tag_stats[t] = {
            "count": count[t],
            "min_depth": int(min_depth[t]) if count[t] else None,
            "max_depth": int(max_depth[t]) if count[t] else None,
            "sample_parents": dict(sorted(parent_rel[t].items(), key=lambda kv: -kv[1])[:8]) if t in parent_rel else {}
        }

    with open(outdir/"tag_stats.json", "w", encoding="utf-8") as f:
        json.dump(tag_stats, f, ensure_ascii=False, indent=2)

    with open(outdir/"tag_stats.txt", "w", encoding="utf-8") as f:
        f.write("TAG STATISTIKK (count, min_depth, max_depth)\n")
        for t in sorted(count.keys()):
            f.write(f"{t:30s}  count={count[t]:>10d}  depth=[{min_depth[t]},{max_depth[t]}]\n")
        f.write("\nFORELDRE→BARN (topp relasjoner):\n")
        for p, ch in sorted(parent_rel.items()):
            top = sorted(ch.items(), key=lambda kv: -kv[1])[:8]
            f.write(f"  {p} -> {', '.join([f'{c}({n})' for c,n in top])}\n")

    with open(outdir/"line_field_presence.json", "w", encoding="utf-8") as f:
        json.dump({k: dict(v) for k, v in line_presence.items()}, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
