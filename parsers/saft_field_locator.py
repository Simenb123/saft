# -*- coding: utf-8 -*-
"""
Analyserer de første N Line-node(ene) i SAF‑T og rapporterer hvilke paths/alias som faktisk treffer.
Bruk: python -m app.parsers.saft_field_locator --input SAF-T.xml|zip --out outdir --sample 500
"""
from __future__ import annotations
from pathlib import Path
import argparse, zipfile, json
from collections import Counter, deque

from .saft_stream_parser import _lname, _alias_keys  # gjenbruk
try:
    from lxml import etree
except Exception as e:
    raise RuntimeError("pip install lxml") from e

TARGETS = ["AccountID","CustomerID","SupplierID","DocumentNumber","DebitAmount","CreditAmount","Amount","ValueDate"]

def _open_source(p: Path):
    p=Path(p)
    if p.suffix.lower()==".zip":
        zf = zipfile.ZipFile(p, "r")
        names=[n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not names: zf.close(); raise RuntimeError("Ingen .xml i zip")
        st = zf.open(names[0], "r")
        def closer():
            try: st.close()
            finally: zf.close()
        return st, closer
    fh = open(p, "rb")
    return fh, fh.close

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--out",   required=True)
    ap.add_argument("--sample", type=int, default=500)
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)

    paths = {k: Counter() for k in TARGETS}
    hits  = Counter()
    src, closer = _open_source(Path(args.input))
    try:
        ctx = etree.iterparse(src, events=("start","end"))
        n_lines = 0
        for evt, el in ctx:
            if evt=="end" and _lname(getattr(el,"tag","")) in ("Line","TransactionLine","JournalLine"):
                n_lines += 1
                q = deque([(el,0,"Line")])
                seen=0
                while q:
                    node,d,path = q.popleft()
                    if seen>3000: break
                    seen+=1
                    nm = _lname(getattr(node,"tag",""))
                    for tgt in TARGETS:
                        if nm.lower() in {a.lower() for a in _alias_keys(tgt)}:
                            if node.text and node.text.strip():
                                paths[tgt][path] += 1; hits[tgt]+=1
                            for ak,av in getattr(node,"attrib",{}).items():
                                if av and str(av).strip():
                                    paths[tgt][path+f"/@{_lname(ak)}"] += 1; hits[tgt]+=1
                    for ch in node:
                        q.append((ch, d+1, path + "/" + _lname(getattr(ch,"tag",""))))
                if n_lines >= args.sample:
                    break
                el.clear()
                par = el.getparent()
                if par is not None:
                    while True:
                        prev = el.getprevious()
                        if prev is None: break
                        par.remove(prev)
    finally:
        closer()

    rep = {
        "sampled_lines": n_lines,
        "hits": dict(hits),
        "top_paths_per_field": {k: dict(v.most_common(20)) for k,v in paths.items()}
    }
    (out/"field_paths.json").write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")
    md = ["# Felt‑paths (sample)\n", f"- Linjer i sample: {n_lines}\n"]
    for k in TARGETS:
        md.append(f"## {k}")
        for p,c in paths[k].most_common(20):
            md.append(f"- {p}  ({c})")
    (out/"field_paths.md").write_text("\n".join(md)+"\n", encoding="utf-8")
    print("[OK] Skrev", out/"field_paths.md")

if __name__ == "__main__":
    main()
