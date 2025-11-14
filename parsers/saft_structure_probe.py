# -*- coding: utf-8 -*-
"""
saft_structure_probe.py
-----------------------
Full struktur- og innholdsprobe for SAF-T (NO) direkte på XML/ZIP.
"""
from __future__ import annotations
from pathlib import Path
import argparse, json, zipfile
from collections import defaultdict, Counter, deque

try:
    from lxml import etree
except Exception as e:
    raise RuntimeError("Installer lxml: pip install lxml") from e

ALIAS = {
    "AccountID": ["AccountID","GLAccount","GLAccountID","GeneralLedgerAccountID","Account","AccountNo","AccountNumber","AccountCode"],
    "CustomerID": ["CustomerID","Customer","CustomerNo","CustomerNumber","CustomerCode","CustomerRef","PartyID","CustomerPartyID","CustomerRefNo"],
    "SupplierID": ["SupplierID","Supplier","Vendor","VendorID","VendorNo","SupplierNumber","SupplierCode","SupplierRef","PartyID","SupplierPartyID","VendorNumber"],
    "DocumentNumber": ["DocumentNumber","DocumentNo","DocNumber","VoucherNumber","VoucherNo","ReferenceNumber","Reference","ReferenceNo","DocumentRef","DocumentID","DocumentId"],
    "SourceDocumentID": ["SourceDocumentID","SourceID","SourceDocID","SourceDocumentNo","SourceDocNo","SourceRef","SourceReference"],
    "RecordID": ["RecordID","LineID","RecordNo","RecordNumber","LineNumber"],
    "JournalID": ["JournalID","Journal","JournalNo","JournalCode"],
    "VoucherID": ["VoucherID","TransactionID","Voucher","EntryID","EntryNumber"],
    "VoucherNo": ["VoucherNo","VoucherNumber","TransactionNo","EntryNo"],
    "PostingDate": ["PostingDate","PostDate","GLDate","EntryDate"],
    "TransactionDate": ["TransactionDate","TransDate","DocumentDate","InvoiceDate","TransactionDatetime","TransactionDateTime"],
    "DebitAmount": ["DebitAmount","Debit","Debet"],
    "CreditAmount": ["CreditAmount","Credit","Kredit"],
    "Amount": ["Amount","LineAmount","AmountLCY","AmountNOK","AmountMST"],
}
INDICATOR_ALIAS = ["DebitCreditIndicator", "DC", "DrCr", "Type", "Sign", "Side"]

def _lname(tag: str) -> str:
    if not tag:
        return ""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag

def _text(el) -> str:
    t = getattr(el, "text", None)
    return t.strip() if t else ""

def _alias_keys(key_or_keys):
    if isinstance(key_or_keys, (list, tuple, set)):
        out = []
        for k in key_or_keys:
            out += ALIAS.get(k, [k])
    else:
        out = ALIAS.get(key_or_keys, [key_or_keys])
    seen, deduped = set(), []
    for n in out:
        nl = n.lower()
        if nl in seen:
            continue
        seen.add(nl); deduped.append(n)
    return deduped

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

def _find_first_value(el, keys, *, max_depth=6, max_nodes=3000):
    targets = {k.lower() for k in _alias_keys(keys)}
    dq = deque([(el, 0)])
    seen = 0
    while dq:
        node, d = dq.popleft()
        if seen > max_nodes:
            break
        seen += 1
        nm = _lname(getattr(node, "tag", "")).lower()
        if nm in targets:
            t = _text(node)
            if t:
                return t, d, "text"
            for ak, av in getattr(node, "attrib", {}).items():
                if _lname(ak).lower() in targets and str(av).strip():
                    return str(av).strip(), d, "attr"
        for ak, av in getattr(node, "attrib", {}).items():
            if _lname(ak).lower() in targets and str(av).strip():
                return str(av).strip(), d, "attr"
        if d < max_depth:
            for ch in node:
                dq.append((ch, d+1))
    return None, None, "text"

def main():
    ap = argparse.ArgumentParser(description="SAF-T strukturprobe (XML/ZIP).")
    ap.add_argument("--input", required=True, help="SAF-T .xml eller .zip")
    ap.add_argument("--out", required=True, help="Outputmappe for rapporter")
    ap.add_argument("--max-paths", type=int, default=5000, help="Maks antall unike paths (default 5000)")
    args = ap.parse_args()

    outdir = Path(args.out); outdir.mkdir(parents=True, exist_ok=True)
    src, closer = _open_source(Path(args.input))

    tag_count = Counter()
    min_depth = defaultdict(lambda: 10**9)
    max_depth = defaultdict(int)
    parent_child = defaultdict(lambda: Counter())
    attr_by_tag = defaultdict(lambda: Counter())
    path_count = Counter()

    gl_transactions = 0
    gl_lines = 0
    voucher_id_counter = Counter()
    voucher_no_counter = Counter()
    journal_counter = Counter()

    line_field_stats = {
        k: {"found_text":0,"found_attr":0,"depth_min":None,"depth_max":None}
        for k in ("AccountID","CustomerID","SupplierID","DocumentNumber","SourceDocumentID","DebitAmount","CreditAmount","Amount")
    }
    indicator_counter = Counter()

    stack = []

    try:
        for evt, el in etree.iterparse(src, events=("start","end")):
            tag = _lname(getattr(el, "tag", ""))

            if evt == "start":
                stack.append(tag)
                for ak in getattr(el, "attrib", {}):
                    attr_by_tag[tag][_lname(ak)] += 1
                continue

            depth = len(stack)-1
            tag_count[tag] += 1
            if depth < min_depth[tag]: min_depth[tag] = depth
            if depth > max_depth[tag]: max_depth[tag] = depth
            if depth >= 1:
                parent = stack[-2]
                parent_child[parent][tag] += 1
            if len(path_count) < args.max_paths or "/".join(stack) in path_count:
                path_count["/" + "/".join(stack)] += 1

            if tag == "Transaction":
                gl_transactions += 1
                jid, _, _ = _find_first_value(el, "JournalID", max_depth=4)
                vid, _, _ = _find_first_value(el, "VoucherID", max_depth=4)
                vno, _, _ = _find_first_value(el, "VoucherNo", max_depth=4)
                if jid: journal_counter[jid] += 1
                if vid: voucher_id_counter[vid] += 1
                if vno: voucher_no_counter[vno] += 1

            if tag in ("Line","TransactionLine","JournalLine"):
                gl_lines += 1
                for key in ("AccountID","CustomerID","SupplierID","DocumentNumber","SourceDocumentID","DebitAmount","CreditAmount","Amount"):
                    val, d, src_type = _find_first_value(el, key, max_depth=5)
                    if val is not None:
                        st = line_field_stats[key]
                        if src_type == "attr":
                            st["found_attr"] += 1
                        else:
                            st["found_text"] += 1
                        if d is not None:
                            st["depth_min"] = d if st["depth_min"] is None else min(st["depth_min"], d)
                            st["depth_max"] = d if st["depth_max"] is None else max(st["depth_max"], d)
                ind = None
                for ak, av in getattr(el, "attrib", {}).items():
                    if _lname(ak) in INDICATOR_ALIAS and str(av).strip():
                        ind = str(av).strip(); break
                if ind:
                    indicator_counter[ind.lower()] += 1

            el.clear()
            if stack:
                stack.pop()
    finally:
        closer()

    tag_stats = {
        t: {
            "count": int(tag_count[t]),
            "min_depth": int(min_depth[t]) if tag_count[t] else None,
            "max_depth": int(max_depth[t]) if tag_count[t] else None,
            "top_children": dict(parent_child[t].most_common(10)) if t in parent_child else {}
        }
        for t in sorted(tag_count.keys())
    }
    attributes = {t: dict(attr_by_tag[t].most_common(50)) for t in sorted(attr_by_tag.keys())}
    paths_top = dict(path_count.most_common(200))

    summary = {
        "tag_stats": tag_stats,
        "attributes_by_tag": attributes,
        "top_paths": paths_top,
        "general_ledger": {
            "transactions": gl_transactions,
            "lines": gl_lines,
            "unique_voucher_id": len(voucher_id_counter),
            "unique_voucher_no": len(voucher_no_counter),
            "unique_journal_id": len(journal_counter),
            "top_journals": dict(journal_counter.most_common(20)),
        },
        "line_fields": line_field_stats,
        "amount_indicator": dict(indicator_counter),
    }

    (outdir/"structure_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    with open(outdir/"structure_report.md", "w", encoding="utf-8") as f:
        f.write("# SAF-T strukturrapport\n\n")
        f.write("## General Ledger (overordnet)\n")
        f.write(f"- Transactions: {gl_transactions:,}\n".replace(",", " "))
        f.write(f"- Lines: {gl_lines:,}\n".replace(",", " "))
        f.write(f"- Unike VoucherID: {len(voucher_id_counter):,}\n".replace(",", " "))
        f.write(f"- Unike VoucherNo: {len(voucher_no_counter):,}\n".replace(",", " "))
        f.write(f"- Unike JournalID: {len(journal_counter):,}\n\n".replace(",", " "))

        f.write("## Line-felt (funn / dybde)\n")
        for k, st in summary["line_fields"].items():
            f.write(f"- {k:16s}: text={st['found_text']}, attr={st['found_attr']}, depth=[{st['depth_min']},{st['depth_max']}]\n")
        if summary["amount_indicator"]:
            f.write("\n## Debit/Credit indikator (rå observasjoner)\n")
            for k, v in summary["amount_indicator"].items():
                f.write(f"- {k}: {v}\n")

        f.write("\n## Tag-statistikk (topp)\n")
        for t, meta in list(tag_stats.items())[:40]:
            f.write(f"- {t:28s} count={meta['count']}, depth=[{meta['min_depth']},{meta['max_depth']}]\n")

        f.write("\n## Vanlige paths (topp 50)\n")
        for p, n in list(paths_top.items())[:50]:
            f.write(f"- {p}  ({n})\n")

        f.write("\n## Attributter per tag (topp)\n")
        for t, attrs in list(attributes.items())[:30]:
            f.write(f"- {t}:\n")
            for a, c in list(attrs.items())[:10]:
                f.write(f"    • @{a}  ({c})\n")

    print("[OK] Skrev:", outdir/"structure_summary.json", "og", outdir/"structure_report.md")

if __name__ == "__main__":
    main()
