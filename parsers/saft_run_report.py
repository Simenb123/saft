# -*- coding: utf-8 -*-
"""
Leser parse_stats.json, parser_meta.json, gl_overview.json og structure_summary.json
og lager EN lesbar rapport: run_report.md (+ run_report.json).
Bruk:  python -m app.parsers.saft_run_report --job <mappa-med-csv-og-structure>
"""
from __future__ import annotations
from pathlib import Path
import argparse, json

def _safe_load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, help="Jobb-rot (mappa som inneholder csv/ og structure/)")
    args = ap.parse_args()
    root = Path(args.job)
    csv = root/"csv"; structure = root/"structure"

    stats = _safe_load(csv/"debug"/"parse_stats.json") or {}
    meta  = _safe_load(csv/"debug"/"parser_meta.json") or {}
    gl    = _safe_load(csv/"gl_overview.json") or {}
    stru  = _safe_load(structure/"structure_summary.json") or {}

    report = {
        "parser_meta": meta,
        "gl_overview": gl,
        "parse_stats": stats,
        "structure": {
            "general_ledger": (stru.get("general_ledger") or {}),
            "line_fields": (stru.get("line_fields") or {}),
            "top_paths": list((stru.get("top_paths") or {}).items())[:50]
        }
    }
    (root/"run_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    md = []
    md.append("# SAF‑T – Run‑rapport\n")
    md.append("## Parser\n")
    md.append(f"- Versjon: **{meta.get('parser_version','?')}**")
    md.append(f"- Støtter streng‑alias: **{meta.get('supports_string_keys','?')}**")
    md.append(f"- Wrapped amounts‑logikk: **{meta.get('wrapped_amount_logic','?')}**\n")

    if stats:
        lf = stats.get("lines_total", 0)
        md.append("## Parse‑stats\n")
        md.append(f"- Linjer i GL: **{lf:,}**".replace(",", " "))
        for fkey in ("AccountID","DocumentNumber","DebitAmount","CreditAmount","Amount"):
            row = stats.get("fields",{}).get(fkey,{})
            found = (row.get("found_text",0) + row.get("found_attr",0))
            miss  = row.get("missing",0)
            md.append(f"  - {fkey:14s}: funn={found:,}  mangler={miss:,}".replace(",", " "))
        md.append("")

    if gl:
        sums = gl.get("sums",{})
        md.append("## GL‑kontroller (fra gl_overview)\n")
        md.append(f"- Rader i transactions.csv: **{gl.get('rows_transactions',0):,}**".replace(",", " "))
        md.append(f"- Sumposter: Debit={sums.get('Debit',0)}, Credit={sums.get('Credit',0)}, Amount={sums.get('Amount',0)}, diff(D-C-A)={sums.get('diff(Debit-Credit-Amount)',0)}\n")
        md.append(f"- Unike: VoucherID={gl.get('unique_counts',{}).get('VoucherID',0)}, VoucherNo={gl.get('unique_counts',{}).get('VoucherNo',0)}, JournalID={gl.get('unique_counts',{}).get('JournalID',0)}\n")

    if stru:
        gls = stru.get("general_ledger",{})
        md.append("## Struktur (fra structure_probe)\n")
        md.append(f"- Transactions={gls.get('transactions',0):,}, Lines={gls.get('lines',0):,}".replace(",", " "))
        md.append(f"- Unike VoucherID={gls.get('unique_voucher_id',0)}, VoucherNo={gls.get('unique_voucher_no',0)}, JournalID={gls.get('unique_journal_id',0)}")
        lf = stru.get("line_fields",{})
        vals = []
        for k in ("AccountID","DocumentNumber","DebitAmount","CreditAmount","Amount"):
            st = lf.get(k, {})
            cnt = st.get("found_text",0) + st.get("found_attr",0)
            vals.append(f"{k}={cnt}")
        md.append("- Line‑felt (funn): " + ", ".join(vals))
        md.append("")

    alerts = []
    if stats and stats.get("lines_total",0):
        zero_core = all(stats.get("fields",{}).get(k,{}).get("found_text",0)+stats.get("fields",{}).get(k,{}).get("found_attr",0)==0 for k in ("AccountID","DebitAmount","CreditAmount","Amount"))
        if zero_core:
            alerts.append("Ingen nøkkelfelt funnet på linjer (AccountID og beløp). Sjekk versjonsmismatch/alias.")
    if alerts:
        md.append("## ⚠️ Alarmer")
        for a in alerts: md.append(f"- {a}")
        md.append("")

    (root/"run_report.md").write_text("\n".join(md)+"\n", encoding="utf-8")
    print("[OK] Skrev", root/"run_report.md")

if __name__ == "__main__":
    main()
