# -*- coding: utf-8 -*-
"""
saft_gl_overview.py
-------------------
Kontrollsummer og oversikter for General Ledger basert på CSV etter parsing.
"""
from __future__ import annotations
from pathlib import Path
import argparse, json
from collections import defaultdict, Counter
import pandas as pd

TX_COLS = [
    "RecordID","VoucherID","VoucherNo","JournalID","TransactionDate","PostingDate",
    "SystemID","BatchID","DocumentNumber","SourceDocumentID",
    "AccountID","CustomerID","SupplierID","Debit","Credit","Amount"
]

def _norm_num_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(" ", "", regex=False)
         .str.replace("\u00A0","", regex=False)
         .str.replace(",", ".", regex=False)
    )

def _read_chunks(path: Path, usecols=None, chunksize=250_000):
    return pd.read_csv(path, dtype=str, low_memory=False, usecols=usecols, chunksize=chunksize)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Sti til csv-mappe (etter parsing)")
    ap.add_argument("--out", default=None, help="(Valgfritt) skriv rapporter her i stedet for i csv-mappen")
    ap.add_argument("--eps", type=float, default=1e-6, help="Toleranse for balanse-sjekker (default 1e-6)")
    args = ap.parse_args()

    csv_dir = Path(args.csv)
    outdir = Path(args.out) if args.out else csv_dir
    outdir.mkdir(parents=True, exist_ok=True)

    tx_path = csv_dir / "transactions.csv"
    if not tx_path.exists():
        raise FileNotFoundError(f"Fant ikke {tx_path}")

    n_rows = 0
    sum_debit = 0.0
    sum_credit = 0.0
    sum_amount = 0.0
    missing = {c: 0 for c in TX_COLS}

    voucher_id_counter = Counter()
    voucher_no_counter = Counter()
    journal_counter = Counter()
    docno_counter = Counter()

    voucher_sum_id = defaultdict(float)
    voucher_sum_no = defaultdict(float)
    voucher_sum_pair = defaultdict(float)
    voucher_lines_id = Counter()
    voucher_lines_no = Counter()
    voucher_lines_pair = Counter()

    journal_sum = defaultdict(float)
    journal_lines = Counter()

    account_sum = defaultdict(float)
    account_lines = Counter()

    tdate_min = None; tdate_max = None
    pdate_min = None; pdate_max = None

    for ch in _read_chunks(tx_path, usecols=[c for c in TX_COLS if (tx_path.exists())]):
        n = len(ch); n_rows += n

        for c in ("Debit","Credit","Amount"):
            if c in ch.columns:
                v = pd.to_numeric(_norm_num_series(ch[c]), errors="coerce").fillna(0.0)
                if c == "Debit":  sum_debit  += float(v.sum())
                if c == "Credit": sum_credit += float(v.sum())
                if c == "Amount": sum_amount += float(v.sum())

        for c in TX_COLS:
            if c in ch.columns:
                missing[c] += (ch[c].isna() | (ch[c].astype(str).str.len() == 0)).sum()

        for col, counter in (("VoucherID", voucher_id_counter), ("VoucherNo", voucher_no_counter),
                             ("JournalID", journal_counter), ("DocumentNumber", docno_counter)):
            if col in ch.columns:
                vc = ch[col].dropna().astype(str).value_counts()
                counter.update(vc.to_dict())

        if "Amount" in ch.columns:
            amt = pd.to_numeric(_norm_num_series(ch["Amount"]), errors="coerce").fillna(0.0)

            if "VoucherID" in ch.columns:
                g = amt.groupby(ch["VoucherID"].fillna(""))
                for k, v in g.sum().items():
                    voucher_sum_id[k] += float(v)
                voucher_lines_id.update(ch["VoucherID"].fillna("").value_counts().to_dict())

            if "VoucherNo" in ch.columns:
                g = amt.groupby(ch["VoucherNo"].fillna(""))
                for k, v in g.sum().items():
                    voucher_sum_no[k] += float(v)
                voucher_lines_no.update(ch["VoucherNo"].fillna("").value_counts().to_dict())

            if {"VoucherID","VoucherNo"}.issubset(ch.columns):
                keypair = list(zip(ch["VoucherID"].fillna(""), ch["VoucherNo"].fillna("")))
                import pandas as _pd
                dfp = _pd.DataFrame({"pair": keypair, "Amount": amt})
                g = dfp.groupby("pair")["Amount"].sum()
                for k, v in g.items():
                    voucher_sum_pair[k] += float(v)
                from collections import Counter as _Counter
                voucher_lines_pair.update(_Counter(keypair))

            if "JournalID" in ch.columns:
                g = amt.groupby(ch["JournalID"].fillna(""))
                for k, v in g.sum().items():
                    journal_sum[k] += float(v)
                journal_lines.update(ch["JournalID"].fillna("").value_counts().to_dict())

            if "AccountID" in ch.columns:
                g = amt.groupby(ch["AccountID"].fillna(""))
                for k, v in g.sum().items():
                    account_sum[k] += float(v)
                account_lines.update(ch["AccountID"].fillna("").value_counts().to_dict())

        if "TransactionDate" in ch.columns:
            dt = pd.to_datetime(ch["TransactionDate"], errors="coerce")
            tmin, tmax = dt.min(), dt.max()
            if pd.notna(tmin):
                tdate_min = tmin if tdate_min is None else min(tdate_min, tmin)
            if pd.notna(tmax):
                tdate_max = tmax if tdate_max is None else max(tdate_max, tmax)
        if "PostingDate" in ch.columns:
            dp = pd.to_datetime(ch["PostingDate"], errors="coerce")
            pmin, pmax = dp.min(), dp.max()
            if pd.notna(pmin):
                pdate_min = pmin if pdate_min is None else min(pdate_min, pmin)
            if pd.notna(pmax):
                pdate_max = pmax if pdate_max is None else max(pdate_max, pmax)

    eps = float(args.eps)
    diff_global = (sum_debit - sum_credit - sum_amount)

    unbalanced_id   = {k: v for k, v in voucher_sum_id.items()   if abs(v) > eps and k}
    unbalanced_no   = {k: v for k, v in voucher_sum_no.items()   if abs(v) > eps and k}
    unbalanced_pair = {k: v for k, v in voucher_sum_pair.items() if abs(v) > eps and (k[0] or k[1])}

    (outdir/"voucher_balance.csv").write_text(
        "KeyType,Key,Lines,SumAmount\n" +
        "\n".join(
            [f"VoucherID,{k},{voucher_lines_id.get(k,0)},{voucher_sum_id[k]}" for k in voucher_sum_id.keys()] +
            [f"VoucherNo,{k},{voucher_lines_no.get(k,0)},{voucher_sum_no[k]}" for k in voucher_sum_no.keys()] +
            [f"Pair,\"{k[0]}|{k[1]}\",{voucher_lines_pair.get(k,0)},{voucher_sum_pair[k]}" for k in voucher_sum_pair.keys()]
        ),
        encoding="utf-8"
    )

    import csv as _csv
    with open(outdir/"journal_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f); w.writerow(["JournalID","Lines","SumAmount"])
        for k in sorted(journal_sum.keys()):
            w.writerow([k, journal_lines.get(k,0), journal_sum[k]])
    with open(outdir/"account_summary.csv", "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f); w.writerow(["AccountID","Lines","SumAmount"])
        for k in sorted(account_sum.keys()):
            w.writerow([k, account_lines.get(k,0), account_sum[k]])
    with open(outdir/"document_duplicates.csv", "w", newline="", encoding="utf-8") as f:
        w = _csv.writer(f); w.writerow(["DocumentNumber","Count"])
        for k, c in docno_counter.items():
            if k and c > 1:
                w.writerow([k, c])

    overview = {
        "rows_transactions": int(n_rows),
        "sums": {
            "Debit": round(sum_debit, 2),
            "Credit": round(sum_credit, 2),
            "Amount": round(sum_amount, 2),
            "diff(Debit-Credit-Amount)": round(diff_global, 6),
        },
        "missing_fraction": {c: (missing[c]/n_rows if n_rows else None) for c in TX_COLS},
        "unique_counts": {
            "VoucherID": len(voucher_id_counter),
            "VoucherNo": len(voucher_no_counter),
            "JournalID": len(journal_counter),
            "DocumentNumber": len([k for k in docno_counter.keys() if k])
        },
        "date_ranges": {
            "TransactionDate": [str(tdate_min.date()) if tdate_min is not None else None,
                                str(tdate_max.date()) if tdate_max is not None else None],
            "PostingDate": [str(pdate_min.date()) if pdate_min is not None else None,
                            str(pdate_max.date()) if pdate_max is not None else None],
        },
        "unbalanced": {
            "by_VoucherID_count": len(unbalanced_id),
            "by_VoucherNo_count": len(unbalanced_no),
            "by_Pair_count": len(unbalanced_pair),
            "examples_VoucherID": dict(sorted(unbalanced_id.items(), key=lambda kv: -abs(kv[1]))[:20]),
            "examples_VoucherNo": dict(sorted(unbalanced_no.items(), key=lambda kv: -abs(kv[1]))[:20]),
            "examples_Pair": {f"{k[0]}|{k[1]}": v for k, v in list(sorted(unbalanced_pair.items(), key=lambda kv: -abs(kv[1]))[:20])},
        },
        "top_journals_by_abs_amount": [
            {"JournalID": k, "SumAmount": v, "Lines": journal_lines.get(k,0)}
            for k, v in sorted(journal_sum.items(), key=lambda kv: -abs(kv[1]))[:20]
        ],
        "top_accounts_by_abs_amount": [
            {"AccountID": k, "SumAmount": v, "Lines": account_lines.get(k,0)}
            for k, v in sorted(account_sum.items(), key=lambda kv: -abs(kv[1]))[:50]
        ],
        "duplicates": {
            "DocumentNumber_gt1": len([1 for _, c in docno_counter.items() if c > 1]),
            "top_DocumentNumber": dict(docno_counter.most_common(20))
        }
    }

    (outdir/"gl_overview.json").write_text(json.dumps(overview, ensure_ascii=False, indent=2), encoding="utf-8")

    with open(outdir/"gl_overview.md", "w", encoding="utf-8") as f:
        f.write("# General Ledger – Oversikt og kontroller\n\n")
        f.write(f"Rader i transactions.csv: {overview['rows_transactions']:,}\n\n".replace(",", " "))
        f.write("## Sumposter\n")
        for k, v in overview["sums"].items():
            f.write(f"- {k:28s}: {v}\n")
        f.write("\n## Unike nøkler\n")
        for k, v in overview["unique_counts"].items():
            f.write(f"- {k:12s}: {v}\n")
        f.write("\n## Dato-spenn\n")
        for k, rng in overview["date_ranges"].items():
            f.write(f"- {k:16s}: {rng[0]} → {rng[1]}\n")
        f.write("\n## Manglende felt (andel av rader)\n")
        for k, frac in overview["missing_fraction"].items():
            if frac is not None:
                f.write(f"- {k:16s}: {frac:.2%}\n")
        f.write("\n## Ubalanserte bilag (eksempler, basert på Amount)\n")
        f.write(f"- By VoucherID: {overview['unbalanced']['by_VoucherID_count']}\n")
        for k, v in overview["unbalanced"]["examples_VoucherID"].items():
            f.write(f"    • {k}: {v}\n")
        f.write(f"- By VoucherNo: {overview['unbalanced']['by_VoucherNo_count']}\n")
        for k, v in overview["unbalanced"]["examples_VoucherNo"].items():
            f.write(f"    • {k}: {v}\n")
        f.write(f"- By Pair(ID|No): {overview['unbalanced']['by_Pair_count']}\n")
        for k, v in overview["unbalanced"]["examples_Pair"].items():
            f.write(f"    • {k}: {v}\n")
        f.write("\n## Topp journaler (absolutt beløp)\n")
        for row in overview["top_journals_by_abs_amount"]:
            f.write(f"- {row['JournalID']}: Sum={row['SumAmount']}, Lines={row['Lines']}\n")
        f.write("\n## Topp konti (absolutt beløp)\n")
        for row in overview["top_accounts_by_abs_amount"]:
            f.write(f"- {row['AccountID']}: Sum={row['SumAmount']}, Lines={row['Lines']}\n")
        f.write("\n## Duplikate dokumentnumre\n")
        f.write(f"- Antall DocumentNumber med count>1: {overview['duplicates']['DocumentNumber_gt1']}\n")
        for k, c in overview["duplicates"]["top_DocumentNumber"].items():
            f.write(f"    • {k}: {c}\n")

    print("[OK] Skrev:",
          outdir/"gl_overview.md",
          outdir/"gl_overview.json",
          outdir/"voucher_balance.csv",
          outdir/"journal_summary.csv",
          outdir/"account_summary.csv",
          outdir/"document_duplicates.csv")

if __name__ == "__main__":
    main()
