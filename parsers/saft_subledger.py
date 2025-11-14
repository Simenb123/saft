
# src/app/parsers/saft_subledger.py
# -*- coding: utf-8 -*-
"""
Subledger (AR/AP) – robust kontrollkonto-detektering, uten MVA-faner.
"""
from __future__ import annotations
from pathlib import Path
from typing import Optional, List, Tuple, Set
import pandas as pd

WRITE_COMPAT_SHEETS = False       # Prefiksark AP_/AR_*? (av)
WRITE_PARTYLESS_SHEETS = True     # Partyless-faner?

ACCOUNTING_FORMAT = '_-* # ##0,00_-;_-* (# ##0,00)_-;_-* "-"_-;_-@_-'
DATE_FORMAT = 'yyyy-mm-dd'

def _read_csv_safe(path: Path | str, dtype: str | dict = "str") -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(p, dtype=dtype, encoding="utf-8-sig", sep=None, engine="python")
    except Exception:
        try:
            return pd.read_csv(p, dtype=dtype, encoding="utf-8-sig", sep=";")
        except Exception:
            return pd.read_csv(p, dtype=dtype, encoding="utf-8")

def _find_csv(base: Path, name: str) -> Optional[Path]:
    for cand in [Path(base) / name, Path(base).parent / "csv" / name, Path(base) / "csv" / name]:
        if Path(cand).exists():
            return Path(cand)
    return None

def _parse_dates(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out

def _to_num(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    return out

def _sanitize(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str)

def _xlsx_writer(path: Path) -> pd.ExcelWriter:
    return pd.ExcelWriter(
        str(path),
        engine="xlsxwriter",
        datetime_format=DATE_FORMAT,
        engine_kwargs={"options": {
            "strings_to_urls": False,
            "strings_to_numbers": False,
            "strings_to_formulas": False
        }}
    )

def _apply_formats(xw, sheet_name: str, df: pd.DataFrame):
    try:
        ws = xw.sheets[sheet_name]
        book = xw.book
        fmt_num = book.add_format({"num_format": ACCOUNTING_FORMAT})
        fmt_dt  = book.add_format({"num_format": DATE_FORMAT})
        cols = list(df.columns)
        head = [len(str(c)) for c in cols]
        sample = df.head(500)
        for i, c in enumerate(cols):
            try:
                m = int(min(sample[c].astype(str).map(len).max(), 60))
            except Exception:
                m = 8
            width = max(10, min(60, max(head[i], m) + 2))
            if c in {"IB","PR","UB","UB_Norm","IB_Amount","PR_Amount","UB_Amount",
                     "Debit","Credit","Amount","SumAmount","Value",
                     "PR_Total","PR_Subledger","PR_Partyless","Diff_PR",
                     "IB_Total","UB_Total"}:
                ws.set_column(i, i, width, fmt_num)
            elif c == "Date":
                ws.set_column(i, i, width, fmt_dt)
            else:
                ws.set_column(i, i, width)
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, 0, len(cols) - 1)
    except Exception:
        pass

def _apply_overview_date_cells(xw, sheet_name: str, df: pd.DataFrame):
    try:
        ws = xw.sheets[sheet_name]
        book = xw.book
        fmt_dt = book.add_format({"num_format": DATE_FORMAT})
        for label in ("Period start", "Period end"):
            idx = df.index[df["Metric"] == label].tolist()
            if not idx:
                continue
            r = 1 + idx[0]
            v = df.loc[idx[0], "Value"]
            if pd.notna(v):
                ws.write_datetime(r, 1, pd.to_datetime(v), fmt_dt)
    except Exception:
        pass

def _write_sum_row(xw, sheet_name: str, df: pd.DataFrame, sum_cols: List[str]):
    try:
        if df is None or df.empty:
            return
        ws = xw.sheets[sheet_name]
        fmt = xw.book.add_format({"num_format": ACCOUNTING_FORMAT})
        r = 1 + len(df) + 1
        ws.write(r, 0, "SUM")
        for col in sum_cols:
            if col in df.columns:
                cidx = list(df.columns).index(col)
                val = float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())
                ws.write_number(r, cidx, val, fmt)
    except Exception:
        pass

_AR_ID = ["CustomerID","CustomerId","CustomerNo","CustomerNumber","Kundenr","KundeID","Kundenummer","PartyID"]
_AR_NM = ["CustomerName","Name","Customer","Kundenavn","PartyName"]
_AP_ID = ["SupplierID","VendorID","SupplierId","SupplierNo","SupplierNumber","LeverandorID","LevID","Leverandornr","VendorNo","PartyID"]
_AP_NM = ["SupplierName","VendorName","Name","LeverandorNavn","PartyName"]

def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in low:
            return low[cand.lower()]
    for c in cols:
        cl = c.lower()
        for cand in candidates:
            if cand.lower() in cl:
                return c
    return None

def _detect_party_cols(df: pd.DataFrame, side: str) -> tuple[str, str]:
    ids, nms = (_AR_ID, _AR_NM) if side == "AR" else (_AP_ID, _AP_NM)
    cols = list(df.columns)
    id_col = _pick_col(cols, ids) or "PartyID"
    nm_col = _pick_col(cols, nms) or "PartyName"
    if id_col not in df.columns:
        df[id_col] = ""
    if nm_col not in df.columns:
        df[nm_col] = ""
    return id_col, nm_col

def _clean_acc(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.replace(r"[^0-9]", "", regex=True)

def _control_candidates_from_mapping(out_dir: Path, side: str) -> set[str]:
    p = _find_csv(out_dir, "mapping_accounts.csv")
    if not p:
        return set()
    m = _read_csv_safe(p, dtype=str)
    if m.empty:
        return set()

    text_cols = [c for c in m.columns if any(k in c.lower() for k in
                 ["type","group","rolle","role","mapping","category","info","note"])]
    acc_col = None
    for c in m.columns:
        if c.lower() in {"accountid","konto","kontoid","account"}:
            acc_col = c; break
    if not acc_col:
        for c in m.columns:
            if "account" in c.lower():
                acc_col = c; break
    if not acc_col or acc_col not in m.columns:
        return set()

    side_tags = {
        "AP": ["ap","accounts payable","leverand","creditor","kreditor"],
        "AR": ["ar","accounts receivable","kunde","debitor","debitorer"],
    }[side]

    candidates = set()
    for _, row in m.iterrows():
        txt = " ".join([str(row.get(c, "")) for c in text_cols]).lower()
        if any(tag in txt for tag in side_tags):
            candidates.add(str(row[acc_col]))
    return {a for a in candidates if a}

def _control_candidates_from_data(tx: pd.DataFrame, side: str, party_col: str) -> set[str]:
    if "AccountID" not in tx.columns or party_col not in tx.columns or tx.empty:
        return set()
    acc = tx["AccountID"].astype(str)
    pc  = tx[party_col].astype(str)
    grp = pd.DataFrame({"acc": acc, "has_party": pc.ne("")}).groupby("acc")["has_party"].mean()
    cand = grp[grp >= 0.8].index.tolist()
    return set(cand)

def _prefiks_default(side: str) -> set[str]:
    return {"AP": {"24"}, "AR": {"15"}}[side]

def _control_mask_smart(tx: pd.DataFrame, out_dir: Path, side: str, party_col: str) -> pd.Series:
    if "AccountID" not in tx.columns:
        return pd.Series(False, index=tx.index)

    acc_clean = _clean_acc(tx["AccountID"])

    mapping_accs = _control_candidates_from_mapping(out_dir, side)
    if mapping_accs:
        mset = {_clean_acc(pd.Series([a])).iloc[0] for a in mapping_accs}
        mask_map = acc_clean.isin(mset)
        if mask_map.any():
            return mask_map

    data_accs = _control_candidates_from_data(tx, side, party_col)
    if data_accs:
        mset = {_clean_acc(pd.Series([a])).iloc[0] for a in data_accs}
        mask_dat = acc_clean.isin(mset)
        if mask_dat.any():
            return mask_dat

    pref = _prefiks_default(side)
    mask_pref = pd.Series(False, index=tx.index)
    for p in pref:
        mask_pref = mask_pref | acc_clean.str.startswith(p)
    if mask_pref.any():
        return mask_pref

    print(f"[excel] Advarsel: fant ingen kontrollkonti for {side}; bruker alle linjer med party-ID.")
    return tx[party_col].astype(str).ne("")

def _prepare_tx(outdir: Path) -> pd.DataFrame:
    txp = _find_csv(outdir, "transactions.csv")
    if not txp:
        return pd.DataFrame()
    tx = _read_csv_safe(txp, dtype=str)
    if tx.empty:
        return pd.DataFrame()

    tx = _parse_dates(tx, ["PostingDate","TransactionDate"])
    tx["Date"] = tx.get("PostingDate").fillna(tx.get("TransactionDate"))

    for c in ["AccountID","AccountDescription","VoucherID","VoucherNo","JournalID",
              "DocumentNumber","Text","Description","CustomerID","CustomerName",
              "SupplierID","SupplierName","TaxAmount","TaxPercent","TaxCode","TaxableBase"]:
        if c in tx.columns:
            tx[c] = _sanitize(tx[c])

    tx = _to_num(tx, ["Debit","Credit","TaxAmount","TaxableBase","TaxPercent","Amount"])
    if "Amount" not in tx.columns:
        tx["Amount"] = tx.get("Debit", 0.0) - tx.get("Credit", 0.0)

    if "IsGL" in tx.columns:
        mask = tx["IsGL"].astype(str).str.lower().isin(["true","1","ja","yes"])
        tx = tx.loc[mask].copy()
    return tx

def _range_dates(outdir: Path, tx_ctrl: pd.DataFrame,
                 date_from: Optional[str], date_to: Optional[str]) -> tuple[pd.Timestamp, pd.Timestamp]:
    d_min = pd.NaT; d_max = pd.NaT
    hdrp = _find_csv(outdir, "header.csv")
    if hdrp:
        hdr = _read_csv_safe(hdrp, dtype=str)
        for c in ["StartDate","FromDate","PeriodStart","FiscalYearStart"]:
            if c in hdr.columns:
                d = pd.to_datetime(hdr[c].iloc[0], errors="coerce")
                if pd.notna(d): d_min = d; break
        for c in ["EndDate","ToDate","PeriodEnd","FiscalYearEnd"]:
            if c in hdr.columns:
                d = pd.to_datetime(hdr[c].iloc[0], errors="coerce")
                if pd.notna(d): d_max = d; break

    if pd.isna(d_min) or pd.isna(d_max):
        dm = pd.to_datetime(tx_ctrl["Date"], errors="coerce")
        if pd.isna(d_min): d_min = dm.min()
        if pd.isna(d_max): d_max = dm.max()

    if date_from: d_min = pd.to_datetime(date_from)
    if date_to:   d_max = pd.to_datetime(date_to)

    if pd.notna(d_max) and pd.notna(d_min):
        span = (d_max - d_min).days
        if span > 460:
            d_min = pd.Timestamp(year=d_max.year, month=1, day=1)
        else:
            d_min = pd.Timestamp(year=d_min.year, month=d_min.month, day=1)

    if pd.isna(d_min): d_min = pd.Timestamp("1900-01-01")
    if pd.isna(d_max): d_max = pd.Timestamp("2999-12-31")
    if d_max < d_min:  d_min, d_max = d_max, d_min

    return d_min.normalize(), d_max.normalize()

def _balances(tx_ctrl: pd.DataFrame, party_col: str, dfrom, dto) -> pd.DataFrame:
    if tx_ctrl.empty or party_col not in tx_ctrl.columns:
        return pd.DataFrame(columns=[party_col,"IB","PR","UB"])
    has_party = tx_ctrl[party_col].astype(str).ne("")
    in_pr = (tx_ctrl["Date"].between(dfrom, dto, inclusive="both")) | (tx_ctrl["Date"].isna())
    ib = tx_ctrl.loc[has_party & (tx_ctrl["Date"] < dfrom)].groupby(party_col)["Amount"].sum().rename("IB")
    pr = tx_ctrl.loc[has_party & in_pr].groupby(party_col)["Amount"].sum().rename("PR")
    ub = tx_ctrl.loc[has_party & ((tx_ctrl["Date"] <= dto) | (tx_ctrl["Date"].isna()))].groupby(party_col)["Amount"].sum().rename("UB")
    out = pd.concat([ib, pr, ub], axis=1).fillna(0.0).reset_index()
    return out.loc[(out[["IB","PR","UB"]].abs().sum(axis=1) != 0.0)].copy()

def _top10(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return pd.DataFrame()
    w = df.copy()
    w["_abs"] = w[col].abs()
    w = w.sort_values("_abs", ascending=False).drop(columns=["_abs"])
    return w.head(10)

def _partyless(tx_ctrl: pd.DataFrame, party_col: str, dfrom, dto):
    if tx_ctrl.empty or party_col not in tx_ctrl.columns:
        return pd.DataFrame(), pd.DataFrame()
    in_scope = (tx_ctrl["Date"].between(dfrom, dto, inclusive="both")) | (tx_ctrl["Date"].isna())
    per = tx_ctrl.loc[in_scope].copy()
    det = per.loc[per[party_col].astype(str).eq("") | per[party_col].isna()].copy()
    if det.empty:
        return pd.DataFrame(), pd.DataFrame()
    sumdf = det.groupby("AccountID")["Amount"].sum().reset_index().rename(columns={"Amount":"SumAmount"})
    sumdf = sumdf.sort_values("SumAmount", ascending=False)
    return sumdf, det

def _missing_date(tx_ctrl: pd.DataFrame) -> pd.DataFrame:
    if tx_ctrl.empty or "Date" not in tx_ctrl.columns:
        return pd.DataFrame()
    md = tx_ctrl.loc[tx_ctrl["Date"].isna()].copy()
    if md.empty:
        return pd.DataFrame()
    keep = [c for c in ["AccountID","AccountDescription","VoucherID","VoucherNo","JournalID",
                        "DocumentNumber","CustomerID","SupplierID","Amount","Debit","Credit"] if c in md.columns]
    out = md[keep].copy()
    for c in ["Amount","Debit","Credit"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    return out

def _control_accounts_breakdown(tx_ctrl: pd.DataFrame, party_col: str, dfrom, dto) -> pd.DataFrame:
    if tx_ctrl.empty:
        return pd.DataFrame(columns=["AccountID","AccountDescription","PR_Total","PR_Subledger","PR_Partyless",
                                     "Diff_PR","IB_Total","UB_Total","Parties"])
    acc_cols = [c for c in ["AccountID","AccountDescription"] if c in tx_ctrl.columns] or ["AccountID"]
    in_pr_any = (tx_ctrl["Date"].between(dfrom, dto, inclusive="both")) | (tx_ctrl["Date"].isna())
    has_party = tx_ctrl[party_col].astype(str).ne("")
    per_all   = tx_ctrl.loc[in_pr_any].copy()
    per_party = tx_ctrl.loc[in_pr_any & has_party].copy()

    pr_total = per_all.groupby(acc_cols)["Amount"].sum().rename("PR_Total")
    pr_sl    = per_party.groupby(acc_cols)["Amount"].sum().rename("PR_Subledger")
    ib_tot   = tx_ctrl.loc[tx_ctrl["Date"] < dfrom].groupby(acc_cols)["Amount"].sum().rename("IB_Total")
    ub_tot   = tx_ctrl.loc[(tx_ctrl["Date"] <= dto) | (tx_ctrl["Date"].isna())].groupby(acc_cols)["Amount"].sum().rename("UB_Total")
    parties  = per_party.groupby(acc_cols)[party_col].nunique().rename("Parties")

    df = pd.DataFrame(pr_total).join([pr_sl, ib_tot, ub_tot, parties], how="outer").fillna(0.0).reset_index()
    df["PR_Partyless"] = df["PR_Total"] - df["PR_Subledger"]
    df["Diff_PR"] = df["PR_Subledger"] + df["PR_Partyless"] - df["PR_Total"]
    df["_abs"] = df["PR_Total"].abs()
    df = df.sort_values("_abs", ascending=False).drop(columns=["_abs"])
    return df

def make_subledger(out_dir: Path,
                   which: str,
                   date_from: Optional[str] = None,
                   date_to: Optional[str] = None) -> Path:
    out_dir = Path(out_dir)
    side = (which or "").upper()
    if side not in {"AR","AP"}:
        raise ValueError("which må være 'AR' eller 'AP'")

    tx = _prepare_tx(out_dir)
    if tx.empty:
        raise FileNotFoundError("transactions.csv mangler eller er tom")

    party_col, name_col = _detect_party_cols(tx, side)

    mask_ctrl = _control_mask_smart(tx, out_dir, side, party_col)
    tx_ctrl = tx.loc[mask_ctrl].copy()

    dfrom, dto = _range_dates(out_dir, tx_ctrl if not tx_ctrl.empty else tx, date_from, date_to)

    per_scope = (tx_ctrl["Date"].between(dfrom, dto, inclusive="both")) | (tx_ctrl["Date"].isna())
    per_all = tx_ctrl.loc[per_scope].copy()
    per_all["DateStatus"] = per_all["Date"].apply(lambda x: "Missing" if pd.isna(x) else "OK")
    per = per_all.loc[per_all[party_col].astype(str).ne("")].copy()

    bal = _balances(tx_ctrl, party_col, dfrom, dto)
    if not bal.empty:
        nm_map = tx_ctrl[[party_col, name_col]].drop_duplicates().rename(columns={name_col:"PartyName"})
        bal = bal.merge(nm_map, on=party_col, how="left")
        if "PartyName" not in bal.columns:
            bal["PartyName"] = ""
        bal["UB_Norm"] = bal["UB"] if side == "AR" else -bal["UB"]
        bal = bal[[party_col,"PartyName","IB","PR","UB","UB_Norm"]].sort_values("UB_Norm", ascending=False)
    else:
        bal = pd.DataFrame(columns=[party_col,"PartyName","IB","PR","UB","UB_Norm"])

    prefer = ["Date","DateStatus","VoucherID","VoucherNo","JournalID","DocumentNumber",
              "AccountID","AccountDescription", party_col, name_col,
              "Text","Description","Debit","Credit","Amount"]
    tx_cols = [c for c in prefer if c in per.columns] + [c for c in per.columns if c not in prefer]
    trans = per.reindex(columns=tx_cols).sort_values(
        [c for c in ["Date","DateStatus",party_col,"AccountID","VoucherID","VoucherNo"] if c in tx_cols],
        na_position="last"
    )

    partyless_sum, partyless_det = _partyless(tx_ctrl, party_col, dfrom, dto)
    missing_date = _missing_date(tx_ctrl)
    top10 = _top10(bal[[c for c in [party_col,"PartyName","UB_Norm"] if c in bal.columns]], "UB_Norm") if not bal.empty else pd.DataFrame()
    ctrl_df = _control_accounts_breakdown(tx_ctrl, party_col, dfrom, dto)

    gl_movement_total = float(pd.to_numeric(ctrl_df.get("PR_Total", pd.Series()), errors="coerce").fillna(0.0).sum())
    sum_tx = float(pd.to_numeric(trans.get("Amount", pd.Series()), errors="coerce").fillna(0.0).sum())
    sum_pr_subledger = float(pd.to_numeric(bal.get("PR", pd.Series()), errors="coerce").fillna(0.0).sum())
    pl_sum = float(pd.to_numeric(partyless_sum.get("SumAmount", pd.Series()), errors="coerce").fillna(0.0).sum()) if WRITE_PARTYLESS_SHEETS and not partyless_sum.empty else 0.0
    diff_tx_vs_sub = sum_tx - (sum_pr_subledger + pl_sum)
    diff_gl_vs_sub = gl_movement_total - (sum_pr_subledger + pl_sum)

    count_label = "Antall kunder (i perioden)" if side == "AR" else "Antall leverandører (i perioden)"
    overview = pd.DataFrame([
        {"Metric":"Period start", "Value": dfrom},
        {"Metric":"Period end",   "Value": dto},
        {"Metric":"Transactions in period", "Value": int(trans.shape[0])},
        { "Metric": count_label,  "Value": int(bal.shape[0]) },
        {"Metric":"Sum Debit (period)",  "Value": float(trans.get("Debit", pd.Series()).sum() if "Debit" in trans.columns else 0.0)},
        {"Metric":"Sum Credit (period)", "Value": float(trans.get("Credit", pd.Series()).sum() if "Credit" in trans.columns else 0.0)},
        {"Metric":"Net Amount (period)", "Value": sum_tx},
    ])

    summary = pd.DataFrame([
        { "Metric": count_label,                                "Value": int(bal.shape[0]) },
        {"Metric":"GL movement (control accounts)",             "Value": gl_movement_total},
        {"Metric":"Subledger movement (sum PR via reskontro)",  "Value": sum_pr_subledger},
        {"Metric":"Partyless (period)",                         "Value": pl_sum},
        {"Metric":"Diff (GL − (Reskontro + Partyless))",        "Value": diff_gl_vs_sub},
        {"Metric":"Sum Transactions (period) [reskontro-linjer]","Value": sum_tx},
        {"Metric":"Diff (Tx − (Reskontro + Partyless))",        "Value": diff_tx_vs_sub},
        {"Metric":"Total UB_Norm",                              "Value": float(bal.get("UB_Norm", pd.Series()).sum() if "UB_Norm" in bal.columns else 0.0)},
    ])

    excel_dir = out_dir.parent / "excel"
    excel_dir.mkdir(parents=True, exist_ok=True)
    out_path = excel_dir / ("ar_subledger.xlsx" if side == "AR" else "ap_subledger.xlsx")

    with _xlsx_writer(out_path) as xw:
        overview.to_excel(xw, index=False, sheet_name="Overview")
        _apply_formats(xw, "Overview", overview)
        _apply_overview_date_cells(xw, "Overview", overview)

        summary.to_excel(xw, index=False, sheet_name="Summary")
        _apply_formats(xw, "Summary", summary)

        bal_out = bal.copy().drop(columns=["UB_Norm"], errors="ignore")
        bal_out.to_excel(xw, index=False, sheet_name="Balances")
        _apply_formats(xw, "Balances", bal_out)
        _write_sum_row(xw, "Balances", bal_out, ["IB","PR","UB"])

        trans.to_excel(xw, index=False, sheet_name="Transactions")
        _apply_formats(xw, "Transactions", trans)
        _write_sum_row(xw, "Transactions", trans, [c for c in ["Debit","Credit","Amount"] if c in trans.columns])

        if not top10.empty:
            top10.to_excel(xw, index=False, sheet_name="Top10")
            _apply_formats(xw, "Top10", top10)

        if not ctrl_df.empty:
            ctrl_df.to_excel(xw, index=False, sheet_name="Control_Accounts")
            _apply_formats(xw, "Control_Accounts", ctrl_df)
            _write_sum_row(xw, "Control_Accounts", ctrl_df,
                           ["PR_Total","PR_Subledger","PR_Partyless","IB_Total","UB_Total"])

        if WRITE_PARTYLESS_SHEETS and not partyless_sum.empty:
            partyless_sum.to_excel(xw, index=False, sheet_name="Partyless")
            _apply_formats(xw, "Partyless", partyless_sum)
            _write_sum_row(xw, "Partyless", partyless_sum, ["SumAmount"])
        if WRITE_PARTYLESS_SHEETS and not partyless_det.empty:
            partyless_det.to_excel(xw, index=False, sheet_name="Partyless_Details")
            _apply_formats(xw, "Partyless_Details", partyless_det)

        if not missing_date.empty:
            missing_date.to_excel(xw, index=False, sheet_name="MissingDate")
            _apply_formats(xw, "MissingDate", missing_date)

    print(f"[excel] Skrev subledger ({side}): {out_path}")
    return out_path

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Generer subledger (AR/AP)")
    p.add_argument("--out", dest="out_dir", required=True, help="Sti til CSV-mappen (../excel for utdata)")
    p.add_argument("--which", dest="which", required=True, choices=["AR","AP"])
    p.add_argument("--date-from", dest="date_from", default=None)
    p.add_argument("--date-to", dest="date_to", default=None)
    args = p.parse_args()
    make_subledger(Path(args.out_dir), args.which, date_from=args.date_from, date_to=args.date_to)
