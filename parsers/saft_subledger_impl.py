# app/parsers/saft_subledger_impl.py
# -*- coding: utf-8 -*-
"""
Fullverdig AR/AP subledger-implementasjon (for GUI/skript).
Fokus:
  - Korrekt filtrering (kun linjer med party for valgt side)
  - Robuste kolonnevalg for party-id/-navn (mange varianter)
  - IB/PR/UB-beregning i perioden + Top10 (absolutt UB)
  - Partyless (summary + details) og MissingDate
  - Norsk tallformat i Excel (# ##0,00;[Red]-# ##0,00) + datoformat (yyyy-mm-dd)
  - Generiske ark *og* kompatibilitetsark (AR_/AP_ prefiks med *_Amount-kolonner)
  - Ingen import av saft_reports (unngår sirkler)

Skriver til:
    ../excel/ar_subledger.xlsx  eller  ../excel/ap_subledger.xlsx
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional, List, Dict, Tuple
import pandas as pd
import re

# ---------------------------- helpers -------------------------------------

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

def _find_csv(outdir: Path, name: str) -> Optional[Path]:
    p = Path(outdir) / name
    if p.exists():
        return p
    for cand in [Path(outdir).parent / "csv" / name, Path(outdir) / "csv" / name]:
        if cand.exists():
            return cand
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

def _xlsx_opts():
    return {"options": {"strings_to_urls": False, "strings_to_numbers": False, "strings_to_formulas": False}}

# ------------------------ party heuristics --------------------------------
_AR_ID  = ["CustomerID","CustomerId","CustomerNo","CustomerNumber","Kundenr","KundeID","Kundenummer","PartyID"]
_AR_NM  = ["CustomerName","Name","Customer","Kundenavn","PartyName"]
_AP_ID  = ["SupplierID","VendorID","SupplierId","SupplierNo","SupplierNumber","LeverandorID","LevID","Leverandornr","VendorNo","PartyID"]
_AP_NM  = ["SupplierName","VendorName","Name","LeverandorNavn","PartyName"]

def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    low = {c.lower(): c for c in cols}
    for cand in candidates:  # eksakt
        k = cand.lower()
        if k in low: return low[k]
    # substring fallback
    for c in cols:
        cl = c.lower()
        for cand in candidates:
            if cand.lower() in cl:
                return c
    return None

def _detect_party_cols(df: pd.DataFrame, which: str) -> Tuple[str, str]:
    if which == "AR":
        ids, nms = _AR_ID, _AR_NM
    else:
        ids, nms = _AP_ID, _AP_NM
    cols = list(df.columns)
    id_col = _pick_col(cols, ids) or "PartyID"
    nm_col = _pick_col(cols, nms) or "PartyName"
    if id_col not in df.columns: df[id_col] = ""
    if nm_col not in df.columns: df[nm_col] = ""
    return id_col, nm_col

# -------------------------- core load -------------------------------------
def _prepare_tx(outdir: Path) -> pd.DataFrame:
    txp = _find_csv(outdir, "transactions.csv")
    if not txp:
        return pd.DataFrame()
    tx = _read_csv_safe(txp, dtype=str)
    if tx.empty:
        return pd.DataFrame()
    tx = _parse_dates(tx, ["PostingDate","TransactionDate"])
    tx["Date"] = tx.get("PostingDate").fillna(tx.get("TransactionDate"))
    # tekst
    for c in ["AccountID","AccountDescription","VoucherID","VoucherNo","JournalID","DocumentNumber","Text","Description",
              "CustomerID","CustomerName","SupplierID","SupplierName"]:
        if c in tx.columns:
            tx[c] = _sanitize(tx[c])
    # tall
    tx = _to_num(tx, ["Debit","Credit","TaxAmount","Amount"])
    if "Amount" not in tx.columns:
        tx["Amount"] = tx.get("Debit", 0.0) - tx.get("Credit", 0.0)
    # Kun GL-linjer hvis flagg finnes
    if "IsGL" in tx.columns:
        tx = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()
    return tx

def _range_dates(outdir: Path, tx: pd.DataFrame, date_from: Optional[str], date_to: Optional[str]):
    hdrp = _find_csv(outdir, "header.csv")
    d_min = pd.to_datetime(tx["Date"], errors="coerce").min()
    d_max = pd.to_datetime(tx["Date"], errors="coerce").max()
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
    if date_from: d_min = pd.to_datetime(date_from)
    if date_to:   d_max = pd.to_datetime(date_to)
    if pd.isna(d_min): d_min = pd.Timestamp("1900-01-01")
    if pd.isna(d_max): d_max = pd.Timestamp("2999-12-31")
    if d_max < d_min: d_min, d_max = d_max, d_min
    return d_min.normalize(), d_max.normalize()

# ----------------------- business rules -----------------------------------
def _balances(tx: pd.DataFrame, party_col: str, dfrom, dto) -> pd.DataFrame:
    if party_col not in tx.columns or tx.empty:
        return pd.DataFrame(columns=[party_col,"IB","PR","UB"])
    ib = tx.loc[tx["Date"] < dfrom].groupby(party_col)["Amount"].sum().rename("IB")
    pr = tx.loc[tx["Date"].between(dfrom, dto, inclusive="both")].groupby(party_col)["Amount"].sum().rename("PR")
    ub = tx.loc[tx["Date"] <= dto].groupby(party_col)["Amount"].sum().rename("UB")
    return pd.concat([ib,pr,ub], axis=1).fillna(0.0).reset_index()

def _top10(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df.empty or col not in df.columns:
        return pd.DataFrame()
    w = df.copy()
    w["_abs"] = w[col].abs()
    w = w.sort_values("_abs", ascending=False).drop(columns=["_abs"])
    return w.head(10)

def _partyless(tx: pd.DataFrame, party_col: str, dfrom, dto):
    if party_col not in tx.columns or tx.empty:
        return pd.DataFrame(), pd.DataFrame()
    per = tx.loc[tx["Date"].between(dfrom, dto, inclusive="both")].copy()
    det = per.loc[per[party_col].astype(str).eq("") | per[party_col].isna()].copy()
    if det.empty:
        return pd.DataFrame(), pd.DataFrame()
    sumdf = det.groupby("AccountID")["Amount"].sum().reset_index().rename(columns={"Amount":"SumAmount"})
    sumdf = sumdf.sort_values("SumAmount", ascending=False)
    return sumdf, det

def _missing_date(tx: pd.DataFrame, party_col: str) -> pd.DataFrame:
    if "Date" not in tx.columns or tx.empty:
        return pd.DataFrame()
    md = tx.loc[tx["Date"].isna()].copy()
    if md.empty:
        return pd.DataFrame()
    cols = [c for c in ["AccountID", party_col, "Amount"] if c in md.columns]
    if not cols:
        return pd.DataFrame()
    out = md[cols].copy()
    out["Amount"] = pd.to_numeric(out["Amount"], errors="coerce").fillna(0.0)
    return out.groupby(cols[:2])["Amount"].sum().reset_index()

# --------------------------- Excel utils ----------------------------------
def _writer(path: Path):
    return pd.ExcelWriter(str(path), engine="xlsxwriter", datetime_format="yyyy-mm-dd",
                         engine_kwargs=_xlsx_opts())

def _apply_formats(xw, sheet_name: str, df: pd.DataFrame):
    try:
        ws = xw.sheets[sheet_name]
        book = xw.book
        fmt_num = book.add_format({"num_format": "# ##0,00;[Red]-# ##0,00"})
        fmt_dt  = book.add_format({"num_format": "yyyy-mm-dd"})
        # enkelt autofit + formats
        cols = list(df.columns)
        head = [len(str(c)) for c in cols]
        sample = df.head(500)
        for i, c in enumerate(cols):
            try:
                m = int(min(sample[c].astype(str).map(len).max(), 60))
            except Exception:
                m = 8
            width = max(8, min(60, max(head[i], m) + 2))
            # velg format på tall/dato
            if c in {"IB","PR","UB","IB_Amount","PR_Amount","UB_Amount","Debit","Credit","Amount","SumAmount","UB_Norm"}:
                ws.set_column(i, i, width, fmt_num)
            elif c == "Date":
                ws.set_column(i, i, width, fmt_dt)
            else:
                ws.set_column(i, i, width)
        # Freeze header
        ws.freeze_panes(1, 0)
        # Autofilter
        ws.autofilter(0, 0, 0, len(cols)-1)
    except Exception:
        pass

# ----------------------------- public API ---------------------------------
def make_subledger(out_dir: Path, which: str,
                   date_from: Optional[str] = None, date_to: Optional[str] = None) -> Path:
    out_dir = Path(out_dir)
    which_u = (which or "").upper()
    if which_u not in {"AR","AP"}:
        raise ValueError("which må være 'AR' eller 'AP'")

    tx = _prepare_tx(out_dir)
    if tx.empty:
        raise FileNotFoundError("transactions.csv mangler eller er tom")

    dfrom, dto = _range_dates(out_dir, tx, date_from, date_to)

    party_col, name_col = _detect_party_cols(tx, which_u)

    # ---- Filtrering pr. side ----
    #   - behold kun linjer med party_id for valgt side
    #   - hvis begge party-kolonner finnes, sørg for at "den andre" er tom, for å unngå feilklassifisering
    per_all = tx.loc[tx["Date"].between(dfrom, dto, inclusive="both")].copy()

    if which_u == "AR":
        # behold transaksjoner som spesifikt har kundekolonnen utfylt
        per = per_all.loc[per_all[party_col].astype(str).ne("")]
        # ekskluder rene leverandør-linjer dersom begge finnes
        if "SupplierID" in per.columns:
            per = per.loc[per["SupplierID"].astype(str).eq("")]
    else:
        per = per_all.loc[per_all[party_col].astype(str).ne("")]
        if "CustomerID" in per.columns:
            per = per.loc[per["CustomerID"].astype(str).eq("")]

    # ---- Balances ----
    bal = _balances(tx, party_col, dfrom, dto)
    if not bal.empty:
        nm_map = tx[[party_col, name_col]].drop_duplicates().rename(columns={name_col:"PartyName"})
        bal = bal.merge(nm_map, on=party_col, how="left")
        if "PartyName" not in bal.columns: bal["PartyName"] = ""
        # normalisert UB for visning (AR: debet positiv, AP: kredit positiv)
        if which_u == "AP":
            bal["UB_Norm"] = -bal["UB"]
        else:
            bal["UB_Norm"] = bal["UB"]
        bal = bal[[party_col, "PartyName", "IB", "PR", "UB", "UB_Norm"]].sort_values(["UB_Norm"], ascending=False)
    else:
        bal = pd.DataFrame(columns=[party_col,"PartyName","IB","PR","UB","UB_Norm"])

    # ---- Transactions (kolonnerekkefølge) ----
    prefer_cols = [
        "Date","VoucherID","VoucherNo","JournalID","DocumentNumber",
        "AccountID","AccountDescription",
        party_col, name_col,
        "Text","Description","Debit","Credit","Amount"
    ]
    tx_cols = [c for c in prefer_cols if c in per.columns] + [c for c in per.columns if c not in prefer_cols]
    trans = per.reindex(columns=tx_cols).sort_values(
        [c for c in ["Date", party_col, "AccountID", "VoucherID", "VoucherNo"] if c in tx_cols],
        na_position="last"
    )

    # ---- Partyless + MissingDate ----
    partyless_sum, partyless_det = _partyless(tx, party_col, dfrom, dto)
    missing_date = _missing_date(tx, party_col)

    # ---- Top10 ----
    top10 = _top10(bal[[c for c in [party_col,"PartyName","UB_Norm"] if c in bal.columns]], "UB_Norm") if not bal.empty else pd.DataFrame()

    # ---- Overview/Summary ----
    overview = pd.DataFrame([
        {"Metric":"Period start", "Value": dfrom.strftime("%Y-%m-%d")},
        {"Metric":"Period end", "Value": dto.strftime("%Y-%m-%d")},
        {"Metric":"Transactions in period", "Value": int(per.shape[0])},
        {"Metric":"Distinct parties (in period)", "Value": int(per[party_col].nunique()) if party_col in per.columns else 0},
        {"Metric":"Sum Debit (period)", "Value": float(per["Debit"].sum() if "Debit" in per.columns else 0.0)},
        {"Metric":"Sum Credit (period)", "Value": float(per["Credit"].sum() if "Credit" in per.columns else 0.0)},
        {"Metric":"Net Amount (period)", "Value": float(per["Amount"].sum() if "Amount" in per.columns else 0.0)},
    ])

    summary_rows = []
    if not bal.empty:
        tot_abs = float(bal["UB_Norm"].abs().sum())
        top10_abs = float(top10["UB_Norm"].abs().sum()) if not top10.empty else 0.0
        summary_rows.extend([
            {"Metric":"Parties", "Value": int(bal.shape[0])},
            {"Metric":"Total abs UB_Norm", "Value": tot_abs},
            {"Metric":"Top10 abs share", "Value": (top10_abs/tot_abs) if tot_abs else 0.0},
            {"Metric":"Total UB_Norm", "Value": float(bal["UB_Norm"].sum())},
        ])
    summary = pd.DataFrame(summary_rows)

    # ----------------------------- skriv Excel -----------------------------
    excel_dir = out_dir.parent / "excel"
    excel_dir.mkdir(parents=True, exist_ok=True)
    out_path = excel_dir / ("ar_subledger.xlsx" if which_u == "AR" else "ap_subledger.xlsx")

    with _writer(out_path) as xw:
        # generiske ark
        overview.to_excel(xw, index=False, sheet_name="Overview");         _apply_formats(xw, "Overview", overview)
        summary.to_excel(xw, index=False, sheet_name="Summary");           _apply_formats(xw, "Summary", summary)
        bal_out = bal.copy().drop(columns=["UB_Norm"], errors="ignore")
        bal_out.to_excel(xw, index=False, sheet_name="Balances");          _apply_formats(xw, "Balances", bal_out)
        trans.to_excel(xw, index=False, sheet_name="Transactions");        _apply_formats(xw, "Transactions", trans)
        if not top10.empty:
            top10.to_excel(xw, index=False, sheet_name="Top10");           _apply_formats(xw, "Top10", top10)
        if not partyless_sum.empty:
            partyless_sum.to_excel(xw, index=False, sheet_name="Partyless"); _apply_formats(xw, "Partyless", partyless_sum)
        if not partyless_det.empty:
            partyless_det.to_excel(xw, index=False, sheet_name="Partyless_Details"); _apply_formats(xw, "Partyless_Details", partyless_det)
        if not missing_date.empty:
            missing_date.to_excel(xw, index=False, sheet_name="MissingDate"); _apply_formats(xw, "MissingDate", missing_date)

        # kompatibilitetsark (prefiks + *_Amount)
        pref = which_u + "_"
        trans.to_excel(xw, index=False, sheet_name=pref + "Transactions"); _apply_formats(xw, pref + "Transactions", trans)
        bal_amt = bal.copy().rename(columns={"IB":"IB_Amount","PR":"PR_Amount","UB":"UB_Amount"})
        bal_amt.to_excel(xw, index=False, sheet_name=pref + "Balances");   _apply_formats(xw, pref + "Balances", bal_amt)
        if not partyless_sum.empty:
            partyless_sum.to_excel(xw, index=False, sheet_name=pref + "Partyless"); _apply_formats(xw, pref + "Partyless", partyless_sum)

        # SUM-rad på Balances
        try:
            if not bal_out.empty:
                ws = xw.sheets["Balances"]; book = xw.book
                fmt_num = book.add_format({"num_format": "# ##0,00;[Red]-# ##0,00"})
                r = 1 + len(bal_out)
                ws.write(r-1, 0, "SUM")
                for name in ("IB","PR","UB"):
                    if name in bal_out.columns:
                        cidx = list(bal_out.columns).index(name)
                        val = float(pd.to_numeric(bal_out[name], errors="coerce").fillna(0).sum())
                        ws.write_number(r-1, cidx, val, fmt_num)
        except Exception:
            pass

    print(f"[excel] Skrev subledger ({which_u}): {out_path}")
    return out_path
