# app/parsers/saft_control_report.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterable, Tuple, List
import sys
import re
import pandas as pd

# Skriv "kjedelig" Excel for maksimal kompatibilitet
EXCEL_SAFE_MODE = True

# -------------------- I/O og helpers --------------------
def _read_csv_safe(path: Optional[Path], dtype=str) -> Optional[pd.DataFrame]:
    if path is None:
        return None
    try:
        return pd.read_csv(path, dtype=dtype, keep_default_na=False)
    except Exception:
        return None

def _find_csv_file(outdir: Path, filename: str) -> Optional[Path]:
    # Søk i outdir, foreldre og rekursivt under
    dirs: List[Path] = []
    cur = Path(outdir)
    while True:
        dirs.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent
    for d in dirs:
        p = d / filename
        if p.is_file():
            return p
    for base in dirs:
        try:
            for p in base.rglob(filename):
                if p.is_file():
                    return p
        except Exception:
            continue
    return None

def _to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df

def _parse_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def _has_value(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip().str.lower()
    return ~t.isin(["", "nan", "none", "nat"])

def _norm_acc(acc: str) -> str:
    s = str(acc).strip()
    if s.endswith(".0"):
        s = s[:-2]
    s = s.lstrip("0") or "0"
    return s

def _norm_acc_series(s: pd.Series) -> pd.Series:
    return s.apply(_norm_acc)

def _sanitize_text_series(s: pd.Series) -> pd.Series:
    # Fjern ikke-trykkbare kontrolltegn som Excel ikke aksepterer i XML
    return s.astype(str).map(lambda x: re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", x))

def _range_dates(header: Optional[pd.DataFrame],
                 date_from: Optional[str],
                 date_to: Optional[str],
                 tx: Optional[pd.DataFrame]) -> Tuple[pd.Timestamp, pd.Timestamp]:
    dfrom = pd.to_datetime(date_from) if date_from else None
    dto = pd.to_datetime(date_to) if date_to else None
    if header is not None and not header.empty:
        row = header.iloc[0]
        if dfrom is None:
            dfrom = pd.to_datetime(row.get("SelectionStart") or row.get("SelectionStartDate") or row.get("StartDate"),
                                   errors="coerce")
        if dto is None:
            dto = pd.to_datetime(row.get("SelectionEnd") or row.get("SelectionEndDate") or row.get("EndDate"),
                                 errors="coerce")
    if ((dfrom is None or pd.isna(dfrom)) or (dto is None or pd.isna(dto))) and tx is not None and not tx.empty:
        if "Date" in tx.columns:
            years = tx["Date"].dropna().dt.year
            if not years.empty:
                year = int(years.value_counts().idxmax())
                if dfrom is None or pd.isna(dfrom):
                    dfrom = pd.Timestamp(year=year, month=1, day=1)
                if dto is None or pd.isna(dto):
                    dto = pd.Timestamp(year=year, month=12, day=31)
    if dfrom is None or pd.isna(dfrom): dfrom = pd.Timestamp.min
    if dto is None or pd.isna(dto):     dto = pd.Timestamp.max
    return dfrom.normalize(), dto.normalize()

# -------------------- Avledning av part‑ID --------------------
def _norm_txt(s: str) -> str:
    s = (s or "").lower().strip()
    s = s.replace("æ", "ae").replace("ø", "o").replace("å", "a")
    return re.sub(r"[^a-z0-9]+", "", s)

_AR_TOKENS = {"customer", "kunde", "debtor", "kund", "ar", "reskontrokunde", "accountsreceivable", "debitor"}
_AP_TOKENS = {"supplier", "leverandor", "leverandør", "vendor", "creditor", "kreditor", "ap", "reskontroleverandor", "accountspayable"}

def _looks_like_ar(val: str) -> bool:
    t = _norm_txt(val);  return any(tok in t for tok in _AR_TOKENS)

def _looks_like_ap(val: str) -> bool:
    t = _norm_txt(val);  return any(tok in t for tok in _AP_TOKENS)

def _find_type_id_pairs(cols: List[str]) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    lower = {c: c.lower() for c in cols}
    fixed = [("PartyType", "PartyID"),
             ("SubLedgerType", "SubLedgerID"),
             ("ReskontroType", "ReskontroID"),
             ("ReskontroType", "ReskontroKode")]
    for t, i in fixed:
        if t in cols and i in cols:
            pairs.append((t, i))

    def suf(c: str) -> str:
        m = re.search(r"(\d+)$", c)
        return m.group(1) if m else ""

    type_like = []
    for c in cols:
        cn = lower[c]
        if "type" in cn and any(x in cn for x in ["party", "subledger", "reskontro", "analysis", "dimension", "dim", "object", "attrib"]):
            type_like.append(c)

    id_like = [c for c in cols if any(x in lower[c] for x in ["id", "code", "kode", "nr", "no"])]

    for tcol in type_like:
        sfx = suf(tcol)
        same = [i for i in id_like if suf(i) == sfx] or id_like
        same = [i for i in same if i != tcol]
        for icol in same:
            pairs.append((tcol, icol))

    seen = set(); uniq: List[Tuple[str, str]] = []
    for p in pairs:
        if p not in seen:
            uniq.append(p); seen.add(p)
    return uniq

def _derive_party_id_generic(tx: pd.DataFrame, which: str) -> Optional[pd.Series]:
    which = which.upper()
    check = _looks_like_ar if which == "AR" else _looks_like_ap
    pairs = _find_type_id_pairs(list(tx.columns))
    if not pairs:
        return None

    def _clean(s: pd.Series) -> pd.Series:
        v = s.astype(str).str.strip()
        return v.mask(v.str.lower().isin(["", "nan", "none", "nat"]))

    out = pd.Series([None] * len(tx), index=tx.index, dtype=object)
    hit_any = False
    for tcol, icol in pairs:
        if tcol not in tx.columns or icol not in tx.columns:
            continue
        tvals = tx[tcol].astype(str).fillna("")
        mask = tvals.apply(check)
        if not mask.any():
            continue
        cand = _clean(tx[icol])
        if cand is None:
            continue
        newvals = cand.where(mask)
        if newvals.notna().any():
            out = out.where(out.notna(), newvals)
            hit_any = True
    return out if hit_any and out.notna().any() else None

# -------------------- Excel-format --------------------
def _format_sheet_xlsxwriter(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame, freeze_cols: int = 0) -> None:
    try:
        ws = writer.sheets[sheet_name]
        book = writer.book
        fmt_num = book.add_format({"num_format": "#,##0.00;[Red]-#,##0.00"})
        fmt_txt = book.add_format({})
        for idx, col in enumerate(df.columns):
            if pd.api.types.is_numeric_dtype(df[col]):
                ws.set_column(idx, idx, 14, fmt_num)
            else:
                try:
                    w = max(10, min(42, int(df[col].astype(str).map(len).quantile(0.90)) + 2))
                except Exception:
                    w = 18
                ws.set_column(idx, idx, w, fmt_txt)
        if not EXCEL_SAFE_MODE and not df.empty:
            ws.freeze_panes(1, freeze_cols)
            ws.autofilter(0, 0, max(1, len(df)), max(0, len(df.columns) - 1))
    except Exception:
        pass

# -------------------- Hovedfunksjon --------------------
def make_control_report(outdir: Path,
                        create_drilldown: bool = True,
                        max_drill_rows: int = 250_000) -> Path:
    """
    Oversikt over GL-konti som inneholder reskontrotransaksjoner.
    Faner:
      - AR_Overview:  AccountID, AccountDescription, IB, Movement, UB (kunde-linjer)
      - AP_Overview:  tilsvarende for leverandør
      - AR_Drilldown / AP_Drilldown (valgfritt): detaljerte linjer (trimmes til max_drill_rows)

    NB: Hvis ar_subledger.xlsx og/eller ap_subledger.xlsx finnes i outdir,
        deaktiveres drilldown automatisk for å unngå duplisering mot subledger-filer.
    """
    outdir = Path(outdir)
    tx_path = _find_csv_file(outdir, "transactions.csv")
    hdr_path = _find_csv_file(outdir, "header.csv")
    acc_path = _find_csv_file(outdir, "accounts.csv")

    tx = _read_csv_safe(tx_path, dtype=str) if tx_path else None
    if tx is None or tx.empty:
        raise FileNotFoundError("transactions.csv mangler eller er tom")

    header = _read_csv_safe(hdr_path, dtype=str) if hdr_path else None
    acc = _read_csv_safe(acc_path, dtype=str) if acc_path else None

    # Normaliser
    tx = _parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = tx["PostingDate"].fillna(tx["TransactionDate"])
    for c in ["AccountID", "CustomerID", "SupplierID", "CustomerName", "SupplierName",
              "VoucherID", "VoucherNo", "Text", "Description"]:
        if c in tx.columns:
            tx[c] = _sanitize_text_series(tx[c].astype(str))
    if "AccountID" in tx.columns:
        tx["AccountID"] = _norm_acc_series(tx["AccountID"])
    tx = _to_num(tx, ["Debit", "Credit", "TaxAmount"])
    tx["Amount"] = tx["Debit"] - tx["Credit"]

    # Foretrekk GL-linjer
    if "IsGL" in tx.columns:
        tx = tx.loc[tx["IsGL"].astype(str).str.lower() == "true"].copy()

    # Avled part-ID om nødvendig
    if "CustomerID" not in tx.columns:
        tx["CustomerID"] = ""
    if "SupplierID" not in tx.columns:
        tx["SupplierID"] = ""
    miss_ar = ~_has_value(tx["CustomerID"])
    miss_ap = ~_has_value(tx["SupplierID"])
    if miss_ar.any():
        der = _derive_party_id_generic(tx, "AR")
        if der is not None:
            tx.loc[miss_ar, "CustomerID"] = tx.loc[miss_ar, "CustomerID"].where(_has_value(tx.loc[miss_ar, "CustomerID"]), der.loc[miss_ar])
    if miss_ap.any():
        der = _derive_party_id_generic(tx, "AP")
        if der is not None:
            tx.loc[miss_ap, "SupplierID"] = tx.loc[miss_ap, "SupplierID"].where(_has_value(tx.loc[miss_ap, "SupplierID"]), der.loc[miss_ap])

    dfrom, dto = _range_dates(header, None, None, tx)

    # Kontonavn
    acc_desc: Optional[pd.DataFrame] = None
    if acc is not None and not acc.empty and "AccountID" in acc.columns:
        acc = acc.copy()
        acc["AccountID"] = _norm_acc_series(acc["AccountID"].astype(str))
        if "AccountDescription" in acc.columns:
            acc_desc = acc[["AccountID", "AccountDescription"]].drop_duplicates()

    # Oversikter
    def _overview(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
        dfp = df.loc[_has_value(df[id_col])].copy()
        if dfp.empty:
            return pd.DataFrame(columns=["AccountID", "AccountDescription", "IB", "Movement", "UB"])
        ib = dfp.loc[dfp["Date"] < dfrom].groupby("AccountID")["Amount"].sum()
        pr = dfp.loc[(dfp["Date"] >= dfrom) & (dfp["Date"] <= dto)].groupby("AccountID")["Amount"].sum()
        ub = dfp.loc[dfp["Date"] <= dto].groupby("AccountID")["Amount"].sum()
        out = pd.concat([ib.rename("IB"), pr.rename("Movement"), ub.rename("UB")], axis=1).fillna(0.0).reset_index()
        if acc_desc is not None:
            out = out.merge(acc_desc, on="AccountID", how="left")
        cols = ["AccountID", "AccountDescription", "IB", "Movement", "UB"]
        for c in cols:
            if c not in out.columns:
                out[c] = "" if c in ("AccountID", "AccountDescription") else 0.0
        out = out[cols].sort_values("AccountID")
        return out

    ar_over = _overview(tx, "CustomerID")
    ap_over = _overview(tx, "SupplierID")

    # Drilldown (kan bli deaktivert)
    def _drill(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
        dfp = df.loc[_has_value(df[id_col])].copy()
        if dfp.empty:
            return pd.DataFrame()
        keep = ["Date", "VoucherID", "VoucherNo", "AccountID", "Debit", "Credit", "Amount",
                id_col]
        if id_col == "CustomerID" and "CustomerName" in dfp.columns:
            keep.append("CustomerName")
        if id_col == "SupplierID" and "SupplierName" in dfp.columns:
            keep.append("SupplierName")
        if "Text" in dfp.columns:
            keep.append("Text")
        if "Description" in dfp.columns and "Text" not in keep:
            keep.append("Description")
        for c in keep:
            if c not in dfp.columns:
                dfp[c] = ""
        dfp = dfp[keep].sort_values(["AccountID", "Date", "VoucherID", "VoucherNo"])
        if len(dfp) > max_drill_rows:
            dfp = dfp.iloc[:max_drill_rows].copy()
        return dfp

    # ---- Unngå dobbelt drilldown hvis subledger finnes ----
    drill_requested = bool(create_drilldown)
    subledger_exists = (outdir / "ar_subledger.xlsx").exists() or (outdir / "ap_subledger.xlsx").exists()
    effective_drill = drill_requested and (not subledger_exists)
    if drill_requested and subledger_exists:
        print("[excel] Control Report: deaktiverer drilldown (subledger-filer finnes – unngår duplisering).")

    ar_drill = _drill(tx, "CustomerID") if effective_drill else pd.DataFrame()
    ap_drill = _drill(tx, "SupplierID") if effective_drill else pd.DataFrame()

    # Excel-ut
    out_path = Path(outdir) / "control_report.xlsx"
    with pd.ExcelWriter(
        out_path,
        engine="xlsxwriter",
        datetime_format="yyyy-mm-dd",
        engine_kwargs={"options": {"strings_to_urls": False, "nan_inf_to_errors": True}},
    ) as xw:
        ar_over.to_excel(xw, index=False, sheet_name="AR_Overview")
        _format_sheet_xlsxwriter(xw, "AR_Overview", ar_over, freeze_cols=1)

        ap_over.to_excel(xw, index=False, sheet_name="AP_Overview")
        _format_sheet_xlsxwriter(xw, "AP_Overview", ap_over, freeze_cols=1)

        if effective_drill and not ar_drill.empty:
            ar_drill.to_excel(xw, index=False, sheet_name="AR_Drilldown")
            _format_sheet_xlsxwriter(xw, "AR_Drilldown", ar_drill, freeze_cols=2)
        if effective_drill and not ap_drill.empty:
            ap_drill.to_excel(xw, index=False, sheet_name="AP_Drilldown")
            _format_sheet_xlsxwriter(xw, "AP_Drilldown", ap_drill, freeze_cols=2)

    print(f"[excel] Control Report: {out_path.name} – ark: 'AR_Overview', 'AP_Overview'"
          f"{', AR_Drilldown' if effective_drill and not ar_drill.empty else ''}"
          f"{', AP_Drilldown' if effective_drill and not ap_drill.empty else ''}")
    return out_path
