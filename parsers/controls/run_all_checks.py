# -*- coding: utf-8 -*-
"""
controls/run_all_checks.py – samler kontroller og lager control_report.xlsx i CSV-mappen.

• Førsteside **Oversikt** (trafikklys) + forklaringer/tiltak.
• AR/AP-avstemming: Subledger-UB vs UB fra TrialBalance (GL) og vs Accounts (closing).
• MVA: både «All 27xx» (grov) og «Tax-only» (ekskl. oppgjør/interim).
• Hver fane får norsk formattering via report_fmt.beautify_sheet().
"""
from __future__ import annotations
from pathlib import Path
from typing import Optional, Iterable, Dict, List, Set, Tuple
import pandas as pd
import numpy as np

# --------- formattering (best effort) ---------
try:
    from .report_fmt import beautify_sheet  # type: ignore
except Exception:
    try:
        from app.parsers.controls.report_fmt import beautify_sheet  # type: ignore
    except Exception:
        def beautify_sheet(*args, **kwargs):  # no-op hvis ikke tilgjengelig
            return

# ---------------------- konstanter / terskler ----------------------
CENT_TOL = 0.01
NOK_TOL  = 1.00
VAT_PREFIXES = ("27",)

# ---------------------- små hjelpere ----------------------
def _read_csv(p: Path, dtype=str) -> Optional[pd.DataFrame]:
    if not p or not p.exists():
        return None
    for sep in (",", ";", "\t"):
        try:
            return pd.read_csv(p, dtype=dtype, keep_default_na=False, sep=sep)
        except Exception:
            continue
    try:
        return pd.read_csv(p, dtype=dtype, keep_default_na=False)
    except Exception:
        return None

def _to_num(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            s = (df[c].astype(str)
                         .str.replace("\u00A0", "", regex=False)
                         .str.replace(" ", "", regex=False)
                         .str.replace(",", ".", regex=False))
            df[c] = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return df

def _parse_dates(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def _norm_acc(s: str) -> str:
    t = str(s or "").strip()
    if t.endswith(".0"): t = t[:-2]
    t = t.lstrip("0") or "0"
    return t

def _norm_acc_series(s: pd.Series) -> pd.Series:
    return s.astype(str).map(_norm_acc)

def _period_ym(d: pd.Series) -> pd.Series:
    return pd.to_datetime(d, errors="coerce").dt.to_period("M").astype(str)

def _year_term(d: pd.Series) -> pd.Series:
    d = pd.to_datetime(d, errors="coerce")
    def lab(x):
        if pd.isna(x): return ""
        t = (x.month + 1) // 2  # 1..6
        return f"{x.year}-T{t}"
    return d.apply(lab)

def _find(outdir: Path, name: str) -> Optional[Path]:
    for p in [outdir / name, outdir / "csv" / name]:
        if p.exists():
            return p
    for base in [outdir, outdir.parent, Path.cwd()]:
        try:
            for fp in base.rglob(name):
                return fp
        except Exception:
            pass
    return None

def _status(ok: bool=None, warn: bool=False) -> str:
    if ok is True:  return "OK"
    if ok is False: return "FEIL"
    return "OBS" if warn else "OK"

# ---------------------- kontrollkonti AR/AP ----------------------
def _pick_ar_ap_controls(outdir: Path) -> Tuple[Set[str], Set[str]]:
    cfg = _read_csv(outdir / "arap_control_accounts.csv", dtype=str)
    if cfg is not None and {"PartyType", "AccountID"}.issubset(cfg.columns):
        cfg["AccountID"] = _norm_acc_series(cfg["AccountID"])
        ar = set(cfg.loc[cfg["PartyType"].str.lower() == "customer", "AccountID"])
        ap = set(cfg.loc[cfg["PartyType"].str.lower() == "supplier", "AccountID"])
        if ar or ap:
            return (ar or {"1510", "1550"}, ap or {"2410", "2460"})
    return {"1510", "1550"}, {"2410", "2460"}

# ---------------------- GL-kontroller ----------------------
def _global_and_voucher(tx: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    t = tx.copy()
    _to_num(t, ["Debit", "Credit"])
    tot = pd.DataFrame([{
        "Delta": float(t["Debit"].sum() - t["Credit"].sum()),
        "OK": abs(float(t["Debit"].sum() - t["Credit"].sum())) <= CENT_TOL
    }])
    if "VoucherID" in t.columns:
        g = t.groupby("VoucherID")[["Debit", "Credit"]].sum().reset_index()
        g["Delta"] = (g["Debit"] - g["Credit"]).round(2)
        g["OK"] = g["Delta"].abs() <= CENT_TOL
        unb = g.loc[~g["OK"]].copy()
    else:
        unb = pd.DataFrame(columns=["VoucherID", "Debit", "Credit", "Delta", "OK"])
    return tot, unb

def _tb_vs_accounts(tx: pd.DataFrame, acc: Optional[pd.DataFrame]) -> pd.DataFrame:
    t = tx.copy()
    t["AccountID"] = _norm_acc_series(t.get("AccountID", pd.Series([], dtype=str)))
    _to_num(t, ["Debit", "Credit"])
    tb = t.groupby("AccountID")[["Debit", "Credit"]].sum().reset_index()
    tb["GL_UB"] = tb["Debit"] - tb["Credit"]
    if acc is None or not {"ClosingDebit", "ClosingCredit"}.issubset(acc.columns):
        return tb[["AccountID", "GL_UB"]].sort_values("AccountID")
    a = acc.copy()
    a["AccountID"] = _norm_acc_series(a["AccountID"])
    _to_num(a, ["ClosingDebit", "ClosingCredit"])
    a["Acc_UB"] = a["ClosingDebit"] - a["ClosingCredit"]
    out = tb.merge(a[["AccountID", "AccountDescription", "Acc_UB"]], on="AccountID", how="left")
    out["Diff_UB"] = (out["GL_UB"] - out["Acc_UB"]).round(2)
    out["OK"] = out["Diff_UB"].abs() <= NOK_TOL
    return out.sort_values("AccountID")

def _period_completeness(tx: pd.DataFrame, dfrom, dto) -> pd.DataFrame:
    t = tx.copy()
    d = pd.to_datetime(t.get("PostingDate")).fillna(pd.to_datetime(t.get("TransactionDate")))
    t["YM"] = _period_ym(d)
    months = pd.period_range(dfrom, dto, freq="M").astype(str).tolist()
    have = set(t["YM"].dropna().unique())
    return pd.DataFrame([{"PeriodYM": m, "HasTx": m in have, "Missing": m not in have} for m in months])

def _dup_candidates(tx: pd.DataFrame) -> pd.DataFrame:
    t = tx.copy()
    _to_num(t, ["Debit", "Credit"])
    if {"VoucherNo", "JournalID", "PostingDate"}.issubset(t.columns):
        grp = (t.groupby(["VoucherNo", "JournalID", "PostingDate"])[["Debit", "Credit"]]
                 .sum().reset_index())
        grp["Net"] = (grp["Debit"] - grp["Credit"]).round(2)
        cnt = t.groupby(["VoucherNo", "JournalID", "PostingDate"]).size().reset_index(name="Lines")
        return (grp.merge(cnt, on=["VoucherNo", "JournalID", "PostingDate"], how="left")
                    .query("Lines>1")
                    .sort_values(["PostingDate", "JournalID", "VoucherNo"]))
    return pd.DataFrame(columns=["VoucherNo", "JournalID", "PostingDate", "Debit", "Credit", "Net", "Lines"])

# ---------------------- AR/AP ----------------------
def _load_tb_ub_and_accounts_ub(outdir: Path, ctrl: Set[str]) -> Tuple[Optional[float], Optional[float]]:
    ub_gl = None
    ub_acc = None

    tbp = outdir / "trial_balance.xlsx"
    if tbp.exists():
        try:
            tb = pd.read_excel(tbp, sheet_name="TrialBalance")
            if {"AccountID", "UB"}.issubset(tb.columns):
                tb["AccountID"] = _norm_acc_series(tb["AccountID"])
                _to_num(tb, ["UB"])
                mask = tb["AccountID"].isin(ctrl)
                if mask.any():
                    ub_gl = float(tb.loc[mask, "UB"].sum())
        except Exception:
            pass

    acc = _read_csv(outdir / "accounts.csv", dtype=str)
    if acc is not None and {"AccountID", "ClosingDebit", "ClosingCredit"}.issubset(acc.columns):
        acc["AccountID"] = _norm_acc_series(acc["AccountID"])
        _to_num(acc, ["ClosingDebit", "ClosingCredit"])
        mask = acc["AccountID"].isin(ctrl)
        if mask.any():
            ub_acc = float((acc.loc[mask, "ClosingDebit"] - acc.loc[mask, "ClosingCredit"]).sum())

    return ub_gl, ub_acc

def _read_subledger_ub(excel_path: Path, sheet: str) -> Optional[float]:
    try:
        if excel_path.exists():
            return float(pd.read_excel(excel_path, sheet_name=sheet)["UB_Amount"].sum())
    except Exception:
        pass
    return None

def _ar_ap_recon(outdir: Path, ar_ctrl: Set[str], ap_ctrl: Set[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    ar_sub = _read_subledger_ub(outdir / "ar_subledger.xlsx", "AR_Balances")
    ap_sub = _read_subledger_ub(outdir / "ap_subledger.xlsx", "AP_Balances")
    ar_gl, ar_acc = _load_tb_ub_and_accounts_ub(outdir, ar_ctrl)
    ap_gl, ap_acc = _load_tb_ub_and_accounts_ub(outdir, ap_ctrl)

    def _mk_row(type_, ctrl, ub_gl, ub_acc, sub):
        row = {
            "Type": type_, "Kontrollkonti": ", ".join(sorted(ctrl)) if ctrl else "",
            "UB_GL": ub_gl, "UB_Accounts": ub_acc, "Subledger_UB": sub,
            "Avvik_GL_mot_Sub": None, "Avvik_Acc_mot_Sub": None, "Avvik_GL_mot_Acc": None,
        }
        if (ub_gl is not None) and (sub is not None):
            row["Avvik_GL_mot_Sub"] = round(ub_gl - sub, 2)
        if (ub_acc is not None) and (sub is not None):
            row["Avvik_Acc_mot_Sub"] = round(ub_acc - sub, 2)
        if (ub_gl is not None) and (ub_acc is not None):
            row["Avvik_GL_mot_Acc"] = round(ub_gl - ub_acc, 2)
        return row

    ar = pd.DataFrame([_mk_row("AR", ar_ctrl, ar_gl, ar_acc, ar_sub)])
    ap = pd.DataFrame([_mk_row("AP", ap_ctrl, ap_gl, ap_acc, ap_sub)])
    return ar, ap

# ---------------------- MVA ----------------------
def _load_vat_gl_config(outdir: Path, accounts: Optional[pd.DataFrame]) -> Tuple[Set[str], Set[str], pd.DataFrame]:
    all27: Set[str] = set()
    tax_only: Set[str] = set()
    config_rows: List[Dict[str, str]] = []

    if accounts is not None and "AccountID" in accounts.columns:
        a = accounts.copy()
        a["AccountID"] = _norm_acc_series(a["AccountID"])
        if "AccountDescription" not in a.columns:
            a["AccountDescription"] = ""
        all27 |= set(a.loc[a["AccountID"].str.startswith(VAT_PREFIXES), "AccountID"])

        desc = a["AccountDescription"].str.lower()
        settlement_mask = (
            desc.str.contains("oppgj", na=False) |
            desc.str.contains("oppgjor", na=False) |
            desc.str.contains("oppgjør", na=False) |
            desc.str.contains("interim", na=False)
        )
        tax_only |= set(a.loc[a["AccountID"].isin(all27) & ~settlement_mask, "AccountID"])

        for _, r in a.loc[a["AccountID"].isin(all27), ["AccountID", "AccountDescription"]].iterrows():
            cat = "tax" if r["AccountID"] in tax_only else "settlement/other"
            config_rows.append({"AccountID": r["AccountID"], "AccountDescription": r["AccountDescription"], "Category": cat})

    p = outdir / "vat_gl_accounts.csv"
    if p.exists():
        cfg = _read_csv(p, dtype=str)
        if cfg is not None and "AccountID" in cfg.columns:
            cfg["AccountID"] = _norm_acc_series(cfg["AccountID"])
            catcol = None
            for c in cfg.columns:
                if c.strip().lower() in {"category", "role", "type"}:
                    catcol = c; break
            if catcol:
                for _, r in cfg.iterrows():
                    acc = _norm_acc(r["AccountID"])
                    cat = str(r[catcol]).strip().lower()
                    all27.add(acc)
                    if cat in {"tax", "mva", "calc"}:
                        tax_only.add(acc)
                    elif cat in {"settlement", "oppgjør", "oppgjor", "interim"}:
                        tax_only.discard(acc)
                    elif cat in {"exclude"}:
                        all27.discard(acc)
                        tax_only.discard(acc)
                    config_rows.append({"AccountID": acc, "AccountDescription": "", "Category": cat})
            else:
                for acc in cfg["AccountID"].tolist():
                    acc = _norm_acc(acc)
                    all27.add(acc); tax_only.add(acc)
                    config_rows.append({"AccountID": acc, "AccountDescription": "", "Category": "tax"})

    cfg_view = pd.DataFrame(config_rows).drop_duplicates().sort_values("AccountID") if config_rows else \
               pd.DataFrame([{"Info": "Ingen vat_gl_accounts.csv – brukte heuristikk (all27xx, ekskl. oppgjør/interim)."}])
    return all27, tax_only, cfg_view

def _vat_views(tx: pd.DataFrame, tax: Optional[pd.DataFrame], acc: Optional[pd.DataFrame], outdir: Path) -> Dict[str, pd.DataFrame]:
    t = tx.copy()
    _to_num(t, ["Debit", "Credit", "DebitTaxAmount", "CreditTaxAmount", "TaxAmount", "TaxPercentage"])
    t["Date"] = pd.to_datetime(t.get("PostingDate")).fillna(pd.to_datetime(t.get("TransactionDate")))
    t["VAT"]  = t.get("DebitTaxAmount", 0.0) - t.get("CreditTaxAmount", 0.0)
    if ("DebitTaxAmount" not in t.columns) and ("CreditTaxAmount" not in t.columns):
        t["VAT"] = t.get("TaxAmount", 0.0)
    if "TaxType" in t.columns:
        t = t.loc[t["TaxType"].str.upper() == "MVA"].copy()
    else:
        t = t.loc[t["VAT"].abs() > 0].copy()
    if tax is not None and {"TaxCode","StandardTaxCode"}.issubset(tax.columns) and "TaxCode" in t.columns:
        t = t.merge(tax[["TaxCode","StandardTaxCode"]].drop_duplicates(), on="TaxCode", how="left")

    t["Month"] = _period_ym(t["Date"])
    t["Term"]  = _year_term(t["Date"])
    by_code_m = (t.groupby(["Month", "TaxCode", "StandardTaxCode"])["VAT"].sum()
                   .reset_index().sort_values(["Month", "TaxCode"]))
    by_code_t = (t.groupby(["Term", "TaxCode", "StandardTaxCode"])["VAT"].sum()
                   .reset_index().sort_values(["Term", "TaxCode"]))

    all27_ids, taxonly_ids, cfg_view = _load_vat_gl_config(outdir, acc)

    g = tx.copy()
    _to_num(g, ["Debit", "Credit"])
    g["AccountID"] = _norm_acc_series(g.get("AccountID", pd.Series([], dtype=str)))
    g["Date"] = pd.to_datetime(g.get("PostingDate")).fillna(pd.to_datetime(g.get("TransactionDate")))
    g["Month"] = _period_ym(g["Date"])
    g["Term"]  = _year_term(g["Date"])
    g["GL_Amount"] = g["Debit"] - g["Credit"]

    gl_all_m = (g.loc[g["AccountID"].isin(all27_ids)]
                  .groupby(["Month"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount": "GL_All27xx"}))
    gl_all_t = (g.loc[g["AccountID"].isin(all27_ids)]
                  .groupby(["Term"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount": "GL_All27xx"}))
    gl_tax_m = (g.loc[g["AccountID"].isin(taxonly_ids)]
                  .groupby(["Month"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount": "GL_TaxOnly"}))
    gl_tax_t = (g.loc[g["AccountID"].isin(taxonly_ids)]
                  .groupby(["Term"])["GL_Amount"].sum().reset_index()
                  .rename(columns={"GL_Amount": "GL_TaxOnly"}))

    mvat = by_code_m.groupby("Month")["VAT"].sum().reset_index().rename(columns={"VAT": "VAT_TaxLines"})
    tvat = by_code_t.groupby("Term")["VAT"].sum().reset_index().rename(columns={"VAT": "VAT_TaxLines"})

    chk_m = mvat.merge(gl_all_m, on="Month", how="outer").fillna(0.0)
    chk_m["Diff"] = (chk_m["VAT_TaxLines"] - chk_m["GL_All27xx"]).round(2)
    chk_m["OK"]   = chk_m["Diff"].abs() <= NOK_TOL

    chk_t = tvat.merge(gl_all_t, on="Term", how="outer").fillna(0.0)
    chk_t["Diff"] = (chk_t["VAT_TaxLines"] - chk_t["GL_All27xx"]).round(2)
    chk_t["OK"]   = chk_t["Diff"].abs() <= NOK_TOL

    recon_m = (mvat.merge(gl_all_m, on="Month", how="outer")
                    .merge(gl_tax_m, on="Month", how="outer")).fillna(0.0)
    recon_m["Diff_All"]     = (recon_m["VAT_TaxLines"] - recon_m["GL_All27xx"]).round(2)
    recon_m["OK_All"]       = recon_m["Diff_All"].abs() <= NOK_TOL
    recon_m["Diff_TaxOnly"] = (recon_m["VAT_TaxLines"] - recon_m["GL_TaxOnly"]).round(2)
    recon_m["OK_TaxOnly"]   = recon_m["Diff_TaxOnly"].abs() <= NOK_TOL

    recon_t = (tvat.merge(gl_all_t, on="Term", how="outer")
                    .merge(gl_tax_t, on="Term", how="outer")).fillna(0.0)
    recon_t["Diff_All"]     = (recon_t["VAT_TaxLines"] - recon_t["GL_All27xx"]).round(2)
    recon_t["OK_All"]       = recon_t["Diff_All"].abs() <= NOK_TOL
    recon_t["Diff_TaxOnly"] = (recon_t["VAT_TaxLines"] - recon_t["GL_TaxOnly"]).round(2)
    recon_t["OK_TaxOnly"]   = recon_t["Diff_TaxOnly"].abs() <= NOK_TOL

    return {
        "VAT_ByCode_Month":    by_code_m,
        "VAT_ByCode_Term":     by_code_t,
        "VAT_GL_Check_Month":  chk_m,
        "VAT_GL_Check_Term":   chk_t,
        "VAT_Recon_Month":     recon_m,
        "VAT_Recon_Term":      recon_t,
        "VAT_GL_Config":       cfg_view,
    }

# ---------------------- hovedkjøringen ----------------------
def run_all_checks(outdir: Path,
                   asof: Optional[str] = None,
                   date_from: Optional[str] = None,
                   date_to: Optional[str] = None) -> Path:
    outdir = Path(outdir)

    tx = _read_csv(_find(outdir, "transactions.csv") or outdir / "transactions.csv")
    if tx is None or tx.empty:
        raise FileNotFoundError("transactions.csv mangler/tom")
    acc = _read_csv(_find(outdir, "accounts.csv") or outdir / "accounts.csv")
    tax = _read_csv(_find(outdir, "tax_table.csv") or outdir / "tax_table.csv")
    unk = _read_csv(_find(outdir, "unknown_nodes.csv") or outdir / "unknown_nodes.csv")
    hdr = _read_csv(_find(outdir, "header.csv") or outdir / "header.csv")

    tx = _parse_dates(tx, ["TransactionDate", "PostingDate"])
    tx["Date"] = pd.to_datetime(tx.get("PostingDate")).fillna(pd.to_datetime(tx.get("TransactionDate")))
    dfrom = pd.to_datetime(date_from) if date_from else (
        pd.to_datetime(hdr.iloc[0].get("SelectionStartDate")
                       or hdr.iloc[0].get("SelectionStart")
                       or hdr.iloc[0].get("StartDate"), errors="coerce")
        if hdr is not None and not hdr.empty else tx["Date"].min()
    )
    dto   = pd.to_datetime(date_to)   if date_to   else (
        pd.to_datetime(hdr.iloc[0].get("SelectionEndDate")
                       or hdr.iloc[0].get("SelectionEnd")
                       or hdr.iloc[0].get("EndDate"), errors="coerce")
        if hdr is not None and not hdr.empty else tx["Date"].max()
    )

    # kontroller
    global_bal, unbalanced = _global_and_voucher(tx)
    tb_vs_acc              = _tb_vs_accounts(tx, acc)
    pc                     = _period_completeness(tx, dfrom, dto)
    dups                   = _dup_candidates(tx)
    ar_ctrl, ap_ctrl       = _pick_ar_ap_controls(outdir)
    ar_rec, ap_rec         = _ar_ap_recon(outdir, ar_ctrl, ap_ctrl)
    vat                    = _vat_views(tx, tax, acc, outdir)
    unk_view               = (unk.head(200) if (unk is not None and not unk.empty)
                              else pd.DataFrame([{"Info": "No unknown_nodes.csv"}]))

    # -------- Oversikt (trafikklys) --------
    delta_global = float(global_bal.iloc[0]["Delta"])
    st_global = _status(ok=abs(delta_global) <= CENT_TOL)

    unb_count = 0 if unbalanced.empty else int(len(unbalanced))
    st_unb = _status(ok=(unb_count == 0))

    miss_months = int(pc["Missing"].sum())
    st_missing = _status(ok=(miss_months == 0), warn=(miss_months > 0))

    dup_count = 0 if dups.empty else int(len(dups))
    st_dups = _status(ok=(dup_count == 0), warn=(dup_count > 0))

    tb_issues = int((~tb_vs_acc.get("OK", pd.Series([], dtype=bool))).sum()) if "OK" in tb_vs_acc.columns else 0
    st_tbvsacc = _status(ok=(tb_issues == 0), warn=(tb_issues > 0))

    ar_diff_gl = ar_rec.iloc[0]["Avvik_GL_mot_Sub"]
    ar_diff_acc = ar_rec.iloc[0]["Avvik_Acc_mot_Sub"]
    ap_diff_gl = ap_rec.iloc[0]["Avvik_GL_mot_Sub"]
    ap_diff_acc = ap_rec.iloc[0]["Avvik_Acc_mot_Sub"]
    st_ar = _status(ok=(None not in (ar_diff_gl, ar_diff_acc) and
                        abs(ar_diff_gl) <= NOK_TOL and abs(ar_diff_acc) <= NOK_TOL))
    st_ap = _status(ok=(None not in (ap_diff_gl, ap_diff_acc) and
                        abs(ap_diff_gl) <= NOK_TOL and abs(ap_diff_acc) <= NOK_TOL))

    vat_ok_m = int((vat["VAT_Recon_Month"]["OK_TaxOnly"]).sum())
    vat_rows_m = int(len(vat["VAT_Recon_Month"]))
    vat_ok_t = int((vat["VAT_Recon_Term"]["OK_TaxOnly"]).sum())
    vat_rows_t = int(len(vat["VAT_Recon_Term"]))
    st_vat = _status(ok=((vat_ok_m == vat_rows_m) and (vat_ok_t == vat_rows_t)),
                     warn=((vat_ok_m < vat_rows_m) or (vat_ok_t < vat_rows_t)))

    oversikt = pd.DataFrame([
        {"Kontroll": "Global balanse (debet = kredit)", "Status": st_global,
         "Nøkkeltall": f"Δ={delta_global:.2f}",
         "Hva betyr dette?": "Hele materialet balanserer. ≠0 indikerer datafeil eller manglende linjer.",
         "Tiltak": "Ved ≠0: se 'GlobalBalance' og 'UnbalancedVouchers'."},

        {"Kontroll": "Ubalanserte bilag", "Status": st_unb,
         "Nøkkeltall": f"Antall={unb_count}",
         "Hva betyr dette?": "Bilag som ikke går i 0 på linjenivå.",
         "Tiltak": "Se arket 'UnbalancedVouchers'."},

        {"Kontroll": "Periodekompletthet", "Status": st_missing,
         "Nøkkeltall": f"Mangler mnd={miss_months}",
         "Hva betyr dette?": "Måneder i utvalget uten posteringer.",
         "Tiltak": "Verifiser periodeutvalg i SAF‑T."},

        {"Kontroll": "Duplikatkandidater", "Status": st_dups,
         "Nøkkeltall": f"Antall={dup_count}",
         "Hva betyr dette?": "Potensielle duplikater (samme voucher/journal/dato).",
         "Tiltak": "Se 'DuplicateCandidates'."},

        {"Kontroll": "TB (GL) vs Accounts (closing)", "Status": st_tbvsacc,
         "Nøkkeltall": f"Konti m/avvik={tb_issues}",
         "Hva betyr dette?": "Avvik mellom GL-summer og oppgitt UB i accounts.csv.",
         "Tiltak": "Se 'TB_vs_Accounts'. (Kontroll – påvirker ikke TB-arket.)"},

        {"Kontroll": "AR-avstemming (UB mot UB)", "Status": st_ar,
         "Nøkkeltall": f"GL-Sub={ar_diff_gl}, Acc-Sub={ar_diff_acc}",
         "Hva betyr dette?": "Sum reskontro skal treffe UB på kontrollkonti.",
         "Tiltak": "Se 'AR_Recon'."},

        {"Kontroll": "AP-avstemming (UB mot UB)", "Status": st_ap,
         "Nøkkeltall": f"GL-Sub={ap_diff_gl}, Acc-Sub={ap_diff_acc}",
         "Hva betyr dette?": "Sum reskontro skal treffe UB på kontrollkonti.",
         "Tiltak": "Se 'AP_Recon'."},

        {"Kontroll": "MVA-avstemming (Tax-only)", "Status": st_vat,
         "Nøkkeltall": f"Mnd OK={vat_ok_m}/{vat_rows_m}, Term OK={vat_ok_t}/{vat_rows_t}",
         "Hva betyr dette?": "Tax-only ekskluderer oppgjør/interim – bør stemme mot tax-linjene.",
         "Tiltak": "Se 'VAT_Recon_Month/Term'. Legg ev. 'vat_gl_accounts.csv' for presis kontoliste."},
    ])

    # topp‑10 avvik i TB_vs_Accounts
    top_issues = pd.DataFrame()
    if "Diff_UB" in tb_vs_acc.columns:
        top_issues = (tb_vs_acc.assign(absdiff=tb_vs_acc["Diff_UB"].abs())
                                .sort_values("absdiff", ascending=False)
                                .head(10)
                                .drop(columns=["absdiff"]))

    # ---------- skriv Excel ----------
    out_xlsx = outdir / "control_report.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as xw:
        oversikt.to_excel(xw, index=False, sheet_name="Oversikt")
        ws = xw.sheets["Oversikt"]
        try:
            cols = list(oversikt.columns)
            st_col = cols.index("Status")
            nrows = len(oversikt.index)
            fmt_ok   = xw.book.add_format({"font_color": "black", "bg_color": "#C6EFCE"})
            fmt_obs  = xw.book.add_format({"font_color": "black", "bg_color": "#FFEB9C"})
            fmt_fail = xw.book.add_format({"font_color": "white", "bg_color": "#FF0000"})
            ws.conditional_format(1, st_col, nrows, st_col,
                                  {"type": "cell", "criteria": "==", "value": '"OK"', "format": fmt_ok})
            ws.conditional_format(1, st_col, nrows, st_col,
                                  {"type": "cell", "criteria": "==", "value": '"OBS"', "format": fmt_obs})
            ws.conditional_format(1, st_col, nrows, st_col,
                                  {"type": "cell", "criteria": "==", "value": '"FEIL"', "format": fmt_fail})
        except Exception:
            pass
        beautify_sheet(xw, "Oversikt", oversikt)

        summary = pd.DataFrame([
            {"Check": "Global debet=kredit", "Delta": round(delta_global, 2), "OK": (st_global == "OK")},
            {"Check": "Ubalanserte bilag", "Count": unb_count},
            {"Check": "TB vs Accounts (UB)", "Issues": tb_issues},
            {"Check": "Manglende måneder i periode", "MissingMonths": miss_months},
            {"Check": "Duplikatbilag (kandidater)", "Count": dup_count},
            {"Check": "AR Avvik GL-Sub", "Value": ar_diff_gl},
            {"Check": "AR Avvik Acc-Sub", "Value": ar_diff_acc},
            {"Check": "AP Avvik GL-Sub", "Value": ap_diff_gl},
            {"Check": "AP Avvik Acc-Sub", "Value": ap_diff_acc},
            {"Check": "MVA OK mnd (Tax-only)", "CountOK": vat_ok_m},
            {"Check": "MVA OK term (Tax-only)", "CountOK": vat_ok_t},
        ])
        summary.to_excel(xw, index=False, sheet_name="Summary");             beautify_sheet(xw, "Summary", summary)

        global_bal.to_excel(xw, index=False, sheet_name="GlobalBalance");    beautify_sheet(xw, "GlobalBalance", global_bal)
        unbalanced.to_excel(xw, index=False, sheet_name="UnbalancedVouchers"); beautify_sheet(xw, "UnbalancedVouchers", unbalanced)
        tb_vs_acc.to_excel(xw, index=False, sheet_name="TB_vs_Accounts");    beautify_sheet(xw, "TB_vs_Accounts", tb_vs_acc)
        if not top_issues.empty:
            top_issues.to_excel(xw, index=False, sheet_name="TB_TopAvvik");  beautify_sheet(xw, "TB_TopAvvik", top_issues)
        pc.to_excel(xw, index=False, sheet_name="PeriodCompleteness");       beautify_sheet(xw, "PeriodCompleteness", pc)
        dups.to_excel(xw, index=False, sheet_name="DuplicateCandidates");    beautify_sheet(xw, "DuplicateCandidates", dups)

        ar_rec.to_excel(xw, index=False, sheet_name="AR_Recon");             beautify_sheet(xw, "AR_Recon", ar_rec)
        ap_rec.to_excel(xw, index=False, sheet_name="AP_Recon");             beautify_sheet(xw, "AP_Recon", ap_rec)

        vat["VAT_ByCode_Month"].to_excel(xw, index=False, sheet_name="VAT_ByCode_Month"); beautify_sheet(xw, "VAT_ByCode_Month", vat["VAT_ByCode_Month"])
        vat["VAT_ByCode_Term"].to_excel(xw, index=False, sheet_name="VAT_ByCode_Term");   beautify_sheet(xw, "VAT_ByCode_Term", vat["VAT_ByCode_Term"])
        vat["VAT_GL_Check_Month"].to_excel(xw, index=False, sheet_name="VAT_GL_Check_Month"); beautify_sheet(xw, "VAT_GL_Check_Month", vat["VAT_GL_Check_Month"])
        vat["VAT_GL_Check_Term"].to_excel(xw, index=False, sheet_name="VAT_GL_Check_Term");   beautify_sheet(xw, "VAT_GL_Check_Term", vat["VAT_GL_Check_Term"])
        vat["VAT_Recon_Month"].to_excel(xw, index=False, sheet_name="VAT_Recon_Month");       beautify_sheet(xw, "VAT_Recon_Month", vat["VAT_Recon_Month"])
        vat["VAT_Recon_Term"].to_excel(xw, index=False, sheet_name="VAT_Recon_Term");         beautify_sheet(xw, "VAT_Recon_Term", vat["VAT_Recon_Term"])
        vat["VAT_GL_Config"].to_excel(xw, index=False, sheet_name="VAT_GL_Config");           beautify_sheet(xw, "VAT_GL_Config", vat["VAT_GL_Config"])

        unk_view.to_excel(xw, index=False, sheet_name="UnknownNodes");       beautify_sheet(xw, "UnknownNodes", unk_view)

    return out_xlsx

# CLI (valgfritt)
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Kjør kontroller og lag control_report.xlsx i CSV-mappen")
    ap.add_argument("outdir", help="Mappe som inneholder SAF‑T CSV")
    ap.add_argument("--asof", default=None)
    args = ap.parse_args()
    p = run_all_checks(Path(args.outdir), asof=args.asof)
    print("Skrev:", p)
