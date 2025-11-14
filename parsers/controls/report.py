# -*- coding: utf-8 -*-
"""
controls/report.py – skriving av control_report.xlsx (ark, oversikt, trafikklys).

Bruk:
    from .report import write_report
    path = write_report(outdir, global_bal, unbalanced, tb_vs_acc, pc, dups,
                        ar_rec, ap_rec, vat_dict, unk_view)

Forutsetter at 'vat_dict' inneholder nøkler:
  VAT_ByCode_Month, VAT_ByCode_Term, VAT_GL_Check_Month, VAT_GL_Check_Term,
  VAT_Recon_Month, VAT_Recon_Term, VAT_GL_Config
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
from typing import Dict
from .common import apply_common_format

def build_overview(global_bal: pd.DataFrame,
                   unbalanced: pd.DataFrame,
                   tb_vs_acc: pd.DataFrame,
                   pc: pd.DataFrame,
                   dups: pd.DataFrame,
                   ar_rec: pd.DataFrame,
                   ap_rec: pd.DataFrame,
                   vat: Dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returnerer (Oversikt, Summary) basert på datarammene."""
    # nøkkeltall
    delta_global = float(global_bal.iloc[0]["Delta"])
    unb_count    = 0 if unbalanced.empty else int(len(unbalanced))
    miss_months  = int(pc["Missing"].sum()) if "Missing" in pc.columns else 0
    dup_count    = 0 if dups.empty else int(len(dups))
    tb_issues    = int((~tb_vs_acc.get("OK", pd.Series([], dtype=bool))).sum()) if "OK" in tb_vs_acc.columns else 0

    # AR/AP
    ar_diff_gl  = ar_rec.iloc[0].get("Avvik_GL_mot_Sub")
    ar_diff_acc = ar_rec.iloc[0].get("Avvik_Acc_mot_Sub")
    ap_diff_gl  = ap_rec.iloc[0].get("Avvik_GL_mot_Sub")
    ap_diff_acc = ap_rec.iloc[0].get("Avvik_Acc_mot_Sub")

    # MVA
    vat_ok_m   = int((vat["VAT_Recon_Month"].get("OK_TaxOnly", pd.Series([], dtype=bool))).sum())
    vat_rows_m = int(len(vat["VAT_Recon_Month"]))
    vat_ok_t   = int((vat["VAT_Recon_Term"].get("OK_TaxOnly", pd.Series([], dtype=bool))).sum())
    vat_rows_t = int(len(vat["VAT_Recon_Term"]))

    oversikt = pd.DataFrame([
        {"Kontroll":"Global balanse (debet = kredit)","Status":"OK" if abs(delta_global) <= 0.01 else "FEIL",
         "Nøkkeltall":f"Δ={delta_global:.2f}",
         "Hva betyr dette?":"Hele materialet balanserer. ≠0 indikerer datafeil eller manglende linjer.",
         "Tiltak":"Ved ≠0: se 'GlobalBalance' og 'UnbalancedVouchers'."},

        {"Kontroll":"Ubalanserte bilag","Status":"OK" if unb_count==0 else "FEIL",
         "Nøkkeltall":f"Antall={unb_count}",
         "Hva betyr dette?":"Bilag som ikke går i 0 på linjenivå.",
         "Tiltak":"Se arket 'UnbalancedVouchers'."},

        {"Kontroll":"Periodekompletthet","Status":"OK" if miss_months==0 else "OBS",
         "Nøkkeltall":f"Mangler mnd={miss_months}",
         "Hva betyr dette?":"Måneder i utvalget uten posteringer.",
         "Tiltak":"Verifiser periodeutvalg i SAF‑T."},

        {"Kontroll":"Duplikatkandidater","Status":"OK" if dup_count==0 else "OBS",
         "Nøkkeltall":f"Antall={dup_count}",
         "Hva betyr dette?":"Potensielle duplikater (samme voucher/journal/dato).",
         "Tiltak":"Se 'DuplicateCandidates'."},

        {"Kontroll":"TB (GL) vs Accounts (closing)","Status":"OK" if tb_issues==0 else "OBS",
         "Nøkkeltall":f"Konti m/avvik={tb_issues}",
         "Hva betyr dette?":"Avvik mellom GL-summer og UB i accounts.csv.",
         "Tiltak":"Se 'TB_vs_Accounts'."},

        {"Kontroll":"AR-avstemming (UB mot UB)","Status":"OK" if (ar_diff_gl is not None and ar_diff_acc is not None and abs(ar_diff_gl)<=1 and abs(ar_diff_acc)<=1) else "OBS",
         "Nøkkeltall":f"GL-Sub={ar_diff_gl}, Acc-Sub={ar_diff_acc}",
         "Hva betyr dette?":"Sum reskontro skal treffe UB på kontrollkonti.",
         "Tiltak":"Se 'AR_Recon'."},

        {"Kontroll":"AP-avstemming (UB mot UB)","Status":"OK" if (ap_diff_gl is not None and ap_diff_acc is not None and abs(ap_diff_gl)<=1 and abs(ap_diff_acc)<=1) else "OBS",
         "Nøkkeltall":f"GL-Sub={ap_diff_gl}, Acc-Sub={ap_diff_acc}",
         "Hva betyr dette?":"Sum reskontro skal treffe UB på kontrollkonti.",
         "Tiltak":"Se 'AP_Recon'."},

        {"Kontroll":"MVA-avstemming (Tax-only)","Status":"OK" if ((vat_ok_m==vat_rows_m) and (vat_ok_t==vat_rows_t)) else "OBS",
         "Nøkkeltall":f"Mnd OK={vat_ok_m}/{vat_rows_m}, Term OK={vat_ok_t}/{vat_rows_t}",
         "Hva betyr dette?":"Tax-only ekskluderer oppgjør/interim – bør stemme mot tax-linjene.",
         "Tiltak":"Se 'VAT_Recon_Month/Term'. Legg ev. 'vat_gl_accounts.csv' for presis kontoliste."},
    ])

    summary = pd.DataFrame([
        {"Check":"Global debet=kredit","Delta":round(delta_global,2),"OK":abs(delta_global)<=0.01},
        {"Check":"Ubalanserte bilag","Count":unb_count},
        {"Check":"TB vs Accounts (UB)","Issues":tb_issues},
        {"Check":"Manglende måneder","MissingMonths":miss_months},
        {"Check":"Duplikatbilag (kand.)","Count":dup_count},
        {"Check":"AR Avvik GL-Sub","Value":ar_diff_gl},
        {"Check":"AR Avvik Acc-Sub","Value":ar_diff_acc},
        {"Check":"AP Avvik GL-Sub","Value":ap_diff_gl},
        {"Check":"AP Avvik Acc-Sub","Value":ap_diff_acc},
        {"Check":"MVA OK mnd (Tax-only)","CountOK":vat_ok_m},
        {"Check":"MVA OK term (Tax-only)","CountOK":vat_ok_t},
    ])
    return oversikt, summary

def write_report(outdir: Path,
                 global_bal: pd.DataFrame,
                 unbalanced: pd.DataFrame,
                 tb_vs_acc: pd.DataFrame,
                 pc: pd.DataFrame,
                 dups: pd.DataFrame,
                 ar_rec: pd.DataFrame,
                 ap_rec: pd.DataFrame,
                 vat: Dict[str, pd.DataFrame],
                 unk_view: pd.DataFrame) -> Path:
    """Skriv control_report.xlsx og returner filsti."""
    out_xlsx = Path(outdir) / "control_report.xlsx"
    oversikt, summary = build_overview(global_bal, unbalanced, tb_vs_acc, pc, dups, ar_rec, ap_rec, vat)

    # topp 10 konti etter absolutt avvik i TB_vs_Accounts
    top_issues = pd.DataFrame()
    if "Diff_UB" in tb_vs_acc.columns:
        top_issues = (tb_vs_acc.assign(_abs=tb_vs_acc["Diff_UB"].abs())
                                .sort_values("_abs", ascending=False).head(10).drop(columns=["_abs"]))

    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter", datetime_format="dd.mm.yyyy") as xw:
        # Oversikt
        oversikt.to_excel(xw, index=False, sheet_name="Oversikt")
        ws = xw.sheets["Oversikt"]
        apply_common_format(xw, "Oversikt", oversikt)
        # Trafikklys på Status (NB: riktig sitering av FEIL)
        try:
            cols   = list(oversikt.columns)
            st_col = cols.index("Status")
            nrows  = len(oversikt.index)
            fmt_ok   = xw.book.add_format({"font_color":"black","bg_color":"#C6EFCE"})
            fmt_obs  = xw.book.add_format({"font_color":"black","bg_color":"#FFEB9C"})
            fmt_fail = xw.book.add_format({"font_color":"white","bg_color":"#FF0000"})
            ws.conditional_format(1, st_col, nrows, st_col, {"type":"cell","criteria":"==","value":'"OK"',   "format":fmt_ok})
            ws.conditional_format(1, st_col, nrows, st_col, {"type":"cell","criteria":"==","value":'"OBS"',  "format":fmt_obs})
            ws.conditional_format(1, st_col, nrows, st_col, {"type":"cell","criteria":"==","value":'"FEIL"', "format":fmt_fail})
        except Exception:
            pass

        # Summary
        summary.to_excel(xw, index=False, sheet_name="Summary")
        apply_common_format(xw, "Summary", summary)

        # Detaljark – hjelpefunksjon
        def _sheet(name: str, df: pd.DataFrame):
            if df is None or (hasattr(df, "empty") and df.empty):
                return
            df.to_excel(xw, index=False, sheet_name=name)
            apply_common_format(xw, name, df)

        _sheet("GlobalBalance",      global_bal)
        _sheet("UnbalancedVouchers", unbalanced)
        _sheet("TB_vs_Accounts",     tb_vs_acc)
        _sheet("TB_TopAvvik",        top_issues)
        _sheet("PeriodCompleteness", pc)
        _sheet("DuplicateCandidates", dups)
        _sheet("AR_Recon",           ar_rec)
        _sheet("AP_Recon",           ap_rec)

        _sheet("VAT_ByCode_Month",   vat["VAT_ByCode_Month"])
        _sheet("VAT_ByCode_Term",    vat["VAT_ByCode_Term"])
        _sheet("VAT_GL_Check_Month", vat["VAT_GL_Check_Month"])
        _sheet("VAT_GL_Check_Term",  vat["VAT_GL_Check_Term"])
        _sheet("VAT_Recon_Month",    vat["VAT_Recon_Month"])
        _sheet("VAT_Recon_Term",     vat["VAT_Recon_Term"])
        _sheet("VAT_GL_Config",      vat["VAT_GL_Config"])

        _sheet("UnknownNodes",       unk_view)

    return out_xlsx
