# src/app/parsers/saft_vat_report.py
# -*- coding: utf-8 -*-
"""
MVA-rapport (minimal + robust):

  make_vat_report(out_dir: Path) -> Path

- Leser transactions.csv
- Lager:
    VAT_By_Term       (signert TaxAmount per termin + SAFT_Map/TaxName + TaxPercentage + Σ Total + NoDate)
    VAT_By_Term_Base  (signert BaseAmount per termin + SAFT_Map/TaxName + TaxPercentage + Σ Total + NoDate)
    VAT_TopAccounts   (topp 3 kontoer per MVA-kode på grunnlag)
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import re

ACCOUNTING_FORMAT = '_-* # ##0,00_-;_-* (# ##0,00)_-;_-* "-"_-;_-@_-'
DATE_FORMAT = "yyyy-mm-dd"


# ---------------------------------------------------------------------------
# CSV / filhjelpere
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Excel-hjelpere
# ---------------------------------------------------------------------------


def _xlsx_writer(path: Path) -> pd.ExcelWriter:
    return pd.ExcelWriter(
        str(path),
        engine="xlsxwriter",
        datetime_format=DATE_FORMAT,
        engine_kwargs={
            "options": {
                "strings_to_urls": False,
                "strings_to_numbers": False,
                "strings_to_formulas": False,
            }
        },
    )


def _apply_formats(xw, sheet: str, df: pd.DataFrame) -> None:
    """Sett kolonnebredder, frys header, aktiver filter + tall/datoformat."""
    try:
        ws = xw.sheets[sheet]
        book = xw.book
        fmt_num = book.add_format({"num_format": ACCOUNTING_FORMAT})
        fmt_dt = book.add_format({"num_format": DATE_FORMAT})

        cols = list(df.columns)
        head = [len(str(c)) for c in cols]
        sample = df.head(300)

        for i, c in enumerate(cols):
            try:
                m = int(min(sample[c].astype(str).map(len).max(), 60))
            except Exception:
                m = 8
            w = max(10, min(60, max(head[i], m) + 2))

            if c in {"Σ Total", "NoDate", "TaxAmount", "BaseAmount"} or (
                c.startswith("20") and "-T" in c
            ):
                ws.set_column(i, i, w, fmt_num)
            elif c == "Date":
                ws.set_column(i, i, w, fmt_dt)
            else:
                ws.set_column(i, i, w)

        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, 0, len(cols) - 1)
    except Exception:
        # Vi vil ikke krasje rapportgenerering bare pga. formatering
        pass


def _write_sum_row(xw, sheet: str, df: pd.DataFrame) -> None:
    """Skriv en enkel SUM-rad nederst på arket."""
    try:
        ws = xw.sheets[sheet]
        fmt = xw.book.add_format({"num_format": ACCOUNTING_FORMAT})
        r = 1 + len(df) + 1
        ws.write(r, 0, "SUM")
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                cidx = list(df.columns).index(col)
                ws.write_number(
                    r,
                    cidx,
                    float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum()),
                    fmt,
                )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Datohjelpere
# ---------------------------------------------------------------------------


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["PostingDate", "TransactionDate"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")
    out["Date"] = out.get("PostingDate").fillna(out.get("TransactionDate"))
    return out


def _term_label(d: pd.Series) -> pd.Series:
    """Lag periodeetiketter YYYY-T1..T6 basert på dato."""
    dt = pd.to_datetime(d, errors="coerce")
    y = dt.dt.year
    m = dt.dt.month
    t = ((m - 1) // 2) + 1
    s = y.astype("Int64").astype(str) + "-T" + t.astype("Int64").astype(str)
    return s.where(~dt.isna(), other="NoDate")


# ---------------------------------------------------------------------------
# Diverse helpers
# ---------------------------------------------------------------------------


def _coerce_percent(x) -> float:
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    m = re.search(r"([0-9]+([.,][0-9]+)?)", str(x))
    if not m:
        return 0.0
    return float(m.group(1).replace(",", "."))


def _norm_type(s) -> str:
    """Normaliserer MVA-type til 'IN'/'OUT' og tåler også tall/NaN.

    Tidligere fikk vi feilen:
        "'float' object has no attribute 'strip'"
    når datagrunnlaget inneholdt float/NaN. Dette unngås nå ved alltid
    å gå via str() etter en pd.isna-sjekk.
    """
    if pd.isna(s):
        raw = ""
    else:
        raw = str(s)

    v = raw.strip().lower()
    if v in {"in", "input", "purchase", "inngående", "inng"}:
        return "IN"
    if v in {"out", "output", "sales", "utgående", "utg"}:
        return "OUT"
    return raw.strip()


def _series_or_default(
    df: pd.DataFrame, primary: str, *alts: str, default: object = ""
) -> pd.Series:
    """
    Returner en Series fra df (primary eller alternativene).
    Hvis ingen av kolonnene finnes, returneres en Series med default-verdi
    for hver rad. Dette sikrer at vi ALDRI får en ren float/int tilbake
    som ikke støtter .map().
    """
    for col in (primary, *alts):
        if col in df.columns:
            return df[col]
    # Ingen kolonner – lag default-serie på samme index
    return pd.Series([default] * len(df), index=df.index)


def _load_mapping(out_dir: Path) -> pd.DataFrame:
    """
    Leser mapping_overview / TaxTable osv. og returnerer et DataFrame med:
      TaxCode, SAFT_Map, TaxName, VAT_Key_Map
    """
    names = ["mapping_overview.xlsx", "vat_mapping.xlsx", "tax_codes.xlsx", "TaxTable.xlsx"]
    p: Optional[Path] = None
    for nm in names:
        for c in [out_dir / nm, out_dir.parent / "excel" / nm, out_dir.parent / "csv" / nm]:
            if c.exists():
                p = c
                break
        if p:
            break

    if p is None:
        return pd.DataFrame(columns=["TaxCode", "SAFT_Map", "TaxName", "VAT_Key_Map"])

    try:
        df = pd.read_excel(p, dtype=str)
    except Exception:
        return pd.DataFrame(columns=["TaxCode", "SAFT_Map", "TaxName", "VAT_Key_Map"])

    if df.empty:
        return pd.DataFrame(columns=["TaxCode", "SAFT_Map", "TaxName", "VAT_Key_Map"])

    out = pd.DataFrame(index=df.index)

    # TaxCode
    out["TaxCode"] = _series_or_default(df, "TaxCode", "Code", default="").astype(str)

    # SAFT standardkode
    out["SAFT_Map"] = _series_or_default(df, "SAFT_Map", "StandardTaxCode", default="").astype(
        str
    )

    # Navn/beskrivelse
    out["TaxName"] = _series_or_default(
        df, "TaxName", "Name", "Description", default=""
    ).astype(str)

    # Prosent + type -> VAT_Key_Map
    pct_raw = _series_or_default(df, "TaxPercent", "Rate", "Sats", default=0.0)
    pct = pct_raw.map(_coerce_percent).fillna(0.0)

    ttype_raw = _series_or_default(df, "TaxType", "Direction", default="")
    ttype = ttype_raw.map(_norm_type)

    out["VAT_Key_Map"] = ttype.astype(str) + "_" + pct.map(lambda x: f"{x:g}%")

    return out.drop_duplicates()


def _attach_mapping(piv: pd.DataFrame, mp: pd.DataFrame) -> pd.DataFrame:
    """Legg SAFT_Map/TaxName på pivot basert på TaxCode / VAT_Key_Map."""
    if piv.empty or mp.empty:
        piv["SAFT_Map"] = ""
        piv["TaxName"] = ""
        return piv

    df = piv.merge(mp[["TaxCode", "SAFT_Map", "TaxName"]], on="TaxCode", how="left")

    # Hvis TaxCode ikke traff, prøv VAT_Key_Map
    miss = df["SAFT_Map"].isna() | (df["SAFT_Map"] == "")
    if miss.any() and "VAT_Key_Map" in mp.columns:
        lk = (
            mp[mp["VAT_Key_Map"].astype(str).str.strip() != ""]
            [["VAT_Key_Map", "SAFT_Map", "TaxName"]]
            .drop_duplicates()
        )
        if not lk.empty:
            sub = df.loc[miss, ["TaxCode"]].merge(
                lk, left_on="TaxCode", right_on="VAT_Key_Map", how="left"
            )
            df.loc[miss, "SAFT_Map"] = sub["SAFT_Map"].values
            df.loc[miss, "TaxName"] = sub["TaxName"].values

    return df


# ---------------------------------------------------------------------------
# Hovedfunksjon
# ---------------------------------------------------------------------------


def make_vat_report(out_dir: Path) -> Path:
    """Generer MVA-rapport basert på transactions.csv i out_dir."""
    out_dir = Path(out_dir)

    txp = _find_csv(out_dir, "transactions.csv")
    if not txp:
        raise FileNotFoundError("transactions.csv mangler")

    tx = _read_csv_safe(txp, dtype=str)
    if tx.empty:
        raise ValueError("transactions.csv er tom")

    tx = _parse_dates(tx)

    keep_cols = [
        c
        for c in [
            "Date",
            "AccountID",
            "AccountDescription",
            "TaxCode",
            "TaxType",
            "TaxPercent",
            "TaxAmount",
            "TaxableBase",
            "Amount",
        ]
        if c in tx.columns
    ]
    vat = tx[keep_cols].copy()

    # Filtrer til linjer med relevant MVA-info
    has_vat = pd.Series(False, index=vat.index)
    if "TaxAmount" in vat.columns:
        has_vat |= pd.to_numeric(vat["TaxAmount"], errors="coerce").fillna(0.0).ne(0.0)
    for c in ["TaxCode", "TaxPercent", "TaxType"]:
        if c in vat.columns:
            has_vat |= vat[c].astype(str).str.strip().ne("")
    vat = vat.loc[has_vat].copy()

    # Termin
    vat = vat.assign(Term=_term_label(vat["Date"]))

    # Konverter tallfelt
    for c in ["TaxAmount", "TaxableBase", "Amount", "TaxPercent"]:
        if c in vat.columns:
            vat[c] = pd.to_numeric(vat[c], errors="coerce").fillna(0.0)

    # BaseAmount – forsøk å bruke TaxableBase, ellers reverser, ellers Amount - TaxAmount
    def _base(row):
        if "TaxableBase" in row and row["TaxableBase"] != 0:
            return row["TaxableBase"]
        ta = row.get("TaxAmount", 0.0)
        p = row.get("TaxPercent", 0.0)
        if p:
            return ta / (p / 100.0)
        return row.get("Amount", 0.0) - ta

    vat["BaseAmount"] = vat.apply(_base, axis=1)

    # VAT_Key / RowCode
    ttype = vat.get("TaxType", "").map(_norm_type)
    vat["VAT_Key"] = ttype.astype(str) + "_" + vat.get("TaxPercent", 0.0).map(
        lambda x: f"{x:g}%"
    )
    vat["RowCode"] = vat.get("TaxCode", "").astype(str).where(
        vat.get("TaxCode", "").astype(str).str.strip() != "", other=vat["VAT_Key"]
    )

    # SIGN – bevar retning fra Amount, men bruk absoluttbeløp
    sign = np.sign(vat.get("Amount", 0.0)).replace(0, 1)
    vat["SignedTaxAmount"] = sign * vat.get("TaxAmount", 0.0).abs()
    vat["SignedBaseAmount"] = sign * vat["BaseAmount"].abs()

    # Dominant prosent pr. RowCode
    def _mode_percent(s: pd.Series) -> float:
        nums = pd.to_numeric(s, errors="coerce").dropna()
        if nums.empty:
            return 0.0
        return float(nums.mode().iloc[0])

    dom = (
        vat.groupby("RowCode")["TaxPercent"]
        .agg(_mode_percent)
        .rename("TaxPercentage")
        .reset_index()
    )

    # Pivot skatt og grunnlag
    piv_tax = (
        vat.pivot_table(
            index="RowCode",
            columns="Term",
            values="SignedTaxAmount",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
        .rename(columns={"RowCode": "TaxCode"})
    )
    piv_base = (
        vat.pivot_table(
            index="RowCode",
            columns="Term",
            values="SignedBaseAmount",
            aggfunc="sum",
            fill_value=0.0,
        )
        .reset_index()
        .rename(columns={"RowCode": "TaxCode"})
    )

    # Legg på prosent
    for piv in (piv_tax, piv_base):
        piv["TaxPercentage"] = 0.0

    piv_tax = piv_tax.merge(dom.rename(columns={"RowCode": "TaxCode"}), on="TaxCode", how="left")
    piv_base = piv_base.merge(
        dom.rename(columns={"RowCode": "TaxCode"}), on="TaxCode", how="left"
    )

    # Mapping (SAFT-kode + navn)
    mp = _load_mapping(out_dir)
    piv_tax = _attach_mapping(piv_tax, mp)
    piv_base = _attach_mapping(piv_base, mp)

    def _add_total(piv: pd.DataFrame) -> pd.DataFrame:
        cols = [c for c in piv.columns if c not in {"TaxCode", "SAFT_Map", "TaxName", "TaxPercentage"}]
        num = [c for c in cols if pd.api.types.is_numeric_dtype(piv[c])]
        piv["Σ Total"] = piv[num].sum(axis=1)
        left = ["TaxCode", "SAFT_Map", "TaxName", "TaxPercentage"]
        dyn = [c for c in piv.columns if c not in left]
        nodate = [c for c in dyn if c == "NoDate"]
        other = [c for c in dyn if c not in nodate and c != "Σ Total"]
        return piv[left + sorted(other) + nodate + ["Σ Total"]]

    piv_tax = _add_total(piv_tax)
    piv_base = _add_total(piv_base)

    # TopAccounts – største kontoer per MVA-kode
    grp = (
        vat.groupby(["RowCode", "AccountID", "AccountDescription"], dropna=False)["BaseAmount"]
        .agg(Lines="size", AbsBase=lambda s: float(np.abs(s).sum()))
        .reset_index()
    )
    grp["Rank"] = grp.groupby("RowCode")["AbsBase"].rank(method="first", ascending=False)
    tot = grp.groupby("RowCode")["AbsBase"].sum().rename("AbsBaseTotal").reset_index()
    top3 = grp.merge(tot, on="RowCode", how="left")
    top3["ShareOfCode"] = np.where(
        top3["AbsBaseTotal"] != 0, top3["AbsBase"] / top3["AbsBaseTotal"], 0.0
    )

    top3 = top3.rename(columns={"RowCode": "TaxCode"}).merge(
        dom.rename(columns={"RowCode": "TaxCode"}), on="TaxCode", how="left"
    )
    if not mp.empty:
        top3 = top3.merge(mp[["TaxCode", "SAFT_Map", "TaxName"]], on="TaxCode", how="left")

    top3 = top3[
        [
            "TaxCode",
            "SAFT_Map",
            "TaxName",
            "TaxPercentage",
            "Rank",
            "AccountID",
            "AccountDescription",
            "Lines",
            "AbsBase",
            "ShareOfCode",
        ]
    ].sort_values(["TaxCode", "Rank", "AbsBase"], ascending=[True, True, False])

    # Skriv Excel
    excel_dir = out_dir.parent / "excel"
    excel_dir.mkdir(parents=True, exist_ok=True)
    out_path = excel_dir / "vat_report.xlsx"

    with _xlsx_writer(out_path) as xw:
        piv_tax.to_excel(xw, index=False, sheet_name="VAT_By_Term")
        _apply_formats(xw, "VAT_By_Term", piv_tax)
        _write_sum_row(xw, "VAT_By_Term", piv_tax)

        piv_base.to_excel(xw, index=False, sheet_name="VAT_By_Term_Base")
        _apply_formats(xw, "VAT_By_Term_Base", piv_base)
        _write_sum_row(xw, "VAT_By_Term_Base", piv_base)

        if not top3.empty:
            top3.to_excel(xw, index=False, sheet_name="VAT_TopAccounts")
            _apply_formats(xw, "VAT_TopAccounts", top3)

    print(f"[excel] Skrev MVA-rapport: {out_path}")
    return out_path
