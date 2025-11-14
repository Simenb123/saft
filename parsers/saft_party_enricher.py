# app/parsers/saft_party_enricher.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from typing import Iterable, List, Optional, Tuple
import pandas as pd

# --- små hjelpere ------------------------------------------------------------

def _read_csv_safe(path: Optional[Path], dtype=str) -> Optional[pd.DataFrame]:
    if path is None:
        return None
    try:
        return pd.read_csv(path, dtype=dtype, keep_default_na=False)
    except Exception:
        return None

def _find_csv_file(root: Path, filename: str) -> Optional[Path]:
    """Søk i root og oppover, og rglob som fallback (robust)."""
    root = Path(root)
    chain: List[Path] = []
    cur = root
    while True:
        chain.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent
    for d in chain:
        p = d / filename
        if p.is_file():
            return p
    for base in chain:
        try:
            for p in base.rglob(filename):
                if p.is_file():
                    return p
        except Exception:
            continue
    return None

def _has_value(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip().str.lower()
    return ~t.isin(["", "nan", "none", "nat"])

def _ana_is_customer(v: str) -> bool:
    v = (v or "").strip().lower()
    return ("cust" in v) or ("kunde" in v)

def _ana_is_supplier(v: str) -> bool:
    v = (v or "").strip().lower()
    return ("suppl" in v) or ("leverand" in v) or ("vendor" in v)

def _first_nonempty(series: pd.Series) -> str:
    for x in series:
        sx = str(x).strip()
        if sx and sx.lower() not in ("nan", "none", "nat"):
            return sx
    return ""

# --- hovedfunksjon -----------------------------------------------------------

def enrich_party_ids(tx: pd.DataFrame, outdir: Path) -> pd.DataFrame:
    """
    Fyller inn manglende CustomerID/SupplierID i transaksjoner ved å slå opp
    i analysis_lines.csv (hvis til stede). Eksisterende verdier beholdes.

    Strategi:
      1) Les analysis_lines.csv (eller analysis.csv) og filtrer til rader som
         gjelder Customer/Kunde og Supplier/Leverandør (på AnalysisType).
      2) Forsøk join mot transaksjoner på prioritert nøkkel:
         LineID -> TransactionLineID/TransLineID -> TransactionID -> (andre felles ID-kolonner).
      3) Aggreger analysis per nøkkel til to kolonner: CustomerID_from_analysis, SupplierID_from_analysis.
      4) Fyll inn manglende CustomerID/SupplierID i tx fra disse kolonnene.
    """
    tx = tx.copy()

    # Sørg for at kolonnene finnes (kan være fraværende i noen filer)
    for col in ("CustomerID", "SupplierID"):
        if col not in tx.columns:
            tx[col] = ""

    # Finn analysefil
    al_path = (
        _find_csv_file(outdir, "analysis_lines.csv")
        or _find_csv_file(outdir, "analysis.csv")
        or _find_csv_file(outdir, "analysis_lines.txt")  # ekstrem fallback
    )
    al = _read_csv_safe(al_path, dtype=str)
    if al is None or al.empty:
        return tx  # ingenting å berike med

    # Standardiser kolonnenavn litt (noen filer kan bruke alternative navn)
    cols = {c.lower(): c for c in al.columns}
    # Kandidatfelt for AnalysisType / AnalysisID:
    type_col = cols.get("analysistype") or cols.get("type") or cols.get("dimensiontype")
    id_col   = cols.get("analysisid")  or cols.get("id")   or cols.get("code")
    if not type_col or not id_col:
        return tx  # mangler essensielle felt

    # Filtrer kun Customer/Supplier-linjer
    al = al.loc[al[type_col].apply(lambda x: _ana_is_customer(x) or _ana_is_supplier(x))].copy()
    if al.empty:
        return tx

    # Velg join-nøkkel(r)
    # Høyeste prioritet: LineID (Analysis henger vanligvis på Linienivå)
    candidate_keys: List[Tuple[str, ...]] = []
    for c in ("lineid", "transactionlineid", "translineid", "entrylineid"):
        if c in cols and c in {x.lower(): x for x in tx.columns}:
            candidate_keys.append((cols[c],))
    # Deretter TransactionID
    if "transactionid" in cols and "TransactionID" in tx.columns:
        candidate_keys.append((cols["transactionid"],))
    # Felles ID-kolonner (bred fallback – men bare enkeltnøkler)
    tx_lc = {c.lower(): c for c in tx.columns}
    for c_lc, c_name in cols.items():
        if c_lc in ("analysistype", "analysisid", "amount", "value"):
            continue
        if c_lc in tx_lc and (tx_lc[c_lc],) not in candidate_keys:
            candidate_keys.append((c_name,))

    if not candidate_keys:
        return tx

    # Velg den nøkkelen som gir flest treff inn i tx
    best_key: Optional[Tuple[str, ...]] = None
    best_hits = -1
    for key in candidate_keys:
        # sjekk match-rate grovt
        if any(k not in al.columns for k in key) or any(k not in tx.columns for k in key):
            continue
        hits = al[key].dropna().astype(str).isin(tx[key].dropna().astype(str)).sum()
        if hits > best_hits:
            best_key, best_hits = key, hits

    if best_key is None or best_hits <= 0:
        return tx

    # Aggreger til én rad per nøkkel
    al_small = al[[*best_key, type_col, id_col]].copy()
    al_small["__cust__"] = al_small.apply(lambda r: r[id_col] if _ana_is_customer(r[type_col]) else "", axis=1)
    al_small["__supp__"] = al_small.apply(lambda r: r[id_col] if _ana_is_supplier(r[type_col]) else "", axis=1)
    grp = al_small.groupby(list(best_key), dropna=False).agg(
        CustomerID_from_analysis=(".__cust__", _first_nonempty),
        SupplierID_from_analysis=(".__supp__", _first_nonempty),
    ).reset_index()

    # Slå sammen inn i tx
    tx = tx.merge(grp, on=list(best_key), how="left")

    # Fyll bare der vi mangler fra før
    def _fill(dst: pd.Series, src: pd.Series) -> pd.Series:
        dst = dst.astype(str)
        src = src.astype(str)
        mask_empty = ~_has_value(dst)
        dst.loc[mask_empty] = src.loc[mask_empty]
        return dst

    if "CustomerID_from_analysis" in tx.columns:
        tx["CustomerID"] = _fill(tx["CustomerID"], tx["CustomerID_from_analysis"])
        tx.drop(columns=["CustomerID_from_analysis"], inplace=True, errors="ignore")
    if "SupplierID_from_analysis" in tx.columns:
        tx["SupplierID"] = _fill(tx["SupplierID"], tx["SupplierID_from_analysis"])
        tx.drop(columns=["SupplierID_from_analysis"], inplace=True, errors="ignore")

    return tx
