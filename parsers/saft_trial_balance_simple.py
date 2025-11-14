# -*- coding: utf-8 -*-
"""
saft_trial_balance_simple.py
----------------------------
Lager ENKEL Trial Balance:

Workbook: trial_balance.xlsx  (skrives i csv/-mappen)
  - Sheet 'TrialBalance'  : AccountID, AccountDescription, IB, Movement, UB
                            (kun rader hvor minst én av IB/Movement/UB != 0)
  - Sheet 'Accounts'      : AccountID, AccountDescription, IB, Movement, UB
                            (ALLE kontoer – full kontoplan + kontoer som kun
                             forekommer i totals/transactions)

Kilder:
  - IB/UB:  KUN fra csv/accounts.csv (nettobeløp hvis tilgjengelig, ellers Opening/Closing Debit/Credit på konto)
  - Movement:
      1) csv/gl_totals.csv (periodens nettobevegelse),
      2) ellers hvis både IB og UB finnes → UB - IB,
      3) ellers fallback: csv/transactions.csv (Debet - Kredit eller Amount)

Meta:
  - csv/simple_trial_balance_meta.json forklarer nøyaktig hvilke kilder som ble brukt og ev. avvik.

Ingen antakelser: Vi beregner ikke IB/UB fra GL. Bevegelser hentes i prioritert rekkefølge, men IB/UB forblir slik de er i Accounts.
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set, Sequence, Union
import csv
import json
import re

try:
    import xlsxwriter  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError("xlsxwriter er påkrevd for å bygge trial_balance.xlsx") from e


# ---------------------------- CSV utils ----------------------------

def _sniff_delimiter(path: Path) -> str:
    sample = path.read_text(encoding="utf-8", errors="replace")[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t,")
        return dialect.delimiter
    except Exception:
        if sample.count(";") > sample.count(","):
            return ";"
        if "\t" in sample:
            return "\t"
        if "|" in sample:
            return "|"
        return ","


def _read_csv_any(path: Path) -> Tuple[List[Dict[str, str]], str]:
    if not path.exists():
        return [], ","
    delim = _sniff_delimiter(path)
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter=delim)
        for row in r:
            norm: Dict[str, str] = {}
            for k, v in row.items():
                if k is None:
                    continue
                nk = k.strip().lstrip("\ufeff")
                norm[nk] = "" if v is None else str(v).strip()
            rows.append(norm)
    return rows, delim


def _to_float(s: str) -> float:
    if s is None:
        return 0.0
    t = str(s).strip()
    if not t or t.lower() == "nan":
        return 0.0
    neg = t.startswith("(") and t.endswith(")")
    if neg:
        t = t[1:-1]
    t = t.replace("\xa0", "").replace(" ", "")
    if t.count(",") > 0 and t.count(".") == 0:
        t = t.replace(",", ".")
    try:
        val = float(t)
    except Exception:
        val = 0.0
    return -val if neg else val


# ---------------------------- felt‑deteksjon ----------------------------

Key = Union[str, re.Pattern]

# Konto-id / navn
_ACC_ID_KEYS   : Sequence[Key] = ("AccountID", "Account", "AccountNumber", "AccountNo", "Konto", "Kontonr", "KontoNr")
_ACC_NAME_KEYS : Sequence[Key] = ("AccountDescription", "AccountName", "Name", "AccountDesc", "Description", "Kontonavn")

# IB/UB fra ACCOUNTS (nettobeløp, uten debit/credit)
_ACC_IB_NET_KEYS: Sequence[Key] = (
    "IB", "OpeningBalance", "Opening_Balance", "OpeningNet", "OpeningBalanceAmount",
    re.compile(r"(open|opening|begin(ning)?|ob)\w*.*(balance|balanse|saldo|net)", re.I),
)
_ACC_UB_NET_KEYS: Sequence[Key] = (
    "UB", "ClosingBalance", "Closing_Balance", "ClosingNet", "ClosingBalanceAmount", "EndBalance", "EndingBalance",
    re.compile(r"(close|closing|end(ing)?|ub|utg)\w*.*(balance|balanse|saldo|net)", re.I),
)

# IB/UB fra ACCOUNTS (parvise debit/credit)
_ACC_OPEN_DEBIT_KEYS   : Sequence[Key] = ("OpeningDebit", "OpeningDebitBalance", "Opening_Debit", "BeginDebit",
                                          re.compile(r"(open|opening|begin(ning)?|ob)\w*.*(debit|debet)", re.I))
_ACC_OPEN_CREDIT_KEYS  : Sequence[Key] = ("OpeningCredit", "OpeningCreditBalance", "Opening_Credit", "BeginCredit",
                                          re.compile(r"(open|opening|begin(ning)?|ob)\w*.*(credit|kredit)", re.I))
_ACC_CLOSE_DEBIT_KEYS  : Sequence[Key] = ("ClosingDebit", "ClosingDebitBalance", "Closing_Debit", "EndDebit",
                                          re.compile(r"(close|closing|end(ing)?|ub|utg)\w*.*(debit|debet)", re.I))
_ACC_CLOSE_CREDIT_KEYS : Sequence[Key] = ("ClosingCredit","ClosingCreditBalance","Closing_Credit","EndCredit",
                                          re.compile(r"(close|closing|end(ing)?|ub|utg)\w*.*(credit|kredit)", re.I))

# Bevegelser fra GL‑totals (perioden)
_GL_MOV_KEYS : Sequence[Key] = (
    "Movement", "NetChange", "Change", "PeriodMovement", "Period_Net", "NetMovement",
    re.compile(r"(period|movement|net)\w*", re.I),
)
_GL_PER_DEBIT_KEYS  : Sequence[Key] = ("PeriodDebit", "MovementDebit", "Period_Debit", "Debit", "DebitAmount", "ThisPeriodDebit", "Debet",
                                       re.compile(r"(period|movement|net|thisperiod)\w*.*(debit|debet)", re.I))
_GL_PER_CREDIT_KEYS : Sequence[Key] = ("PeriodCredit","MovementCredit","Period_Credit","Credit","CreditAmount","ThisPeriodCredit","Kredit",
                                       re.compile(r"(period|movement|net|thisperiod)\w*.*(credit|kredit)", re.I))

# Fallback fra transactions.csv
_TRX_AMOUNT_KEYS : Sequence[Key] = ("Amount", "AmountNOK", "AmountBase", "AmountMST", "NetAmount", "Beløp")
_TRX_DEBIT_KEYS  : Sequence[Key] = ("Debit", "DebitAmount", "Debet")
_TRX_CREDIT_KEYS : Sequence[Key] = ("Credit", "CreditAmount", "Kredit")


def _match_one(k_lower: str, pat: Key) -> bool:
    if isinstance(pat, str):
        if k_lower == pat.lower():
            return True
        if pat.lower() in k_lower:
            return True
        return False
    return bool(pat.search(k_lower))


def _first_value_by_patterns(row: Dict[str, str], patterns: Sequence[Key], *,
                             forbid_substrings: Sequence[str] = ()) -> Tuple[Optional[float], Optional[str]]:
    """Finn første verdi i rad som matcher gitte patterns (respekter header-rekkefølge).
       Kan filtrere ut nøkler som inneholder ord vi ikke vil ha (f.eks. 'debit'/'credit')."""
    # 1) bygg liste over (key, lower) i header-rekkefølge
    ordered_keys = list(row.keys())
    lowers = [k.strip().lower() for k in ordered_keys]

    # 2) sjekk hvert pattern i gitt rekkefølge; hent første match i header-rekkefølge
    for pat in patterns:
        for idx, k_lower in enumerate(lowers):
            if not _match_one(k_lower, pat):
                continue
            if forbid_substrings and any(bad in k_lower for bad in forbid_substrings):
                continue
            key = ordered_keys[idx]
            v = row.get(key, "")
            if v not in (None, ""):
                return _to_float(v), key
    return None, None


def _get_text(row: Dict[str, str], keys: Sequence[Key]) -> Optional[str]:
    """Hent tekst via patterns (brukes i navn/id)."""
    ordered_keys = list(row.keys())
    lowers = [k.strip().lower() for k in ordered_keys]
    # 1) eksakt/substr for str-nøkler
    for pat in keys:
        if isinstance(pat, str):
            for idx, k_lower in enumerate(lowers):
                if k_lower == pat.lower() or pat.lower() in k_lower:
                    v = row.get(ordered_keys[idx])
                    if v not in (None, ""):
                        return v
    # 2) regex
    for pat in keys:
        if isinstance(pat, re.Pattern):
            for idx, k_lower in enumerate(lowers):
                if pat.search(k_lower):
                    v = row.get(ordered_keys[idx])
                    if v not in (None, ""):
                        return v
    return None


def _get_acc_id(row: Dict[str, str]) -> str:
    v = _get_text(row, _ACC_ID_KEYS)
    return "" if v is None else str(v)


def _get_acc_name(row: Dict[str, str]) -> str:
    v = _get_text(row, _ACC_NAME_KEYS)
    return "" if v is None else str(v)


def _ib_ub_from_accounts_row(row: Dict[str, str]) -> Tuple[float, float, Dict[str, str]]:
    """Hent IB/UB fra accounts-rad: først nettobeløp (uten debit/credit), ellers parvise debit/credit."""
    meta: Dict[str, str] = {"ib_source": "missing", "ub_source": "missing"}

    # IB netto (utelukker debit/credit)
    ib, ib_key = _first_value_by_patterns(row, _ACC_IB_NET_KEYS,
                                          forbid_substrings=("debit", "debet", "credit", "kredit"))
    if ib is not None:
        meta["ib_source"] = f"accounts_net:{ib_key}"

    # UB netto (utelukker debit/credit)
    ub, ub_key = _first_value_by_patterns(row, _ACC_UB_NET_KEYS,
                                          forbid_substrings=("debit", "debet", "credit", "kredit"))
    if ub is not None:
        meta["ub_source"] = f"accounts_net:{ub_key}"

    # Om ikke funnet som nettobeløp: forsøk parvise
    if ib is None:
        od, _ = _first_value_by_patterns(row, _ACC_OPEN_DEBIT_KEYS)
        oc, _ = _first_value_by_patterns(row, _ACC_OPEN_CREDIT_KEYS)
        if od is not None or oc is not None:
            ib = (od or 0.0) - (oc or 0.0)
            meta["ib_source"] = "accounts_pair"

    if ub is None:
        cd, _ = _first_value_by_patterns(row, _ACC_CLOSE_DEBIT_KEYS)
        cc, _ = _first_value_by_patterns(row, _ACC_CLOSE_CREDIT_KEYS)
        if cd is not None or cc is not None:
            ub = (cd or 0.0) - (cc or 0.0)
            meta["ub_source"] = "accounts_pair"

    return ib or 0.0, ub or 0.0, meta


def _movement_from_gl_totals_row(row: Dict[str, str]) -> Tuple[Optional[float], str]:
    """Finn periodens nettobevegelse i GL-totals-rad (direkte eller Debet/Kredit-par)."""
    mv, mv_key = _first_value_by_patterns(row, _GL_MOV_KEYS)
    if mv is not None:
        return mv, f"gl_totals:direct:{mv_key}"
    pd, _ = _first_value_by_patterns(row, _GL_PER_DEBIT_KEYS)
    pc, _ = _first_value_by_patterns(row, _GL_PER_CREDIT_KEYS)
    if pd is not None or pc is not None:
        return (pd or 0.0) - (pc or 0.0), "gl_totals:pair"
    return None, "missing"


def _fallback_movement_from_transactions(csv_dir: Path) -> Tuple[Dict[str, float], Dict[str, str], str]:
    """Aggreger netto bevegelse per konto fra transactions.csv."""
    trx_p = csv_dir / "transactions.csv"
    if not trx_p.exists():
        return {}, {}, ","
    rows, delim = _read_csv_any(trx_p)
    mv: Dict[str, float] = {}
    names: Dict[str, str] = {}

    for r in rows:
        acc = _get_acc_id(r) or r.get("AccountID", "")
        if not acc:
            continue
        amt = None
        txt = _get_text(r, _TRX_AMOUNT_KEYS)
        if txt not in (None, ""):
            amt = _to_float(txt)
        else:
            d, _ = _first_value_by_patterns(r, _TRX_DEBIT_KEYS)
            c, _ = _first_value_by_patterns(r, _TRX_CREDIT_KEYS)
            if d is not None or c is not None:
                amt = (d or 0.0) - (c or 0.0)
        if amt is None:
            continue
        mv[acc] = mv.get(acc, 0.0) + amt
        nm = _get_acc_name(r) or r.get("AccountDescription", "")
        if nm and acc not in names:
            names[acc] = nm
    return mv, names, delim


# -------------------------- hovedlogikk --------------------------

def make_simple_trial_balance(csv_dir: Path) -> Path:
    """
    Leser csv/accounts.csv (for IB/UB) + csv/gl_totals.csv (for Movement om mulig),
    ellers transactions.csv for Movement. Bygger trial_balance.xlsx og meta.
    """
    csv_dir = Path(csv_dir)

    accounts_rows, acc_delim = _read_csv_any(csv_dir / "accounts.csv")
    totals_rows, tot_delim   = _read_csv_any(csv_dir / "gl_totals.csv")

    # 1) Hent IB/UB fra Accounts
    acc_name: Dict[str, str] = {}
    ib_by_acc: Dict[str, float] = {}
    ub_by_acc: Dict[str, float] = {}
    ib_src_count: Dict[str, int] = {}
    ub_src_count: Dict[str, int] = {}

    for r in accounts_rows:
        aid = _get_acc_id(r) or r.get("AccountID", "")
        if not aid:
            continue
        nm = _get_acc_name(r) or r.get("AccountDescription", "")
        if nm:
            acc_name[aid] = nm
        ib, ub, m = _ib_ub_from_accounts_row(r)
        if ib != 0.0 or ub != 0.0:
            ib_by_acc[aid] = ib_by_acc.get(aid, 0.0) + ib
            ub_by_acc[aid] = ub_by_acc.get(aid, 0.0) + ub
        ib_src_count[m["ib_source"]] = ib_src_count.get(m["ib_source"], 0) + 1
        ub_src_count[m["ub_source"]] = ub_src_count.get(m["ub_source"], 0) + 1

    # 2) Finn Movement (prioritert)
    mv_by_acc: Dict[str, float] = {}
    mv_src_count: Dict[str, int] = {}

    if totals_rows:
        for r in totals_rows:
            aid = _get_acc_id(r) or r.get("AccountID", "")
            if not aid:
                # løs fallback – se etter "account" i header
                for k, v in r.items():
                    if k and "account" in k.lower() and v:
                        aid = str(v).strip()
                        break
            if not aid:
                continue
            mv, src = _movement_from_gl_totals_row(r)
            if mv is not None:
                mv_by_acc[aid] = mv_by_acc.get(aid, 0.0) + mv
                mv_src_count[src] = mv_src_count.get(src, 0) + 1
            nm = _get_acc_name(r) or r.get("AccountDescription", "")
            if nm and aid not in acc_name:
                acc_name[aid] = nm

    # Hvis noen kontoer fortsatt mangler Movement: forsøk UB-IB (kun der IB & UB finnes)
    for aid in set(ib_by_acc.keys()) & set(ub_by_acc.keys()):
        if aid not in mv_by_acc:
            mv_by_acc[aid] = ub_by_acc[aid] - ib_by_acc[aid]
            mv_src_count["accounts_diff"] = mv_src_count.get("accounts_diff", 0) + 1

    # Fortsatt hull? Fallback til transactions
    need_fallback = [aid for aid in (set(acc_name.keys()) | set(ib_by_acc.keys()) | set(ub_by_acc.keys())) if aid not in mv_by_acc]
    if need_fallback:
        trx_mv, trx_names, trx_delim = _fallback_movement_from_transactions(csv_dir)
        for aid in need_fallback:
            if aid in trx_mv:
                mv_by_acc[aid] = trx_mv[aid]
        for aid, nm in trx_names.items():
            acc_name.setdefault(aid, nm)
        tot_delim = tot_delim or trx_delim  # meta
        if trx_mv:
            mv_src_count["transactions"] = mv_src_count.get("transactions", 0) + len(trx_mv)

    # 3) Union av alle kontoer vi har sett
    all_accounts: List[str] = sorted(set(acc_name.keys()) | set(ib_by_acc.keys()) | set(ub_by_acc.keys()) | set(mv_by_acc.keys()),
                                     key=lambda x: (x or ""))

    # 4) Bygg rader for Accounts (alle) og TrialBalance (filtrert)
    accounts_rows_out: List[Tuple[str, str, float, float, float]] = []
    tb_rows: List[Tuple[str, str, float, float, float]] = []
    discrepancies: List[Dict[str, float]] = []

    for aid in all_accounts:
        nm = acc_name.get(aid, "")
        ib = ib_by_acc.get(aid, 0.0)
        ub = ub_by_acc.get(aid, 0.0)
        mv = mv_by_acc.get(aid, 0.0)

        # Avvik: dersom vi har alle tre, sjekk UB ≈ IB + MV
        if (aid in ib_by_acc) and (aid in ub_by_acc) and (aid in mv_by_acc):
            if abs((ib + mv) - ub) > 1e-6:
                discrepancies.append({"AccountID": aid, "ib_plus_movement": ib + mv, "ub": ub, "diff": (ib + mv) - ub})

        accounts_rows_out.append((aid, nm, ib, mv, ub))
        if abs(ib) > 1e-9 or abs(mv) > 1e-9 or abs(ub) > 1e-9:
            tb_rows.append((aid, nm, ib, mv, ub))

    # 5) Skriv Excel (TrialBalance først)
    xlsx_path = csv_dir / "trial_balance.xlsx"
    wb = xlsxwriter.Workbook(str(xlsx_path))
    fmt_head = wb.add_format({"bold": True})
    fmt_amt  = wb.add_format({"num_format": "#,##0.00"})

    # TrialBalance (første ark)
    ws_tb = wb.add_worksheet("TrialBalance")
    ws_tb.write_row(0, 0, ["AccountID", "AccountDescription", "IB", "Movement", "UB"], fmt_head)
    for r, (aid, nm, ib, mv, ub) in enumerate(tb_rows, start=1):
        ws_tb.write(r, 0, aid)
        ws_tb.write(r, 1, nm)
        ws_tb.write_number(r, 2, ib, fmt_amt)
        ws_tb.write_number(r, 3, mv, fmt_amt)
        ws_tb.write_number(r, 4, ub, fmt_amt)
    ws_tb.freeze_panes(1, 0)
    ws_tb.autofilter(0, 0, max(1, len(tb_rows)), 4)
    ws_tb.set_column(0, 0, 12); ws_tb.set_column(1, 1, 40); ws_tb.set_column(2, 4, 16)

    # Accounts (full kontoplan)
    ws_acc = wb.add_worksheet("Accounts")
    ws_acc.write_row(0, 0, ["AccountID", "AccountDescription", "IB", "Movement", "UB"], fmt_head)
    for r, (aid, nm, ib, mv, ub) in enumerate(accounts_rows_out, start=1):
        ws_acc.write(r, 0, aid)
        ws_acc.write(r, 1, nm)
        ws_acc.write_number(r, 2, ib, fmt_amt)
        ws_acc.write_number(r, 3, mv, fmt_amt)
        ws_acc.write_number(r, 4, ub, fmt_amt)
    ws_acc.freeze_panes(1, 0)
    ws_acc.autofilter(0, 0, max(1, len(accounts_rows_out)), 4)
    ws_acc.set_column(0, 0, 12); ws_acc.set_column(1, 1, 40); ws_acc.set_column(2, 4, 16)

    wb.close()

    # 6) Meta med sporbarhet
    meta = {
        "sources": {
            "accounts_csv": str(csv_dir / "accounts.csv"),
            "gl_totals_csv": str(csv_dir / "gl_totals.csv"),
            "transactions_csv": str(csv_dir / "transactions.csv"),
            "accounts_delimiter": acc_delim,
            "gl_totals_delimiter": tot_delim,
        },
        "counts": {
            "accounts_rows_csv": len(accounts_rows),
            "gl_totals_rows": len(totals_rows),
            "accounts_sheet_rows": len(accounts_rows_out),
            "trialbalance_rows": len(tb_rows),
        },
        "ib_sources": ib_src_count,
        "ub_sources": ub_src_count,
        "movement_sources": mv_src_count,
        "discrepancies": discrepancies[:25],  # vis de første 25 for oversikt
        "notes": {
            "principle": "IB/UB tas fra Accounts. Movement prioriteres fra GL‑totals, ellers UB-IB, ellers transactions.",
            "explanation": (
                "TrialBalance viser kun kontoer med ikke‑null IB/Movement/UB. "
                "IB/UB beregnes ALDRI fra GL; de leses kun fra accounts.csv. "
                "Hvis både IB/UB og Movement finnes og UB != IB+Movement, listes dette under 'discrepancies'."
            ),
        }
    }
    (csv_dir / "simple_trial_balance_meta.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    return xlsx_path
