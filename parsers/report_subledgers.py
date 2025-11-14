# app/parsers/report_subledgers.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
from typing import Optional, Callable, Dict, List, Tuple
import importlib
import inspect

def _resolve(mods: List[str], fn_name: str) -> Tuple[Callable | None, str | None]:
    for m in mods:
        try:
            mod = importlib.import_module(m)
        except Exception:
            continue
        fn = getattr(mod, fn_name, None)
        if callable(fn):
            return fn, f"{m}.{fn_name}"
    return None, None

def _with_dates(fn: Callable, out_dir: Path, date_from: Optional[str], date_to: Optional[str]):
    try:
        sig = inspect.signature(fn)
        kwargs: Dict[str, str] = {}
        for a, v in (("date_from", date_from), ("date_to", date_to),
                     ("from_date", date_from), ("to_date", date_to),
                     ("start_date", date_from), ("end_date", date_to),
                     ("dfrom", date_from), ("dto", date_to)):
            if v is not None and a in sig.parameters and a not in kwargs:
                kwargs[a] = v
        return fn(Path(out_dir), **kwargs)
    except TypeError:
        return fn(Path(out_dir))

def make_subledger(out_dir: Path, which: str,
                   date_from: Optional[str] = None, date_to: Optional[str] = None) -> Path:
    """
    Low risk: bare rute inn til din eksisterende implementasjon.
    Foretrekk subledgers.make_subledger (full), ellers evt. eldre modulnavn.
    """
    which_u = (which or "").upper()
    if which_u not in {"AR", "AP"}:
        raise ValueError("which må være 'AR' eller 'AP'")

    impl, _ = _resolve(
        ["app.parsers.subledgers", "parsers.subledgers", "subledgers",
         "app.parsers.saft_subledger", "parsers.saft_subledger", "saft_subledger"],
        "make_subledger"
    )
    if impl is None:
        raise ImportError("Fant ikke make_subledger i subledgers/saft_subledger.")

    sig = inspect.signature(impl)
    kwargs: Dict[str, str] = {}
    for a, v in (("date_from", date_from), ("date_to", date_to),
                 ("from_date", date_from), ("to_date", date_to),
                 ("start_date", date_from), ("end_date", date_to),
                 ("dfrom", date_from), ("dto", date_to)):
        if v is not None and a in sig.parameters and a not in kwargs:
            kwargs[a] = v

    if "which" in sig.parameters:
        return impl(Path(out_dir), which=which_u, **kwargs)  # type: ignore[misc]
    else:
        return impl(Path(out_dir), which_u, **kwargs)        # type: ignore[misc]

# (valgfritt) bro for GL/TB hvis subledgers inneholder dem
def make_general_ledger(out_dir: Path,
                        date_from: Optional[str] = None, date_to: Optional[str] = None) -> Path:
    impl, _ = _resolve(
        ["app.parsers.subledgers", "parsers.subledgers", "subledgers",
         "app.parsers.saft_general_ledger", "parsers.saft_general_ledger", "saft_general_ledger"],
        "make_general_ledger"
    )
    if impl is None:
        raise ImportError("Fant ikke make_general_ledger i subledgers/saft_general_ledger.")
    return _with_dates(impl, Path(out_dir), date_from, date_to)

def make_trial_balance(out_dir: Path,
                       date_from: Optional[str] = None, date_to: Optional[str] = None) -> Path:
    impl, _ = _resolve(
        ["app.parsers.subledgers", "parsers.subledgers", "subledgers",
         "app.parsers.saft_trial_balance", "parsers.saft_trial_balance", "saft_trial_balance"],
        "make_trial_balance"
    )
    if impl is None:
        raise ImportError("Fant ikke make_trial_balance i subledgers/saft_trial_balance.")
    return _with_dates(impl, Path(out_dir), date_from, date_to)
