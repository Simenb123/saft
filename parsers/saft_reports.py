
# src/app/parsers/saft_reports.py
# -*- coding: utf-8 -*-

from __future__ import annotations
from pathlib import Path

# Opsjonelle moduler (GUI-kall holder selv om noe mangler)
try:
    from . import saft_general_ledger
except Exception:
    saft_general_ledger = None

try:
    from . import saft_trial_balance
except Exception:
    saft_trial_balance = None

from . import saft_subledger
from . import saft_vat_report
from . import saft_gl_monthly


def make_general_ledger(out_dir: Path) -> Path:
    out_dir = Path(out_dir)
    if saft_general_ledger is None:
        print("[excel] (advarsel) saft_general_ledger-modul ikke funnet – hopper over.")
        return out_dir
    print("[excel] Kaller app.parsers.saft_reports.make_general_ledger")
    return saft_general_ledger.make_general_ledger(out_dir)


def make_trial_balance(out_dir: Path) -> Path:
    out_dir = Path(out_dir)
    if saft_trial_balance is None:
        print("[excel] (advarsel) saft_trial_balance-modul ikke funnet – hopper over.")
        return out_dir
    print("[excel] Kaller app.parsers.saft_reports.make_trial_balance")
    return saft_trial_balance.make_trial_balance(out_dir)


def make_subledger(out_dir: Path, which: str) -> Path:
    """
    Etter AP-subledger genereres i tillegg:
      - MVA-rapport (vat_report.xlsx)
      - GL monthly (gl_monthly.xlsx)
    Begge pakkes i try/except så feil påvirker ikke subledger.
    """
    out_dir = Path(out_dir)
    which_u = (which or "").upper()
    print(f"[excel] Kaller app.parsers.saft_reports.make_subledger({out_dir!s}, '{which_u}')")
    out_path = saft_subledger.make_subledger(out_dir, which_u)

    if which_u == "AP":
        try:
            saft_vat_report.make_vat_report(out_dir)
        except Exception as e:
            print(f"[excel] (advarsel) VAT-rapport feilet: {e!s}")
        try:
            saft_gl_monthly.make_gl_monthly(out_dir)
        except Exception as e:
            print(f"[excel] (advarsel) GL Monthly feilet: {e!s}")

    return out_path
