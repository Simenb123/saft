# -*- coding: utf-8 -*-
"""Init-modul for parsers-pakken.

Eksponerer noen høy-nivå funksjoner slik at de kan importeres direkte fra
`parsers`-pakken:

    from parsers import make_trial_balance, make_subledger
"""

from __future__ import annotations

from .saft_reports import (
    make_subledger,
    make_trial_balance,
    make_general_ledger,
)

__all__ = ["make_subledger", "make_trial_balance", "make_general_ledger"]
