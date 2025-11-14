# app/parsers/saft_subledger_from_gl.py
# -*- coding: utf-8 -*-
"""
Alias/wrapper slik at all eksisterende kode som kaller denne modulen,
automatisk får den nye, utvidede subledgeren (Overview, Summary, Partyless_Details).
"""
from __future__ import annotations
from pathlib import Path

try:
    # Pakkeimport
    from .saft_subledger import make_subledger as _make_subledger  # type: ignore
except Exception:
    # Standalone fallback
    from saft_subledger import make_subledger as _make_subledger  # type: ignore


def make_subledger(outdir: Path, which: str, date_from: str | None = None, date_to: str | None = None):
    path = _make_subledger(outdir, which, date_from=date_from, date_to=date_to)
    return path
