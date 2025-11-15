import sys
from pathlib import Path

import pandas as pd
import pytest

# Sørg for at prosjektroten (der parsers/ ligger) er på sys.path
ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from parsers.saft_vat_report import _norm_type  # type: ignore[import]


def test_norm_type_text_variants():
    assert _norm_type("IN") == "IN"
    assert _norm_type("input") == "IN"
    assert _norm_type("inngående") == "IN"
    assert _norm_type("OUT") == "OUT"
    assert _norm_type("sales") == "OUT"
    assert _norm_type("utgående") == "OUT"


def test_norm_type_handles_none_and_float_without_crash():
    # Tidligere feilet dette med: 'float' object has no attribute 'strip'
    assert _norm_type(None) == ""
    assert _norm_type(float("nan")) == ""
    # Andre tall blir bare konvertert til tekst uten feil
    assert _norm_type(25.0) == "25.0"
