import sys
from pathlib import Path

import pandas as pd
import pytest

# Sørg for at prosjektroten (der parsers/ ligger) er på sys.path
ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

from parsers.common import (  # type: ignore[import]
    norm_acc,
    norm_acc_series,
    has_value,
    to_numeric_series,
    to_numeric_df,
    range_dates,
)


def test_norm_acc_happy_path():
    # Fjerner ikke-numeriske tegn og ledende nuller
    assert norm_acc("00123") == "123"
    assert norm_acc("  0000  ") == "0"
    assert norm_acc("ACC-1500") == "1500"


def test_norm_acc_edge_cases():
    # None og tomme/ikke-siffer skal gi tom streng
    assert norm_acc(None) == ""
    assert norm_acc("") == ""
    assert norm_acc("abc") == ""


def test_norm_acc_series_vectorised():
    s = pd.Series(["001", "020", "300"])
    result = norm_acc_series(s)
    assert list(result) == ["1", "20", "300"]


def test_has_value():
    s = pd.Series(["", "  ", "a", None])
    mask = has_value(s)
    # Dagens implementasjon: alt som ikke er tom streng blir True (inkl. None)
    assert mask.tolist() == [False, False, True, True]


def test_to_numeric_helpers():
    s = pd.Series(["1", "2.5", "x"])
    num = to_numeric_series(s)
    # "x" blir 0.0, resten konverteres
    assert num.tolist() == [1.0, 2.5, 0.0]

    df = pd.DataFrame({"a": ["1", "x"], "b": ["3.3", "4.4"]})
    out = to_numeric_df(df, ["a", "b"])
    assert out["a"].tolist() == [1.0, 0.0]
    assert out["b"].tolist() == [3.3, 4.4]


def test_range_dates_header_and_tx():
    # Transaksjoner fra 2024-01-05 til 2024-02-10
    tx = pd.DataFrame(
        {"Date": ["2024-01-05", "2024-02-10", None]},
    )
    header = pd.DataFrame(
        {
            "StartDate": ["2024-01-01"],
            "EndDate": ["2024-12-31"],
        }
    )

    d_min, d_max = range_dates(header, None, None, tx)
    assert str(d_min.date()) == "2024-01-01"
    assert str(d_max.date()) == "2024-12-31"


def test_range_dates_manual_override_and_swap():
    # Transaksjoner har ulogisk rekkefølge; date_from/date_to skal styre
    tx = pd.DataFrame({"Date": ["2024-12-31", "2024-01-01"]})

    d_min, d_max = range_dates(None, "2024-03-01", "2024-02-01", tx)
    # Funksjonen bytter om dersom slutt < start
    assert str(d_min.date()) == "2024-02-01"
    assert str(d_max.date()) == "2024-03-01"
