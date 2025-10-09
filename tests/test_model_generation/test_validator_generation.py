"""Tests for dynamic validator generation"""

# pylint: disable=redefined-outer-name
import math

import pytest

from dve.metadata_parser.function_library import _nullcheck


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("Hello", "HELLO"),
        (None, None),
        ("   ", None),
        (0, "0"),
        (math.nan, "NAN"),
    ],
)
def test_nullchecker(value, expected):
    assert _nullcheck(lambda x: str(x).upper())(value) == expected
