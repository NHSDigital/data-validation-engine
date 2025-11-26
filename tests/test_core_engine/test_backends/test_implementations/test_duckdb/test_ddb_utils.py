from typing import Dict, List
import pytest

from dve.core_engine.backends.implementations.duckdb.utilities import (
    expr_mapping_to_columns,
    expr_array_to_columns,
)


@pytest.mark.parametrize(
    ["expressions", "expected"],
    [
        (
            {"size(array_field)": "field_length", "another_field": "rename_another_field"},
            ["size(array_field) as field_length", "another_field as rename_another_field"],
        ),
    ],
)
def test_expr_mapping_to_columns(expressions: Dict[str, str], expected: list[str]):
    observed = expr_mapping_to_columns(expressions)
    assert observed == expected


@pytest.mark.parametrize(
    ["expressions", "expected"],
    [
        (
            [
                "a_field",
                "another_field as renamed",
                "struct(a_field, another_field) as struct_field",
            ],
            [
                "a_field",
                "another_field as renamed",
                "struct(a_field, another_field) as struct_field",
            ],
        ),
        (
            [
                "size(array_field)",
                "another_field as rename_another_field",
                "a_dynamic_field, another_dynamic_field",
            ],
            [
                "size(array_field)",
                "another_field as rename_another_field",
                "a_dynamic_field",
                "another_dynamic_field",
            ],
        ),
    ],
)
def test_expr_array_to_columns(expressions: Dict[str, str], expected: list[str]):
    observed = expr_array_to_columns(expressions)
    assert observed == expected
