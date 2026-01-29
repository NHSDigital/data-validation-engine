import tempfile
import datetime as dt
from pathlib import Path
from uuid import uuid4
from pydantic import BaseModel, create_model
import pytest

from dve.core_engine.backends.implementations.duckdb.utilities import (
    expr_mapping_to_columns,
    expr_array_to_columns,
    check_csv_header_expected,
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
def test_expr_mapping_to_columns(expressions: dict[str, str], expected: list[str]):
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
def test_expr_array_to_columns(expressions: dict[str, str], expected: list[str]):
    observed = expr_array_to_columns(expressions)
    assert observed == expected


@pytest.mark.parametrize(
    ["header_row", "delim", "schema", "expected"],
    [
        (
            "field1,field2,field3",
            ",",
            {"field1": (str, ...), "field2": (int, ...), "field3": (float, 1.2)},
            set(),
        ),
        (
            "field2,field3,field1",
            ",",
            {"field1": (str, ...), "field2": (int, ...), "field3": (float, 1.2)},
            set(),
        ),
        (
            "str_field|int_field|date_field|",
            ",",
            {"str_field": (str, ...), "int_field": (int, ...), "date_field": (dt.date, dt.date.today())},
            {"str_field","int_field","date_field"},
        ),
        (
            '"str_field"|"int_field"|"date_field"',
            "|",
            {"str_field": (str, ...), "int_field": (int, ...), "date_field": (dt.date, dt.date.today())},
            set(),
        ),
        
    ],
)
def test_check_csv_header_expected(
    header_row: str, delim: str, schema: type[BaseModel], expected: set[str]
):
    mdl = create_model("TestModel", **schema)
    with tempfile.TemporaryDirectory() as tmpdir:
        fle = Path(tmpdir).joinpath(f"test_file_{uuid4().hex}.csv")
        fle.open("w+").write(header_row)
        res = check_csv_header_expected(fle.as_posix(), mdl, delim)
    assert res == expected
