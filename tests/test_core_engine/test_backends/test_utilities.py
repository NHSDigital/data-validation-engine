"""
Test backend utility functions
"""

# pylint: disable=C0115,C0116

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import BaseModel

from dve.core_engine.backends.utilities import is_field_complex, stringify_model
from dve.metadata_parser.domain_types import FormattedDatetime


def model_to_schema(mdl: BaseModel) -> dict[str, Any]:
    schema = {}
    for field_name, field_md in mdl.model_fields.items():
        if issubclass(field_md.annotation, BaseModel):
            schema[field_name] = model_to_schema(field_md.annotation)
        else:
            schema[field_name] = field_md.annotation

    return schema


class AnotherTestModel(BaseModel):
    test_field: str


class TestModel(BaseModel):
    simple_str: str
    simple_int: int
    simple_bool: bool
    simple_list: list[str]
    simple_dict: dict[str, Any]
    simple_set: set[str]
    simple_tuple: tuple[int, str]
    another_model: AnotherTestModel
    date_example: date
    test_fdatetime: FormattedDatetime


class ChildMultiTypeModel(BaseModel):
    child_test_int: int


class MultiTypeModel(BaseModel):
    test_str: str
    test_int: int
    test_dict: dict[str, int]
    test_list: list[int]
    test_decimal: Decimal
    test_date: date
    test_datetime: datetime
    test_fdatetime: FormattedDatetime
    test_child_model: ChildMultiTypeModel


TEST_MODEL = TestModel(
    simple_str="abc",
    simple_int=123,
    simple_bool=True,
    simple_list=["apple", "banana", "orange",],
    simple_dict={"key1": "abc"},
    simple_set={"a", "b", "c"},
    simple_tuple=(1, "wow"),
    another_model=AnotherTestModel(test_field="lemon"),
    date_example=date(2026,1,1),
    test_fdatetime="2026-01-01T12:30:10",
)


@pytest.mark.parametrize(
    "field_name, expected",
    [
        ("simple_str", False),
        ("simple_int", False),
        ("simple_bool", False),
        ("simple_list", True),
        ("simple_dict", True),
        ("simple_set", True),
        ("simple_tuple", True),
        ("another_model", True),
        ("test_fdatetime", False),
    ]
)
def test_is_field_complex(field_name: str, expected: bool):
    _field = TEST_MODEL.model_fields[field_name]
    _res = is_field_complex(_field)
    assert _res == expected, f"Expected {expected}. Got {_res}."


def test_stringify_model():
    expected_dict = {
        "test_str": str,
        "test_int": str,
        "test_dict": dict[str, str],
        "test_list": list[str],
        "test_decimal": str,
        "test_date": str,
        "test_datetime": str,
        "test_fdatetime": str,
        "test_child_model": {
            "child_test_int": str
        },
    }
    serialised_model_result = model_to_schema(stringify_model(MultiTypeModel))

    assert expected_dict == serialised_model_result
