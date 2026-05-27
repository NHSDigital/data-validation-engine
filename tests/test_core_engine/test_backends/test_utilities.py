from datetime import date
from typing import Any

import pytest
from pydantic import BaseModel

from dve.core_engine.backends.utilities import is_field_complex


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


TEST_MODEL = TestModel(
    simple_str="abc",
    simple_int=123,
    simple_bool=True,
    simple_list=["apple", "banana", "orange",],
    simple_dict={"key1": "abc"},
    simple_set={"a", "b", "c"},
    simple_tuple=(1, "wow"),
    another_model=AnotherTestModel(test_field="lemon"),
    date_example=date(2026,1,1)
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
    ]
)
def test_is_field_complex(field_name: str, expected: bool):
    _field = TEST_MODEL.model_fields[field_name]
    _res = is_field_complex(_field)
    assert _res == expected, f"Expected {expected}. Got {_res}."
