"""Test objects in dve.reporting.utility"""
# pylint: disable=missing-function-docstring

import polars as pl

from dve.core_engine.backends.utilities import pl_row_count
from dve.reporting.utils import extract_and_pivot_keys


def test_extract_and_pivot_keys():
    df = pl.DataFrame({
        "entity": ["test1", "test2", "test3", "test4"],
        "FailureType": ["submission1", "submission2", "submission3", "submission4"],
        "id": [
            "Key1: Value1 -- Key2: Value2 -- Key3: Value3",
            "Key1: Value1 -- Key2: Value2",
            "",
            None,
        ]
    })
    result_df = extract_and_pivot_keys(df, key_field="id")
    expected_df = pl.DataFrame({
        "entity": ["test1", "test2", "test3", "test4"],
        "FailureType": ["submission1", "submission2", "submission3", "submission4"],
        "Key1_Identifier": ["Value1", "Value1", None, None],
        "Key2_Identifier": ["Value2", "Value2", None, None],
        "Key3_Identifier": ["Value3", None, None, None],
    })

    assert pl_row_count(result_df) == pl_row_count(df)
    assert result_df.equals(expected_df)


def test_extract_and_pivot_keys_with_empty_key_field():
    df = pl.DataFrame({
        "entity": ["test1", "test2", "test3"],
        "FailureType": ["submission1", "submission2", "submission3"],
        "Key": ["", "None", None]
    })
    result_df = extract_and_pivot_keys(df)

    assert pl_row_count(result_df) == pl_row_count(df)
    assert result_df.equals(df)
