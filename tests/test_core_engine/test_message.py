"""Tests for feedback messages."""

from string import ascii_letters

import pytest

from dve.core_engine.constants import ROWID_COLUMN_NAME
from dve.core_engine.message import FeedbackMessage


def test_rowid_column_stripped():
    """Ensure that the rowID column is stripped from FeedbackMessages."""

    message = FeedbackMessage(
        entity="entity", record={"key": "value", ROWID_COLUMN_NAME: "some identifier"}
    )

    assert message.record.get(ROWID_COLUMN_NAME) is None


@pytest.mark.parametrize(
    ("derived_column", "expected"),
    [
        ([{"sub_key": "sub_value"}], "sub_value"),
        ([], ""),
        (
            [{"sub_key": "sub_value"}, {"sub_key": "sub_value2"}, {"sub_key": "sub_value3"}],
            "sub_value\nsub_value2\nsub_value3",
        ),
        (
            [{"sub_key": "sub_value"}] * 20,
            "sub_value\n" * 10 + "only first 10 shown",
        ),
        (["a", "b", "c"], "a\nb\nc"),
        (list(ascii_letters), "A\nB\nC\nD\nE\nF\nG\nH\nI\nJ\nonly first 10 shown"),
    ],
)
def test_values_from_derived_column(derived_column, expected):
    """test to ensure that values within a derived column can be pulled out and reported on
    an example is where a column containing all the sub records is created, this should then
    pulled out into the report
    """

    message = FeedbackMessage(
        entity="entity",
        record={"key": "value", "derived_column": derived_column},
        reporting_field="sub_key",
        error_location="derived_column",
    )

    assert message.to_dict(max_number_of_values=10, value_separator="\n")["Value"] == expected


def test_values_from_regular_column():
    """test to ensure that values within a derived column can be pulled out and reported on
    an example is where a column containing all the sub records is created, this should then
    pulled out into the report
    """

    message = FeedbackMessage(
        entity="entity",
        record={"key": "value", "derived_column": [{"sub_key": "sub_value"}]},
        reporting_field="key",
        error_location="derived_column",
    )

    assert message.to_dict()["Value"] == "value"


@pytest.mark.parametrize("reporting_field", (["key", "key2"], "['key', 'key2']"))
def test_values_with_multiple_reporting_fields(reporting_field):
    """test to ensure that when 2 reporting fields are provided that the values are pulled out correctly"""

    message = FeedbackMessage(
        entity="entity",
        record={"key": "value", "key2": "value2", "derived_column": [{"sub_key": "sub_value"}]},
        reporting_field=reporting_field,
        error_location="Filter test filter",
    )

    assert message.to_dict()["Value"] == "key2=value2, key=value"


@pytest.mark.parametrize("value", (["value"], ["value", "value2", "value3"]))
def test_values_with_list_of_values(value):
    """test to ensure that values within a list are pulled out correctly"""

    message = FeedbackMessage(
        entity="entity",
        record={"key": value, "key2": "value2", "derived_column": [{"sub_key": "sub_value"}]},
        reporting_field="key",
        error_location="Filter test filter",
    )

    assert message.to_dict()["Value"] == ", ".join(value)


@pytest.mark.parametrize("sep", (",", "\n", "hello"))
def test_value_separators(sep):
    """test to ensure that values within a list are pulled out correctly"""

    value = ["value", "value2", "value3"]
    message = FeedbackMessage(
        entity="entity",
        record={
            "key": value,
            "key2": "value2",
            "derived_column": [{"sub_key": "sub_value"}],
        },
        reporting_field="key",
        error_location="Filter test filter",
    )

    assert message.to_dict(value_separator=sep)["Value"] == sep.join(value)


@pytest.mark.parametrize(("max_value", "expected_len"), ((None, 3), (3, 3), (5, 4)))
def test_max_values(max_value, expected_len):
    """test to ensure that values within a list are pulled out correctly"""

    if max_value is None:
        multiple = 3
    else:
        multiple = max_value
    value = ["value"] * multiple
    message = FeedbackMessage(
        entity="entity",
        record={
            "key": value,
            "key2": "value2",
            "derived_column": [{"sub_key": "sub_value"}],
        },
        reporting_field="key",
        error_location="Filter test filter",
    )

    val = message.to_dict(max_number_of_values=3)["Value"].split(", ")
    assert len(val) == expected_len
    if multiple != expected_len:
        # make sure message that only the first n were shown
        assert "only first 3" in val[-1]
