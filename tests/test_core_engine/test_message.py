"""Tests for feedback messages."""

from datetime import date
import json
from string import ascii_letters
from typing import Dict, Optional

from pydantic import BaseModel, ValidationError
import pytest

from dve.core_engine.constants import ROWID_COLUMN_NAME
from dve.core_engine.message import DEFAULT_ERROR_DETAIL, DataContractErrorDetail, FeedbackMessage


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


def test_from_pydantic_error():
    
    bad_val_default = DEFAULT_ERROR_DETAIL.get("Bad value")
    blank_default = DEFAULT_ERROR_DETAIL.get("Blank")
    
    class TestModel(BaseModel):
        idx: int
        other_field: str
        
    _bad_value_data = {"idx": "ABC", "other_field": 123}
    _blank_value_data = {"other_field": "hi"}
    
    try:
        TestModel(**_bad_value_data)
    except ValidationError as e:
        _error_bad_value = e
    
    try:
        TestModel(**_blank_value_data)
    except ValidationError as e:
        _error_blank = e
           
    msgs_bad= FeedbackMessage.from_pydantic_error(entity="test_entity",
                                                      record = _bad_value_data,
                                                      error=_error_bad_value)
       
    assert len(msgs_bad) == 1
    assert msgs_bad[0].error_code == bad_val_default.error_code
    assert msgs_bad[0].error_message == bad_val_default.error_message
    
    msgs_blank = FeedbackMessage.from_pydantic_error(entity="test_entity",
                                                      record = _blank_value_data,
                                                      error=_error_blank)
    
    assert len(msgs_blank) == 1
    assert msgs_blank[0].error_code == blank_default.error_code
    assert msgs_blank[0].error_message == blank_default.error_message

def test_from_pydantic_error_custom_error_details():
    
    bad_val_default = DEFAULT_ERROR_DETAIL.get("Bad value")
    blank_default = DEFAULT_ERROR_DETAIL.get("Blank")
    class TestModel(BaseModel):
        idx: int
        str_field: str
        date_field: Optional[date]
        unimportant_field: Optional[int]
        
    custom_error_details: str = """
    {"idx": {"Blank": {"error_code": "IDBLANKERRCODE",
                      "error_message": "idx is a mandatory field"},
            "Bad value": {"error_code": "IDDODGYVALCODE",
                          "error_message": "idx value is dodgy: {{idx}}"}},
     "date_field": {"Bad value": {"error_code": "DATEDODGYVALCODE",
                                  "error_message": "date_field value is dodgy: idx: {{idx}}, date_field: {{date_field}}"}}}    
    """
    error_details: Dict[str, Dict[str, DataContractErrorDetail]] = {field: {err_type: DataContractErrorDetail(**detail) 
                                                                            for err_type, detail in err_details.items()} 
                                                                    for field, err_details in json.loads(custom_error_details).items()}
        
    _bad_value_data = {"idx": "ABC", "str_field": "test", "date_field": "terry", "unimportant_field": "dog"}
    _blank_value_data = {}
    
    try:
        TestModel(**_bad_value_data)
    except ValidationError as e:
        _error_bad_value = e
    
    try:
        TestModel(**_blank_value_data)
    except ValidationError as e:
        _error_blank = e
           
    msgs_bad= FeedbackMessage.from_pydantic_error(entity="test_entity",
                                                  record = _bad_value_data,
                                                  error=_error_bad_value,
                                                  error_details=error_details)
    
    msgs_bad = sorted(msgs_bad, key=lambda x: x.error_location)
       
    assert len(msgs_bad) == 3
    assert msgs_bad[0].error_code == error_details.get("date_field").get("Bad value").error_code
    assert msgs_bad[0].error_message == error_details.get("date_field").get("Bad value").template_message(_bad_value_data)
    assert msgs_bad[1].error_code == error_details.get("idx").get("Bad value").error_code
    assert msgs_bad[1].error_message == error_details.get("idx").get("Bad value").template_message(_bad_value_data)
    assert msgs_bad[2].error_code == bad_val_default.error_code
    assert msgs_bad[2].error_message == bad_val_default.error_message
    
    msgs_blank = FeedbackMessage.from_pydantic_error(entity="test_entity",
                                                     record = _blank_value_data,
                                                     error=_error_blank,
                                                     error_details=error_details)
    
    
    msgs_blank = sorted(msgs_blank, key=lambda x: x.error_location)
     
    assert len(msgs_blank) == 2
    assert msgs_blank[0].error_code == error_details.get("idx").get("Blank").error_code
    assert msgs_blank[0].error_message == error_details.get("idx").get("Blank").template_message(_blank_value_data)
    assert msgs_blank[1].error_code == blank_default.error_code
    assert msgs_blank[1].error_message == blank_default.error_message
