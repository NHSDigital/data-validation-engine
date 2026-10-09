"""Test utility functions & objects for readers"""

from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from pydantic import BaseModel
import pytest

from dve.core_engine.backends.readers.utilities import get_all_model_fields

@pytest.fixture
def temp_dir():
    with TemporaryDirectory(prefix="ddb_test_json_reader") as temp_dir:
        yield Path(temp_dir)

class Model1(BaseModel):  # pylint: disable=C0115
    model1_field_1: str
    model1_field_2: int

class Model2(BaseModel):  # pylint: disable=C0115
    model2_field_1: str


def test_get_all_model_fields():
    """Test get_all_model_fields returns a unique set of fields from multiple models"""
    md1 = Model1(model1_field_1="hello", model1_field_2=123)
    md2 = Model2(model2_field_1="world")

    result = get_all_model_fields([md1, md2])

    assert result == {"model1_field_1", "model1_field_2", "model2_field_1"}
