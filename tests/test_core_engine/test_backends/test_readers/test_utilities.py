"""Test utility functions & objects for readers"""

from pydantic import BaseModel

from dve.core_engine.backends.readers.utilities import get_all_model_fields


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
