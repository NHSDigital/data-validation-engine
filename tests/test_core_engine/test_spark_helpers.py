"""Tests for UDF helpers."""

# pylint: disable=redefined-outer-name
import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, List, Optional, Union
from uuid import UUID

import pytest
from pydantic import BaseModel
from pydantic.types import condecimal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as st
from pyspark.sql.functions import col
from typing_extensions import Annotated, TypedDict

from dve.core_engine.backends.implementations.spark.spark_helpers import (
    DecimalConfig,
    create_udf,
    get_type_from_annotation,
    object_to_spark_literal,
)

from ..fixtures import spark  # pylint: disable=unused-import

EXPECTED_STRUCT = st.StructType(
    [
        st.StructField("some_key", st.StringType()),
        st.StructField("another_key", st.LongType()),
    ]
)
"""The struct expected to be created from the working struct implementations."""


class StructImpl(TypedDict):
    """An example struct implementation."""

    some_key: str
    another_key: Optional[int]


class StructPydModelImpl(BaseModel):
    """An example struct pydantic model implementation."""

    some_key: str
    another_key: Optional[int]


@dataclass
class StructDataclassImpl:
    """An example struct dataclass implementation."""

    some_key: str
    another_key: Optional[int]


class StructImplNoKeys(TypedDict):
    """An example struct with no keys. This should fail."""


@pytest.mark.parametrize(
    ["annotation", "expected"],
    [
        (str, st.StringType()),
        (int, st.LongType()),
        (float, st.DoubleType()),
        (bytes, st.BinaryType()),
        (bool, st.BooleanType()),
        (dt.date, st.DateType()),
        (dt.datetime, st.TimestampType()),
        (Decimal, st.DecimalType(38, 18)),
        (List[Optional[int]], st.ArrayType(st.LongType())),
        (StructImpl, EXPECTED_STRUCT),
        (StructDataclassImpl, EXPECTED_STRUCT),
        (StructPydModelImpl, EXPECTED_STRUCT),
        (Annotated[Decimal, "something"], st.DecimalType(38, 18)),
        (Annotated[Decimal, DecimalConfig(10, 0)], st.DecimalType(10, 0)),
        (Annotated[str, "some_annotation"], st.StringType()),
        (List[Union[str, None]], st.ArrayType(st.StringType())),
        (condecimal(max_digits=18, decimal_places=8), st.DecimalType(18, 8)),
    ],
)
def test_get_type_from_annotation(annotation: Any, expected: st.DataType):
    """Test that annotations can be converted to the expected Spark DataTypes."""
    assert get_type_from_annotation(annotation) == expected
    assert get_type_from_annotation(Optional[annotation]) == expected


@pytest.mark.parametrize(
    "annotation",
    [
        list,
        dict,
        UUID,
        Any,
        Union[str, int],
        List[None],
        None,
        StructImplNoKeys,
    ],
)
def test_get_type_from_annotation_errors(annotation: Any):
    """Test that errors are raised as expected."""
    with pytest.raises(ValueError):
        get_type_from_annotation(annotation)


@pytest.mark.parametrize(
    ["precision", "scale"],
    [
        (39, 10),
        (10, 11),
        (-1, -2),
        (10, -1),
        (10, -2),
    ],
)
def test_decimal_config_failures(precision: int, scale: int):
    """Ensure that decimal configs can't be created with incompatible values."""
    with pytest.raises(ValueError):
        DecimalConfig(precision, scale)


def test_create_udf(spark: SparkSession):
    """Test the UDFs can be created from Python functions."""
    with pytest.raises(ValueError):  # Lambdas have no annotations.
        create_udf(lambda _: "string")

    with pytest.raises(ValueError):  # A string isn't callable
        create_udf("string")  # type: ignore

    def uppercase(string: Optional[str]) -> Optional[str]:  # pragma: no cover
        """Uppercase a string."""
        if string is None:
            return None
        return string.upper()

    # pylint: disable=invalid-name
    def multiply(x: Optional[int], y: Optional[int]) -> Optional[int]:  # pragma: no cover
        """Multiply two numbers."""
        if x is None or y is None:
            return None
        return x * y

    df: DataFrame = spark.createDataFrame(  # type: ignore
        [
            {"Identifier": "Hat Stand", "Cost": 10, "N_Units": 20},
            {"Identifier": "Table", "Cost": 20, "N_Units": 5},
        ]
    )

    result = df.select(
        create_udf(uppercase)(col("Identifier")).alias("Identifier"),
        create_udf(multiply)(col("Cost"), col("N_Units")).alias("Total_Value"),
    )
    assert set(result.collect()) == {
        st.Row(Identifier="HAT STAND", Total_Value=200),
        st.Row(Identifier="TABLE", Total_Value=100),
    }


@pytest.mark.parametrize(
    "obj",
    [
        "string",
        10,
        4_294_967_296,
        1.0,
        dt.date(2020, 1, 1),
        dt.datetime(2020, 1, 1, 23, 59, 1),
        Decimal("20.0"),
        True,
        False,
        None,
        ["list", "of", "strings"],
        ["list", None, "with", None, "nulls"],
        {"struct": "string", "values": 1, "date": dt.date(1980, 1, 23)},
        [{"struct_key": "string"}, {"struct_key": "value"}],
    ],
)
def test_object_to_spark_literal(obj: Any):
    """Ensure that objects are correctly converted to literals."""
    spark: SparkSession = SparkSession.builder.getOrCreate()
    df = spark.sql("SELECT 1 AS One")

    df = df.select(object_to_spark_literal(obj).alias("obj"))
    assert df.first().asDict(recursive=True)["obj"] == obj


@pytest.mark.parametrize(
    "obj",
    [
        ["heterogenous", 1, "list"],
        [{"struct_key": "string"}, {"struct_key_2": "another_string"}],
    ],
)
def test_object_to_spark_literal_blocks_some_footguns(obj: Any):
    """
    Ensure that some objects correctly raise ValueErrors as they
    cannot be converted to literals.

    """
    with pytest.raises(ValueError):
        object_to_spark_literal(obj)
