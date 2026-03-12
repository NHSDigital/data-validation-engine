"""Test Spark readers"""

# pylint: disable=W0621
# pylint: disable=C0116
# pylint: disable=C0103
# pylint: disable=C0115

import tempfile
from pathlib import Path

import polars as pl
import pytest
from pydantic import BaseModel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from dve.core_engine.backends.implementations.spark.readers.csv import SparkCSVReader


class SparkCSVTestModel(BaseModel):
    test_col: str


@pytest.fixture
def spark_null_csv_resource():
    test_df = pl.DataFrame({"test_col": ["fine", " ", "    "]})

    with tempfile.TemporaryDirectory() as tdir:
        resource_uri = Path(tdir, "test_spark_csv_reader.csv").as_posix()
        test_df.write_csv(resource_uri, include_header=True, quote_style="always")

        yield resource_uri


def test_SparkCSVReader_clean_empty_strings(spark: SparkSession, spark_null_csv_resource):
    resource_uri = spark_null_csv_resource
    expected_df = spark.createDataFrame(
        [
            Row(
                test_col="fine",
            ),
            Row(
                test_col=None,
            ),
            Row(test_col=None),
        ],
        StructType([StructField("test_field", StringType())]),
    )

    reader = SparkCSVReader(null_empty_strings=True, spark_session=spark)

    result_df: DataFrame = reader.read_to_dataframe(
        resource=resource_uri, entity_name="test", schema=SparkCSVTestModel
    )

    assert result_df.exceptAll(expected_df).count() == 0
