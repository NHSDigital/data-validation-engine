import tempfile
from pathlib import Path

from dve.core_engine.backends.implementations.spark.readers import SparkParquetReader
from dve.core_engine.backends.utilities import stringify_model

import pyspark.sql.functions as psf
import pyspark.sql.types as pst
import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from ..reader_utils import TestParquetModel


@pytest.fixture(scope="module")
def test_spark_parquet(spark: SparkSession):
    with tempfile.TemporaryDirectory() as tdir:
        schema = pst.StructType(
            [
                pst.StructField("col1", pst.StringType()),
                pst.StructField("col2", pst.IntegerType()),
            ]
        )
        df = spark.createDataFrame(
            [
                Row(
                    col1="abc",
                    col2=123,
                ),
            ],
            schema,
        )
        write_path = Path(tdir, "test_parquet.parquet")
        df.coalesce(1).write.parquet(write_path.as_posix())

        yield write_path, schema


def test_spark_parquet_read_all_defaults(test_spark_parquet):
    test_file_path, _expected_schema = test_spark_parquet

    reader = SparkParquetReader()
    df: DataFrame = reader.read_to_dataframe(test_file_path.as_posix(), "test", TestParquetModel)
    ignore_fields = ["__record_index__"]
    df_fields = [f for f in df.schema.fields if f.name not in ignore_fields]

    assert df.count() == 1
    assert df_fields == _expected_schema.fields
