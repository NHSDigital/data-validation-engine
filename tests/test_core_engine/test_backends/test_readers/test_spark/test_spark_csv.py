"""Test Spark readers"""

# pylint: disable=W0621
# pylint: disable=C0116
# pylint: disable=C0103
# pylint: disable=C0115

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from dve.core_engine.backends.implementations.spark.readers.csv import SparkCSVReader
from dve.core_engine.backends.utilities import stringify_model
from tests.test_core_engine.test_backends.test_readers.fixtures import (
    temp_dir,
    temp_csv_with_null_strings,
    temp_csv_with_null_records
)


def test_SparkCSVReader_clean_empty_strings(spark: SparkSession, temp_csv_with_null_strings):
    resource_uri, mdl = temp_csv_with_null_strings
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
        resource=resource_uri, entity_name="test", schema=stringify_model(mdl)
    )

    assert result_df.exceptAll(expected_df).count() == 0

def test_SparkCSVReader_remove_null_records(spark, temp_csv_with_null_records):
    uri, mdl = temp_csv_with_null_records
    
    reader = SparkCSVReader(null_empty_strings=True, spark_session=spark)

    result_df: DataFrame = reader.read_to_entity_type(
        entity_type=DataFrame, resource=uri, entity_name="test", schema=stringify_model(mdl)
    )

    assert result_df.count() == 2
    assert [rw.id_field for rw in result_df.select("id_field").collect()] == ["1", "2"]
    
