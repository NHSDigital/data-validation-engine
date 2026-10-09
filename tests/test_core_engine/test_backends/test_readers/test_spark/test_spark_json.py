from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import pytest
from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StructType, StructField, StringType 

from dve.core_engine.backends.implementations.spark.spark_helpers import (
    get_type_from_annotation,
)
from dve.core_engine.backends.implementations.spark.readers.json import SparkJSONReader
from dve.core_engine.backends.utilities import stringify_model
from dve.core_engine.constants import RECORD_INDEX_COLUMN_NAME
from tests.test_core_engine.test_backends.test_readers.fixtures import (
    temp_dir,
    temp_json_file,
    temp_json_file_w_null_recs
)


def test_spark_json_reader_all_str(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.model_fields] + [RECORD_INDEX_COLUMN_NAME]
    reader = SparkJSONReader()
    df: DataFrame = reader.read_to_entity_type(
        DataFrame, uri.as_posix(), "test", stringify_model(mdl)
    )
    assert df.columns == expected_fields
    assert df.schema == StructType([StructField(nme, StringType() if not nme == RECORD_INDEX_COLUMN_NAME else LongType()) for nme in expected_fields])
    assert [rw.asDict() for rw in df.collect()] == [{**{k: str(v) for k, v in rw.items()}, RECORD_INDEX_COLUMN_NAME: idx} for idx, rw in enumerate(data, start=1)]

def test_spark_json_reader_cast(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.model_fields] + [RECORD_INDEX_COLUMN_NAME]
    reader = SparkJSONReader()
    df: DataFrame = reader.read_to_entity_type(DataFrame, uri.as_posix(), "test", mdl)
    
    assert df.columns == expected_fields
    assert df.schema == StructType([StructField(name, get_type_from_annotation(fld.annotation)) 
                                    for name, fld in mdl.model_fields.items()] + [StructField(RECORD_INDEX_COLUMN_NAME, get_type_from_annotation(int))])
    assert [rw.asDict() for rw in df.collect()] == [{**rw, RECORD_INDEX_COLUMN_NAME: idx} for idx, rw in enumerate(data, start=1)]


def test_spark_json_write_parquet(spark, temp_json_file):
    uri, _, mdl = temp_json_file
    reader = SparkJSONReader()
    df: DataFrame = reader.read_to_entity_type(
        DataFrame, uri.as_posix(), "test", stringify_model(mdl)
    )
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(df, target_loc)
    parquet_df = spark.read.parquet(target_loc)
    assert parquet_df.collect() == df.collect()

def test_spark_json_write_parquet_py_iterator(spark, temp_json_file):
    uri, _, mdl = temp_json_file
    reader = SparkJSONReader()
    data = list(reader.read_to_py_iterator(uri.as_posix(), "test", stringify_model(mdl)))
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(spark.createDataFrame(data), target_loc)
    parquet_data = sorted([rw.asDict() for rw
                           in spark.read.parquet(target_loc).collect()],
                          key= lambda x: x.get("bigint_field"))
    assert parquet_data == list(data)

def test_SparkJSONReader_remove_null_records(spark, temp_json_file_w_null_recs):
    uri, _, mdl = temp_json_file_w_null_recs
    
    reader = SparkJSONReader()

    result_df: DataFrame = reader.read_to_entity_type(
        entity_type=DataFrame, resource=uri.as_posix(), entity_name="test", schema=stringify_model(mdl)
    )

    assert result_df.count() == 2
    assert [rw.id_field for rw in result_df.select("id_field").collect()] == ["1", "3"]
