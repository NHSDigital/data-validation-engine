from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import pytest
from pydantic import BaseModel
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType 

from dve.core_engine.backends.implementations.spark.spark_helpers import (
    get_type_from_annotation,
)
from dve.core_engine.backends.implementations.spark.readers.json import SparkJSONReader
from dve.core_engine.backends.utilities import stringify_model


class SimpleModel(BaseModel):
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


@pytest.fixture
def temp_dir():
    with TemporaryDirectory(prefix="spark_test_json_reader") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_json_file(temp_dir: Path):
    field_names: List[str] = ["varchar_field","bigint_field","date_field","timestamp_field"]
    typed_data = [
        ["hi", 1, date(2023, 1, 3), datetime(2023, 1, 3, 12, 0, 3)],
        ["bye", 2, date(2023, 3, 7), datetime(2023, 5, 9, 15, 21, 53)],
    ]
    
    test_data = [dict(zip(field_names, rw)) for rw in typed_data]

    with open(temp_dir.joinpath("test.json"), mode="w") as json_file:
        json.dump(test_data, json_file, default=str)

    yield temp_dir.joinpath("test.json"), test_data, SimpleModel


class SimpleModel(BaseModel):
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


def test_spark_json_reader_all_str(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.__fields__]
    reader = SparkJSONReader()
    df: DataFrame = reader.read_to_entity_type(
        DataFrame, uri.as_posix(), "test", stringify_model(mdl)
    )
    assert df.columns == expected_fields
    assert df.schema == StructType([StructField(nme, StringType()) for nme in expected_fields])
    assert [rw.asDict() for rw in df.collect()] == [{k: str(v) for k, v in rw.items()} for rw in data]

def test_spark_json_reader_cast(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.__fields__]
    reader = SparkJSONReader()
    df: DataFrame = reader.read_to_entity_type(DataFrame, uri.as_posix(), "test", mdl)
    
    assert df.columns == expected_fields
    assert df.schema == StructType([StructField(fld.name, get_type_from_annotation(fld.annotation)) 
                                    for fld in mdl.__fields__.values()])
    assert [rw.asDict() for rw in df.collect()] == data


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
