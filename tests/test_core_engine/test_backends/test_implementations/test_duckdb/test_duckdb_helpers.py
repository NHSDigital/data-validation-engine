"""Test Duck DB helpers"""

# pylint: disable=C0301,C0116

import datetime
import json
import os
import tempfile
from pathlib import Path
from typing import Any, List

import polars as pl
import pytest
import pyspark.sql.types as pst
from duckdb import DuckDBPyRelation, DuckDBPyConnection
from pydantic import BaseModel
from pyspark.sql import Row, SparkSession

from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    _ddb_filter_contract_errors,
    _ddb_read_parquet,
    duckdb_rel_to_dictionaries,
    get_duckdb_cast_statement_from_annotation,
    get_duckdb_type_from_annotation,
    relation_is_empty,
)

@pytest.fixture
def casting_test_table(temp_ddb_conn):
    _, conn = temp_ddb_conn
    conn.sql("""CREATE TABLE test_casting (
    str_test VARCHAR,
    int_test VARCHAR,
    date_test VARCHAR,
    timestamp_test VARCHAR,
    list_int_field VARCHAR[],
    basic_model STRUCT(str_field VARCHAR, date_field VARCHAR),
    another_model STRUCT(unique_id VARCHAR, basic_models STRUCT(str_field VARCHAR, date_field VARCHAR)[]))""")
    
    conn.sql("""INSERT INTO test_casting
              VALUES(
              'good_one',
              '1',
              '2024-11-13',
              '2024-04-15 12:25:36',
              ['1', '2', '3'],
              {'str_field': 'test', 'date_field': '2024-12-11'},
              {'unique_id': '1', "basic_models": [{'str_field': 'test_nest', 'date_field': '2020-01-04'}, {'str_field': 'test_nest2', 'date_field': '2020-01-05'}]}),
              (
              'dodgy_dates',
              '2',
              '24-11-13',
              '2024-4-15 12:25:36',
              ['4', '5', '6'],
              {'str_field': 'test', 'date_field': '202-1-11'},
              {'unique_id': '2', "basic_models": [{'str_field': 'test_dd', 'date_field': '20-01-04'}, {'str_field': 'test_dd2', 'date_field': '2020-1-5'}]})""")
    
    
    yield temp_ddb_conn
    
    conn.sql("DROP TABLE IF EXISTS test_casting")


@pytest.fixture
def example_data_contract_error_codes(temp_ddb_conn):
    _, con = temp_ddb_conn

    test_df = pl.DataFrame([  # pylint: disable=W0612
        {"id": "field1", "attr": 1, "__record_index__": 1,},
        {"id": "field2", "attr": None, "__record_index__": 2,},
        {"id": "field3", "attr": 2, "__record_index__": 3,},
        {"id": "field4", "attr": None, "__record_index__": 4,},
    ])
    test_entity = con.sql("SELECT * FROM test_df")
    error_contract_messages = [
        {
            "Entity": "test_entity",
            "Key": "",
            "FailureType": "record",
            "Status": "error",
            "ErrorType": "",
            "ErrorLocation": "attr",
            "ErrorMessage": "",
            "ErrorCode": "",
            "ReportingField": "attr",
            "RecordIndex": 2,
            "Value": "hello",
            "Category": "Bad value"
        },
        {
            "Entity": "test_entity",
            "Key": "",
            "FailureType": "record",
            "Status": "error",
            "ErrorType": "",
            "ErrorLocation": "attr",
            "ErrorMessage": "",
            "ErrorCode": "",
            "ReportingField": "attr",
            "RecordIndex": 4,
            "Value": "world",
            "Category": "Bad value"
        }
    ]
    with tempfile.TemporaryDirectory() as temp_dir_path:
        os.mkdir(Path(temp_dir_path, "errors"))
        temp_error_file = Path(temp_dir_path, "errors", "data_contract_errors.jsonl")
        with open(temp_error_file, encoding="utf-8", mode="w") as tpf:
            for error in error_contract_messages:
                json.dump(error, tpf)
                tpf.write("\n")

        yield con, test_entity, temp_dir_path



class BasicModel(BaseModel):
    str_field: str
    date_field: datetime.date
    
class AnotherModel(BaseModel):
    unique_id: int
    basic_models: List[BasicModel]

class CastingRecord(BaseModel):
    str_test: str
    int_test: int
    date_test: datetime.date
    timestamp_test: datetime.datetime
    list_int_field: list[int]
    basic_model: BasicModel
    another_model: AnotherModel

class TempConnection:
    """
    Full object would be a DataContract object but this simplified down to meet min requirements
    of the test.
    """

    def __init__(self, connection: DuckDBPyConnection) -> None:
        self._connection = connection



@pytest.mark.parametrize(
    "outpath",
    [
        ("movie_ratings"),
        ("movie_ratings/"),
        ("file://movie_ratings/"),
    ],
)
def test__ddb_read_parquet_with_hive_format(
    spark: SparkSession, temp_ddb_conn: DuckDBPyConnection, outpath: str
):
    """Test to check that duckdb can handle parquets written in a hive based format (e.g. Spark)."""
    _, ddb_conn = temp_ddb_conn  # type: ignore
    with tempfile.TemporaryDirectory() as temp_dir_path:
        test_data_df = spark.createDataFrame(
            [
                Row(movie_name="Hot Fuzz", avg_user_rating=7.7, avg_critic_rating=6.5),
                Row(movie_name="Nemo", avg_user_rating=8.8, avg_critic_rating=7.6),
            ],
            pst.StructType(
                [
                    pst.StructField("movie_name", pst.StringType()),
                    pst.StructField("avg_user_rating", pst.FloatType()),
                    pst.StructField("avg_critic_rating", pst.FloatType()),
                ]
            ),
        )
        out_path = str(Path(temp_dir_path, outpath))
        test_data_df.coalesce(1).write.parquet(out_path)

        ddby_relation = _ddb_read_parquet(TempConnection(ddb_conn), out_path)

        assert isinstance(ddby_relation, DuckDBPyRelation)
        assert ddby_relation.count("*").fetchone()[0] == 2  # type: ignore


@pytest.mark.parametrize(
    "data",
    (
    
        [
            {
                "str_field": "hi",
                "int_field": 5,
                "array_float_field": [6.5, 7.25],
                "date_field": datetime.date(2021, 5, 3),
                "timestamp_field": datetime.datetime(2022, 6, 7, 1, 2, 3),
            },
            {
                "str_field": "bye",
                "int_field": 3,
                "array_float_field": None,
                "date_field": datetime.date(2021, 8, 11),
                "timestamp_field": datetime.datetime(2022, 4, 3, 1, 2, 3),
            },
        ],
    
    ),
)
def test_duckdb_rel_to_dictionaries(temp_ddb_conn: DuckDBPyConnection,
                                    data: list[dict[str, Any]]):
    _, con = temp_ddb_conn
    test_rel = con.query("select dta.* from (select unnest($data) as dta)",
                                                  params={"data": data})
    res: list = []
    for chunk in duckdb_rel_to_dictionaries(test_rel, 1):
        res.append(chunk)
    
    assert res == data

# add decimal check
@pytest.mark.parametrize("field_name,field_type,cast_statement",
                         [("str_test", str, "try_cast(trim(\"str_test\") as VARCHAR)"),
                          ("int_test", int, "try_cast(trim(\"int_test\") as BIGINT)"),
                          ("date_test", datetime.date,"TRY_CAST(try_strptime(TRIM(\"date_test\"), 'YYYY-MM-DD') as DATE)"),
                          ("timestamp_test", datetime.datetime, "TRY_CAST(try_strptime(TRIM(\"timestamp_test\"), 'YYYY-MM-DDTHH:MM:SS') as TIMESTAMP)"),
                          ("list_int_field", list[int], "try_cast(list_transform(\"list_int_field\", x -> trim(\"x\")) as BIGINT[])"),
                          ("basic_model", BasicModel, "try_cast(struct_pack(\"str_field\":= trim(\"basic_model\".str_field),\"date_field\":= TRY_CAST(try_strptime(TRIM(\"basic_model\".date_field), 'YYYY-MM-DD') as DATE)) as STRUCT(str_field VARCHAR, date_field DATE))"),
                          ("another_model", AnotherModel, "try_cast(struct_pack(\"unique_id\":= trim(\"another_model\".unique_id),\"basic_models\":= list_transform(\"another_model\".basic_models, x -> struct_pack(\"str_field\":= trim(\"x\".str_field),\"date_field\":= TRY_CAST(try_strptime(TRIM(\"x\".date_field), 'YYYY-MM-DD') as DATE)))) as STRUCT(unique_id BIGINT, basic_models STRUCT(str_field VARCHAR, date_field DATE)[]))")])
def test_get_duckdb_cast_statement_from_annotation(field_name, field_type, cast_statement):
    assert get_duckdb_cast_statement_from_annotation(field_name, field_type) == cast_statement


def test_use_cast_statements(casting_test_table):
    _, conn = casting_test_table
    test_rel = conn.sql("SELECT * from test_casting")
    casting_statements = [ f"{get_duckdb_cast_statement_from_annotation(fld.name, fld.annotation)} as {fld.name}" for fld in CastingRecord.__fields__.values()]
    test_rel = test_rel.project(",".join(casting_statements))
    assert dict(zip(test_rel.columns, test_rel.dtypes)) == {fld.name: get_duckdb_type_from_annotation(fld.annotation) for fld in CastingRecord.__fields__.values()}
    dodgy_date_rec = test_rel.pl()[1].to_dicts()[0]
    assert (not dodgy_date_rec.get("date_test") and 
            not dodgy_date_rec.get("basic_model",{}).get("date_field")
            and all(not val.get("date_field") for val in dodgy_date_rec.get("another_model",{}).get("basic_models",[]))
    )


def test_ddb_filter_contract_errors(example_data_contract_error_codes):  # pylint: disable=W0621
    ddb_cnn, entity_rel, temp_dir = example_data_contract_error_codes
    expected_df = pl.DataFrame([  # pylint: disable=W0612
        {"id": "field1", "attr": 1, "__record_index__": 1,},
        {"id": "field3", "attr": 2, "__record_index__": 3,},
    ])
    expected_rel = ddb_cnn.sql("SELECT * FROM expected_df")
    result_rel = _ddb_filter_contract_errors(
        TempConnection(ddb_cnn), temp_dir, entity_rel, "test_entity"
    )
    assert result_rel.pl().shape[0] == 2
    assert expected_rel.join(result_rel, "__record_index__", "anti").pl().shape[0] == 0


def test_relation_is_empty(temp_ddb_conn: DuckDBPyConnection):
    _, con = temp_ddb_conn
    rel = con.sql("SELECT 'abc' AS test").filter("test IS NULL")
    assert relation_is_empty(rel)
