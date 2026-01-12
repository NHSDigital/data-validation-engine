"""Test Duck DB helpers"""

import datetime
import tempfile
from pathlib import Path
from typing import Any

import pytest
import pyspark.sql.types as pst
from duckdb import DuckDBPyRelation, DuckDBPyConnection
from pyspark.sql import Row, SparkSession

from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    _ddb_read_parquet,
    duckdb_rel_to_dictionaries)


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
    for chunk in duckdb_rel_to_dictionaries(test_rel):
        res.extend(chunk)
    
    assert res == data
        
