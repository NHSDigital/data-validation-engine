"""Test Duck DB helpers"""
import tempfile
from pathlib import Path

import pytest
import pyspark.sql.types as pst
from duckdb import DuckDBPyRelation, DuckDBPyConnection
from pyspark.sql import Row, SparkSession

from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import _ddb_read_parquet


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
    ]
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
            pst.StructType([
                pst.StructField("movie_name", pst.StringType()),
                pst.StructField("avg_user_rating", pst.FloatType()),
                pst.StructField("avg_critic_rating", pst.FloatType()),
            ])
        )
        out_path = str(Path(temp_dir_path, outpath))
        test_data_df.coalesce(1).write.parquet(out_path)

        ddby_relation = _ddb_read_parquet(TempConnection(ddb_conn), out_path)

        assert isinstance(ddby_relation, DuckDBPyRelation)
        assert ddby_relation.count("*").fetchone()[0] == 2  # type: ignore
