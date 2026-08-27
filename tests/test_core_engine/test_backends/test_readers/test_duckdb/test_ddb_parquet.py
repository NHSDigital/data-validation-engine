from dve.core_engine.backends.implementations.duckdb.readers.parquet import DuckDBParquetReader
from dve.core_engine.backends.utilities import stringify_model

import duckdb as ddb
import polars as pl

from ..reader_utils import TestParquetModel, test_parquet


def test_ddb_parquet_read_all_defaults(test_parquet):
    test_file_path, _expected_schema = test_parquet

    reader = DuckDBParquetReader()
    rel: ddb.DuckDBPyRelation = reader.read_to_relation(
        test_file_path.as_posix(), "test", TestParquetModel
    )
    ignore_fields = ["__record_index__"]

    materialised_df = rel.pl()
    assert materialised_df.shape[0] == 1
    assert all(
        v == _expected_schema.get(k) or k in ignore_fields for k, v in materialised_df.schema.items()
    )
