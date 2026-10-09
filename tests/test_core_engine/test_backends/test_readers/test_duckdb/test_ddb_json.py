from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import duckdb
import pytest
from duckdb import DuckDBPyRelation
from pydantic import BaseModel

from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.readers.json import DuckDBJSONReader
from dve.core_engine.backends.utilities import stringify_model
from dve.core_engine.constants import RECORD_INDEX_COLUMN_NAME
from tests.test_core_engine.test_backends.fixtures import duckdb_connection
from tests.test_core_engine.test_backends.test_readers.fixtures import (
    temp_dir,
    temp_json_file,
    temp_json_file_w_null_recs)





def test_ddb_json_reader_all_str(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.model_fields]
    reader = DuckDBJSONReader()
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, uri.as_posix(), "test", stringify_model(mdl)
    )
    assert rel.columns == expected_fields + [RECORD_INDEX_COLUMN_NAME]
    assert dict(zip(rel.columns, rel.dtypes)) == {**{fld: "VARCHAR" for fld in expected_fields}, RECORD_INDEX_COLUMN_NAME: "BIGINT"}
    assert rel.fetchall() == [(*[str(val) for val in rw.values()], idx) for idx, rw in enumerate(data, start=1)]


def test_ddb_json_reader_cast(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.model_fields]
    reader = DuckDBJSONReader()
    rel: DuckDBPyRelation = reader.read_to_entity_type(DuckDBPyRelation, uri.as_posix(), "test", mdl)
    
    assert rel.columns == expected_fields + [RECORD_INDEX_COLUMN_NAME]
    assert dict(zip(rel.columns, rel.dtypes)) == {**{
        name: str(get_duckdb_type_from_annotation(fld.annotation))
        for name, fld in mdl.model_fields.items()
    }, RECORD_INDEX_COLUMN_NAME: "BIGINT"}
    assert rel.fetchall() == [(*rw.values(), idx) for idx, rw in enumerate(data, start = 1)]


def test_ddb_json_write_parquet(temp_json_file):
    uri, _, mdl = temp_json_file
    reader = DuckDBJSONReader()
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, uri.as_posix(), "test", stringify_model(mdl)
    )
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(rel, target_loc)
    with duckdb.connect() as cnn:
        parquet_rel = cnn.read_parquet(target_loc)
        assert parquet_rel.df().to_dict(orient="records") == rel.df().to_dict(orient="records")

def test_ddb_json_write_parquet_py_iterator(temp_json_file):
    uri, _, mdl = temp_json_file
    reader = DuckDBJSONReader()
    conn = duckdb.connect()
    data = list(reader.read_to_py_iterator(uri.as_posix(), "test", stringify_model(mdl)))
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(conn.query("select dta.* from (select unnest($data) as dta)",
                                                  params={"data": data}),
                         target_loc)
    parquet_data = sorted(conn.read_parquet(target_loc).pl().iter_rows(named=True),
                          key= lambda x: x.get("bigint_field"))
    assert parquet_data == list(data)

def test_ddb_json_remove_null_records(temp_json_file_w_null_recs):
    uri, _, mdl = temp_json_file_w_null_recs
    reader = DuckDBJSONReader()
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, uri.as_posix(), "test", stringify_model(mdl)
    )
    assert rel.shape[0] == 2
    assert rel.select("id_field").pl().to_dict(as_series=False).get("id_field") == ["1", "3"]
