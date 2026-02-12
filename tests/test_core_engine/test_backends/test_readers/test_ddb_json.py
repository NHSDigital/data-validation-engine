from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

import pytest
from duckdb import DuckDBPyRelation, default_connection
from pydantic import BaseModel

from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.readers.json import DuckDBJSONReader
from dve.core_engine.backends.utilities import stringify_model
from tests.test_core_engine.test_backends.fixtures import duckdb_connection


class SimpleModel(BaseModel):
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


@pytest.fixture
def temp_dir():
    with TemporaryDirectory(prefix="ddb_test_json_reader") as temp_dir:
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


def test_ddb_json_reader_all_str(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.__fields__]
    reader = DuckDBJSONReader()
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, uri.as_posix(), "test", stringify_model(mdl)
    )
    assert rel.columns == expected_fields
    assert dict(zip(rel.columns, rel.dtypes)) == {fld: "VARCHAR" for fld in expected_fields}
    assert rel.fetchall() == [tuple(str(val) for val in rw.values()) for rw in data]


def test_ddb_json_reader_cast(temp_json_file):
    uri, data, mdl = temp_json_file
    expected_fields = [fld for fld in mdl.__fields__]
    reader = DuckDBJSONReader()
    rel: DuckDBPyRelation = reader.read_to_entity_type(DuckDBPyRelation, uri.as_posix(), "test", mdl)
    
    assert rel.columns == expected_fields
    assert dict(zip(rel.columns, rel.dtypes)) == {
        fld.name: str(get_duckdb_type_from_annotation(fld.annotation))
        for fld in mdl.__fields__.values()
    }
    assert rel.fetchall() == [tuple(rw.values()) for rw in data]


def test_ddb_csv_write_parquet(temp_json_file):
    uri, _, mdl = temp_json_file
    reader = DuckDBJSONReader()
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, uri.as_posix(), "test", stringify_model(mdl)
    )
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(rel, target_loc)
    parquet_rel = default_connection().read_parquet(target_loc)
    assert parquet_rel.df().to_dict(orient="records") == rel.df().to_dict(orient="records")

def test_ddb_json_write_parquet_py_iterator(temp_json_file):
    uri, _, mdl = temp_json_file
    reader = DuckDBJSONReader()
    data = list(reader.read_to_py_iterator(uri.as_posix(), "test", stringify_model(mdl)))
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(default_connection().query("select dta.* from (select unnest($data) as dta)",
                                                  params={"data": data}),
                         target_loc)
    parquet_data = sorted(default_connection().read_parquet(target_loc).pl().iter_rows(named=True),
                          key= lambda x: x.get("bigint_field"))
    assert parquet_data == list(data)
