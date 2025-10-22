from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from duckdb import DuckDBPyRelation, default_connection
from pydantic import BaseModel

from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.readers.csv import DuckDBCSVReader, SQLType
from dve.core_engine.backends.utilities import stringify_model
from tests.test_core_engine.test_backends.fixtures import duckdb_connection


class SimpleModel(BaseModel):
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


@pytest.fixture
def temp_dir():
    with TemporaryDirectory(prefix="ddb_test_csv_reader") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_csv_file(temp_dir: Path):
    header: str = "varchar_field,bigint_field,date_field,timestamp_field"
    typed_data = [
        ["hi", 1, date(2023, 1, 3), datetime(2023, 1, 3, 12, 0, 3)],
        ["bye", 2, date(2023, 3, 7), datetime(2023, 5, 9, 15, 21, 53)],
    ]

    with open(temp_dir.joinpath("dummy.csv"), mode="w") as csv_file:
        csv_file.write(header + "\n")
        for rw in typed_data:
            csv_file.write(",".join([str(val) for val in rw]) + "\n")

    yield temp_dir.joinpath("dummy.csv"), header, typed_data, SimpleModel


class SimpleModel(BaseModel):
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


def test_ddb_csv_reader_all_str(temp_csv_file):
    uri, header, data, mdl = temp_csv_file
    reader = DuckDBCSVReader(header=True, delim=",", connection=default_connection)
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, uri, "test", stringify_model(mdl)
    )
    assert rel.columns == header.split(",")
    assert dict(zip(rel.columns, rel.dtypes)) == {fld: "VARCHAR" for fld in header.split(",")}
    assert rel.fetchall() == [tuple(str(val) for val in rw) for rw in data]


def test_ddb_csv_reader_cast(temp_csv_file):
    uri, header, data, mdl = temp_csv_file
    reader = DuckDBCSVReader(header=True, delim=",", connection=default_connection)
    rel: DuckDBPyRelation = reader.read_to_entity_type(DuckDBPyRelation, uri, "test", mdl)
    assert rel.columns == header.split(",")
    assert dict(zip(rel.columns, rel.dtypes)) == {
        fld.name: str(get_duckdb_type_from_annotation(fld.annotation))
        for fld in mdl.__fields__.values()
    }
    assert rel.fetchall() == [tuple(rw) for rw in data]


def test_ddb_csv_write_parquet(temp_csv_file):
    uri, header, data, mdl = temp_csv_file
    reader = DuckDBCSVReader(header=True, delim=",", connection=default_connection)
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, uri, "test", stringify_model(mdl)
    )
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(rel, target_loc)
    parquet_rel = reader._connection.read_parquet(target_loc)
    assert parquet_rel.df().to_dict(orient="records") == rel.df().to_dict(orient="records")
