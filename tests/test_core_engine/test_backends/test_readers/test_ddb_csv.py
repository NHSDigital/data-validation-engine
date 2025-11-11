from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from duckdb import DuckDBPyRelation, default_connection
from pydantic import BaseModel

from dve.core_engine.backends.exceptions import EmptyFileError, MessageBearingError
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.readers.csv import (
    DuckDBCSVReader,
    DuckDBCSVRepeatingHeaderReader,
    PolarsToDuckDBCSVReader,
)
from dve.core_engine.backends.utilities import stringify_model
from tests.test_core_engine.test_backends.fixtures import duckdb_connection

# pylint: disable=C0116


class SimpleModel(BaseModel):
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


class SimpleHeaderModel(BaseModel):
    header_1: str
    header_2: str


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


@pytest.fixture
def temp_empty_csv_file(temp_dir: Path):
    with open(temp_dir.joinpath("empty.csv"), mode="w"):
        pass

    yield temp_dir.joinpath("empty.csv"), SimpleModel


def test_ddb_csv_reader_all_str(temp_csv_file):
    uri, header, data, mdl = temp_csv_file
    reader = DuckDBCSVReader(header=True, delim=",", connection=default_connection)
    rel: DuckDBPyRelation = reader.read_to_entity_type(
        DuckDBPyRelation, str(uri), "test", stringify_model(mdl)
    )
    assert rel.columns == header.split(",")
    assert dict(zip(rel.columns, rel.dtypes)) == {fld: "VARCHAR" for fld in header.split(",")}
    assert rel.fetchall() == [tuple(str(val) for val in rw) for rw in data]


def test_ddb_csv_reader_cast(temp_csv_file):
    uri, header, data, mdl = temp_csv_file
    reader = DuckDBCSVReader(header=True, delim=",", connection=default_connection)
    rel: DuckDBPyRelation = reader.read_to_entity_type(DuckDBPyRelation, str(uri), "test", mdl)
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
        DuckDBPyRelation, str(uri), "test", stringify_model(mdl)
    )
    target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
    reader.write_parquet(rel, target_loc)
    parquet_rel = reader._connection.read_parquet(target_loc)
    assert parquet_rel.df().to_dict(orient="records") == rel.df().to_dict(orient="records")


def test_ddb_csv_read_empty_file(temp_empty_csv_file):
    uri, mdl = temp_empty_csv_file
    reader = DuckDBCSVReader(header=True, delim=",", connection=default_connection)

    with pytest.raises(EmptyFileError):
        reader.read_to_relation(str(uri), "test", mdl)


def test_polars_to_ddb_csv_reader(temp_csv_file):
    uri, header, data, mdl = temp_csv_file
    reader = PolarsToDuckDBCSVReader(
        header=True, delim=",", quotechar='"', connection=default_connection
    )
    entity = reader.read_to_relation(str(uri), "test", mdl)

    assert entity.shape[0] == 2


def test_ddb_csv_repeating_header_reader_non_duplicate(temp_dir):
    header = "header_1,header_2,non_header_1"
    typed_data = [
        ["hvalue1", "hvalue1", "nhvalue1"],
        ["hvalue1", "hvalue1", "nhvalue2"],
        ["hvalue1", "hvalue1", "nhvalue3"],
    ]
    with open(temp_dir.joinpath("test_header.csv"), mode="w") as csv_file:
        csv_file.write(header + "\n")
        for rw in typed_data:
            csv_file.write(",".join([str(val) for val in rw]) + "\n")

    file_uri = temp_dir.joinpath("test_header.csv")

    reader = DuckDBCSVRepeatingHeaderReader(
        header=True, delim=",", quotechar='"', connection=default_connection
    )
    entity = reader.read_to_relation(str(file_uri), "test", SimpleHeaderModel)

    assert entity.shape[0] == 1


def test_ddb_csv_repeating_header_reader_with_more_than_one_set_of_distinct_values(temp_dir):
    header = "header_1,header_2,non_header_1"
    typed_data = [
        ["hvalue1", "hvalue2", "nhvalue1"],
        ["hvalue2", "hvalue2", "nhvalue2"],
        ["hvalue1", "hvalue1", "nhvalue3"],
    ]
    with open(temp_dir.joinpath("test_header.csv"), mode="w") as csv_file:
        csv_file.write(header + "\n")
        for rw in typed_data:
            csv_file.write(",".join([str(val) for val in rw]) + "\n")

    file_uri = temp_dir.joinpath("test_header.csv")
    reader = DuckDBCSVRepeatingHeaderReader(
        header=True, delim=",", quotechar='"', connection=default_connection
    )

    with pytest.raises(MessageBearingError):
        reader.read_to_relation(str(file_uri), "test", SimpleHeaderModel)
