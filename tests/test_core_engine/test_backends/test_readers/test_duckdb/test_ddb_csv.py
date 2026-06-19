from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import duckdb
import polars as pl
import pytest
from duckdb import DuckDBPyRelation
from pydantic import BaseModel

from dve.core_engine.backends.exceptions import (
    EmptyFileError,
    MessageBearingError,
    UnableToParseCSVError,
)
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.readers.csv import (
    DuckDBCSVReader,
    DuckDBCSVRepeatingHeaderReader,
    PolarsToDuckDBCSVReader,
)
from dve.core_engine.backends.utilities import stringify_model
from dve.core_engine.constants import RECORD_INDEX_COLUMN_NAME

# pylint: disable=C0103,C0115,C0116,W0621


class SimpleModel(BaseModel):
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


class SimpleHeaderModel(BaseModel):
    header_1: str
    header_2: str


class VerySimpleModel(BaseModel):
    test_col: str


@pytest.fixture
def temp_dir():
    with TemporaryDirectory(prefix="ddb_test_csv_reader") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture(scope="function")
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


@pytest.fixture(scope="function")
def temp_csv_file_additional_fields(temp_dir: Path) -> Iterator[str]:
    test_df = pl.DataFrame({"test_col": ["fine"], "test_col2": ["wow"]})
    file_uri = temp_dir.joinpath("test_additional_fields.csv").as_posix()
    test_df.write_csv(
        file_uri,
        include_header=True,
        quote_style="always"
    )

    yield file_uri


@pytest.fixture(scope="function")
def temp_csv_file_missing_fields(temp_dir: Path) -> Iterator[str]:
    test_df = pl.DataFrame({"header_1": ["fine"]})
    file_uri = temp_dir.joinpath("test_missing_fields.csv").as_posix()
    test_df.write_csv(
        file_uri,
        include_header=True,
        quote_style="always"
    )

    yield file_uri


@pytest.fixture
def temp_empty_csv_file(temp_dir: Path):
    with open(temp_dir.joinpath("empty.csv"), mode="w"):
        pass

    yield temp_dir.joinpath("empty.csv"), SimpleModel


class TestDuckDBCSVReader:
    """Test DuckDBCSVReader"""

    def test_ddb_csv_reader_all_str(self, temp_csv_file):
        uri, header, data, mdl = temp_csv_file
        reader = DuckDBCSVReader(header=True, delim=",", connection=duckdb.connect())
        rel: DuckDBPyRelation = reader.read_to_entity_type(
            DuckDBPyRelation, str(uri), "test", stringify_model(mdl)
        )
        expected_dtypes = {**{fld: "VARCHAR" for fld in header.split(",")}, RECORD_INDEX_COLUMN_NAME: "BIGINT"}
        expected_data = [(*[str(val) for val in rw], idx) for idx, rw in enumerate(data, start=1)]
        assert rel.columns == header.split(",") + [RECORD_INDEX_COLUMN_NAME]
        assert dict(zip(rel.columns, rel.dtypes)) == expected_dtypes
        assert rel.fetchall() == expected_data

    def test_ddb_csv_reader_cast(self, temp_csv_file):
        uri, header, data, mdl = temp_csv_file
        reader = DuckDBCSVReader(header=True, delim=",", connection=duckdb.connect())
        rel: DuckDBPyRelation = reader.read_to_entity_type(DuckDBPyRelation, str(uri), "test", mdl)
        expected_dtypes = {**{
            fld_name: str(get_duckdb_type_from_annotation(fld_info.annotation))
            for fld_name, fld_info in mdl.model_fields.items()
        }, RECORD_INDEX_COLUMN_NAME: get_duckdb_type_from_annotation(int)}
        expected_data = [(*rw, idx) for idx, rw in enumerate(data, start=1)]
        assert rel.columns == header.split(",") + [RECORD_INDEX_COLUMN_NAME]
        assert dict(zip(rel.columns, rel.dtypes)) == expected_dtypes
        assert rel.fetchall() == expected_data

    def test_ddb_csv_write_parquet(self, temp_csv_file):
        uri, header, data, mdl = temp_csv_file
        reader = DuckDBCSVReader(header=True, delim=",", connection=duckdb.connect())
        rel: DuckDBPyRelation = reader.read_to_entity_type(
            DuckDBPyRelation, str(uri), "test", stringify_model(mdl)
        )
        target_loc: Path = uri.parent.joinpath("test_parquet.parquet").as_posix()
        reader.write_parquet(rel, target_loc)
        parquet_rel = reader._connection.read_parquet(target_loc)
        assert sorted(parquet_rel.df().to_dict(orient="records"), key=lambda x: x.get(RECORD_INDEX_COLUMN_NAME)) == sorted([{**rec, RECORD_INDEX_COLUMN_NAME: idx} for idx, rec in enumerate(rel.df().to_dict(orient="records"), start=1)], key=lambda x: x.get(RECORD_INDEX_COLUMN_NAME))

    def test_ddb_csv_read_empty_file(self, temp_empty_csv_file):
        uri, mdl = temp_empty_csv_file
        reader = DuckDBCSVReader(header=True, delim=",", connection=duckdb.connect())

        with pytest.raises(EmptyFileError):
            reader.read_to_relation(str(uri), "test", mdl)

    def test_DuckDBCSVReader_with_null_empty_strings(self, temp_dir):
        test_df = pl.DataFrame({"test_col": ["fine", " ", "    "]})
        file_uri = temp_dir.joinpath("test_empty_string1.csv").as_posix()
        test_df.write_csv(
            file_uri,
            include_header=True,
            quote_style="always"
        )

        reader = DuckDBCSVReader(
            header=True,
            delim=",",
            quotechar='"',
            connection=duckdb.connect(),
            null_empty_strings=True,
        )

        entity = reader.read_to_relation(file_uri, "test", VerySimpleModel)

        assert entity.shape[0] == 3
        assert entity.filter("test_col IS NULL").shape[0] == 2

    def test_DuckDBCSVReader_with_malformed_header(self, temp_dir):
        test_data_headers = '"varchar_field,bigint_field,date_field,timestamp_field"'
        row_data = "hello,1,2023-04-01,2023-04-01T12:30:00"
        temp_id = uuid4().hex
        fqp = Path(temp_dir, f"{temp_id}.csv")

        with open(fqp, mode="w", encoding="utf-8") as f:
            f.write(f"{test_data_headers}\n{row_data}")

        reader = DuckDBCSVReader(
            header=True,
            delim=",",
            connection=duckdb.connect(),
        )

        with pytest.raises(UnableToParseCSVError) as err:
            reader.read_to_relation(fqp.as_posix(), "test", SimpleModel)
            assert len(err.messages) == 1


class TestPolarsToDuckDBCSVReader:
    """Test PolarsToDuckDBCSVReader"""

    def test_polars_to_ddb_csv_reader(self, temp_csv_file):
        uri, header, data, mdl = temp_csv_file
        reader = PolarsToDuckDBCSVReader(
            header=True, delim=",", quotechar='"', connection=duckdb.connect()
        )
        entity = reader.read_to_relation(str(uri), "test", mdl)

        assert entity.shape[0] == 2

    def test_PolarsToDuckDBCSVReader_with_null_empty_strings(self, temp_dir):
        test_df = pl.DataFrame({"test_col": ["fine", " ", "    "]})
        file_uri = temp_dir.joinpath("test_empty_string3.csv").as_posix()
        test_df.write_csv(
            file_uri,
            include_header=True,
            quote_style="always"
        )

        reader = PolarsToDuckDBCSVReader(
            header=True,
            delim=",",
            quotechar='"',
            connection=duckdb.connect(),
            null_empty_strings=True,
        )

        entity = reader.read_to_relation(file_uri, "test", VerySimpleModel)

        assert entity.shape[0] == 3
        assert entity.filter("test_col IS NULL").shape[0] == 2

    def test_PolarsToDuckDBCSVReader_with_malformed_header(self, temp_dir):
        test_data_headers = '"varchar_field,bigint_field,date_field,timestamp_field"'
        row_data = "hello,1,2023-04-01,2023-04-01T12:30:00"
        temp_id = uuid4().hex
        fqp = Path(temp_dir, f"{temp_id}.csv")

        with open(fqp, mode="w", encoding="utf-8") as f:
            f.write(f"{test_data_headers}\n{row_data}")

        reader = PolarsToDuckDBCSVReader(
            header=True,
            delim=",",
            connection=duckdb.connect(),
        )

        with pytest.raises(UnableToParseCSVError) as err:
            reader.read_to_relation(fqp.as_posix(), "test", SimpleModel)
            assert len(err.messages) == 1


class TestDuckDBCSVRepeatingHeaderReader:
    """Test DuckDBCSVRepeatingHeaderReader"""

    def test_ddb_csv_repeating_header_reader_non_duplicate(self, temp_dir):
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
            header=True, delim=",", quotechar='"', connection=duckdb.connect()
        )
        entity = reader.read_to_relation(str(file_uri), "test", SimpleHeaderModel)

        assert entity.shape[0] == 1

    def test_ddb_csv_repeating_header_reader_with_more_than_one_set_of_distinct_values(self, temp_dir):
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
            header=True, delim=",", quotechar='"', connection=duckdb.connect()
        )

        with pytest.raises(MessageBearingError):
            reader.read_to_relation(str(file_uri), "test", SimpleHeaderModel)

    def test_DuckDBCSVRepeatingHeaderReader_with_null_empty_strings(self, temp_dir):
        test_df = pl.DataFrame({
            "header_1": ["fine",], "header_2": ["    "],
        })
        file_uri = temp_dir.joinpath("test_empty_string2.csv").as_posix()
        test_df.write_csv(
            file_uri,
            include_header=True,
            quote_style="always"
        )

        reader = DuckDBCSVRepeatingHeaderReader(
            header=True,
            delim=",",
            quotechar='"',
            connection=duckdb.connect(),
            null_empty_strings=True,
        )

        entity = reader.read_to_relation(file_uri, "test", SimpleHeaderModel)

        assert entity.shape[0] == 1
        assert entity.filter("header_2 IS NULL").shape[0] == 1
