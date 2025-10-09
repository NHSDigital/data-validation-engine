"""Tests for the CSV readers."""

# pylint: disable=redefined-outer-name,expression-not-assigned,line-too-long,unused-import
# pylint: disable=missing-class-docstring
import csv
from pathlib import Path
from typing import Dict, Iterator, Optional

import pandas as pd
import pytest
from pydantic import BaseModel

from dve.core_engine.backends.exceptions import EmptyFileError, FieldCountMismatch
from dve.core_engine.backends.readers import CSVFileReader

from ....conftest import get_test_file_path
from ....fixtures import temp_dir


@pytest.fixture(scope="module")
def planet_location() -> Iterator[str]:
    """The URI of the planet data"""
    yield get_test_file_path("planets/planets.csv").as_uri()


@pytest.fixture(scope="function")
def planet_data() -> Iterator[Dict[str, Dict[str, str]]]:
    """The planet data, as loaded by Python's default parser."""
    with get_test_file_path("planets/planets.csv").open("r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        yield {row["planet"]: row for row in reader}


@pytest.fixture(scope="function")
def null_values_location(temp_dir: str) -> Iterator[str]:
    """The URI of a file containing nullable values."""
    path = Path(temp_dir).joinpath("thing.csv")
    path.write_text("Column\nNULL\nnull\n", encoding="utf-8")
    yield path.as_uri()


@pytest.fixture(scope="function")
def pipe_delimited_location(temp_dir: str) -> Iterator[str]:
    """The URI of a file containing pipe-delimited values."""
    path = Path(temp_dir).joinpath("thing.csv")
    path.write_text("ColumnA|ColumnB|ColumnC\n1|2|3\n", encoding="utf-8")
    yield path.as_uri()


@pytest.fixture(scope="function")
def empty_location(temp_dir: str) -> Iterator[str]:
    """The URI of an empty file."""
    path = Path(temp_dir).joinpath("thing.csv")
    path.write_text("", encoding="utf-8")
    yield path.as_uri()


@pytest.fixture(scope="function")
def header_only_location(temp_dir: str) -> Iterator[str]:
    """The URI of a file with only a header."""
    path = Path(temp_dir).joinpath("thing.csv")
    path.write_text("Column_A,Column_B,Column_C\n", encoding="utf-8")
    yield path.as_uri()


class PlanetsSubset(BaseModel):
    """A subset of the planets data."""

    planet: str
    mass: str
    diameter: str


class PlanetsSubsetWithExtra(PlanetsSubset):
    """A subset of the planets data with an extra field."""

    random_null: str


class Planets(PlanetsSubset):
    """The model for the planets data."""

    density: str
    gravity: str
    escape_velocity: str
    rotation_period: str
    length_of_day: str
    distance_from_sun: str
    perihelion: str
    aphelion: str
    orbital_period: str
    orbital_velocity: str
    orbital_inclination: str
    orbital_eccentricity: str
    obliquity_to_orbit: str
    mean_temperature: str
    surface_pressure: str
    number_of_moons: str
    has_ring_system: str
    has_global_magnetic_field: str


class SingleColumnModel(BaseModel):
    Column: str


class BasicModel(BaseModel):
    ColumnA: str
    ColumnB: str
    ColumnC: str


class TestParametrizedCSVParser:
    """Tests for the CSV parser implementation."""

    def test_csv_file_parser(
        self,
        planet_location: str,
        planet_data: Dict[str, Dict[str, str]],
    ):
        """Test that a basic CSV file can be loaded and read."""
        reader = CSVFileReader()
        results = list(reader.read_to_py_iterator(planet_location, "", Planets))
        parsed = {row["planet"]: row for row in results}
        assert parsed == planet_data

    def test_csv_file_get_subset(
        self,
        planet_location: str,
        planet_data: Dict[str, Dict[str, str]],
    ):
        """Test that a subset of columns can be loaded from the CSV."""
        reader = CSVFileReader()

        results = list(reader.read_to_py_iterator(planet_location, "", PlanetsSubset))
        parsed = {row["planet"]: row for row in results}

        # Keep only keys in the subset from the source
        subset_keys = set(PlanetsSubset.__fields__.keys())
        for data in planet_data.values():
            to_pop = set(data.keys()) - subset_keys
            for key in to_pop:
                del data[key]

        assert parsed == planet_data

    def test_csv_file_get_subset_add_missing(
        self, planet_location: str, planet_data: Dict[str, Dict[str, str]]
    ):
        """
        Test that column names are auto-inferred properly and row order is maintained
        if the header is missing. Also: new, empty columns can be added.

        """
        reader = CSVFileReader()

        results = list(reader.read_to_py_iterator(planet_location, "", PlanetsSubsetWithExtra))
        parsed = {row["planet"]: row for row in results}

        # Keep only keys in the subset from the source
        subset_keys = set(PlanetsSubset.__fields__.keys())
        for data in planet_data.values():
            to_pop = set(data.keys()) - subset_keys
            for key in to_pop:
                del data[key]
            data["random_null"] = None  # type: ignore

        assert parsed == planet_data

    def test_csv_file_filled_from_provided(
        self,
        planet_location: str,
        planet_data: Dict[str, Dict[str, str]],
    ):
        """
        Test that a basic CSV file without header can be read, and all the field
        names can be passed.

        """
        reader = CSVFileReader(header=False)

        results = list(reader.read_to_py_iterator(planet_location, "", Planets))
        parsed = {row["planet"]: row for row in results}
        del parsed["planet"]
        assert parsed == planet_data

    def test_csv_file_raises_missing_cols(self, planet_location: str):
        """
        Test that a basic CSV file without header can be read, and if some field
        names are missing an error is raised.

        """
        reader = CSVFileReader(header=False)

        with pytest.raises(FieldCountMismatch):
            list(reader.read_to_py_iterator(planet_location, "", PlanetsSubset))

    def test_csv_file_raises_empty_file(
        self,
        empty_location: str,
    ):
        """
        Test that an error will be raised if an empty file is attempted to be
        read.

        """
        reader = CSVFileReader()
        with pytest.raises(EmptyFileError):
            list(reader.read_to_py_iterator(empty_location, "", PlanetsSubset))

    def test_csv_file_nulls_null_values(
        self,
        null_values_location: str,
    ):
        """
        Test that values which are null-equivalent are successfully nulled.

        """
        reader = CSVFileReader(null_values=["NULL", "null", ""])
        results = list(reader.read_to_py_iterator(null_values_location, "", SingleColumnModel))
        assert all(row["Column"] is None for row in results)

    def test_csv_file_with_only_header_parses(
        self,
        header_only_location: str,
    ):
        """Test that a CSV with only a header can be parsed."""
        reader = CSVFileReader()
        assert not list(reader.read_to_py_iterator(header_only_location, "", BasicModel))

    def test_csv_file_can_be_pipe_delimited(
        self,
        pipe_delimited_location: str,
    ):
        """Test that a pipe-delimited CSV file can be parsed."""
        reader = CSVFileReader(delimiter="|")
        results = list(reader.read_to_py_iterator(pipe_delimited_location, "", BasicModel))
        assert results == [{"ColumnA": "1", "ColumnB": "2", "ColumnC": "3"}]

    @pytest.mark.parametrize(["schema"], [(None,), (Planets,)])
    def test_base_csv_reader_parquet_write(
        self,
        schema: Optional[BaseModel],
        temp_dir: str,
        planet_location: str,
        planet_data: Dict[str, Dict[str, str]],
    ):
        """Test that a basic CSV file can be loaded and read."""
        reader = CSVFileReader()
        target_location: str = Path(temp_dir).joinpath("test_base_csv.parquet").as_posix()
        entity = reader.read_to_py_iterator(planet_location, "", Planets)
        reader.write_parquet(entity=entity, target_location=target_location, schema=schema)
        assert sorted(
            pd.read_parquet(target_location).to_dict(orient="records"),
            key=lambda x: x.get("planet"),
        ) == sorted([dict(val) for val in planet_data.values()], key=lambda x: x.get("planet"))
