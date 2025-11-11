"""
Helper objects for testing pipeline objects
"""
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

import json
import shutil
import tempfile
from pathlib import Path
from typing import Iterator, Tuple
from uuid import uuid4

import polars as pl
import pytest

from dve.core_engine.models import SubmissionInfo
from dve.pipeline.utils import SubmissionStatus

import dve.pipeline.utils

from ..conftest import get_test_file_path

def clear_config_cache():
    dve.pipeline.utils._configs = {}

PLANET_POLARS_SCHEMA = {
    "planet": pl.Utf8(),
    "mass": pl.Float32(),
    "diameter": pl.Float32(),
    "density": pl.Float32(),
    "gravity": pl.Float32(),
    "escapeVelocity": pl.Float32(),
    "rotationPeriod": pl.Float32(),
    "lengthOfDay": pl.Float32(),
    "distanceFromSun": pl.Float32(),
    "perihelion": pl.Float32(),
    "aphelion": pl.Float32(),
    "orbitalPeriod": pl.Float32(),
    "orbitalVelocity": pl.Float32(),
    "orbitalInclination": pl.Float32(),
    "orbitalEccentricity": pl.Float32(),
    "obliquityToOrbit": pl.Float32(),
    "meanTemperature": pl.Float32(),
    "surfacePressure": pl.Float32(),
    "numberOfMoons": pl.Int16(),
    "hasRingSystem": pl.Boolean(),
    "hasGlobalMagneticField": pl.Boolean(),
}
LARGEST_SATELLITES_SCHEMA = {
    "planet": pl.Utf8(),
    "gm": pl.Float64(),
    "radius": pl.Float64(),
    "density": pl.Float32(),
    "magnitude": pl.Float64(),
    "albedo": pl.Float64(),
    "OrbitsPlanetWithNiceTemp": pl.Boolean(),
}
PLANETS_RULES_PATH = str(get_test_file_path("planets/planets.dischema.json"))


@pytest.fixture(scope="function")
def planet_test_files() -> Iterator[str]:
    clear_config_cache()
    with tempfile.TemporaryDirectory() as tdir:
        shutil.copytree(get_test_file_path("planets/"), Path(tdir, "planets"))
        yield tdir + "/planets"


@pytest.fixture(scope="function")
def planet_data_after_file_transformation() -> Iterator[Tuple[SubmissionInfo, str]]:
    with tempfile.TemporaryDirectory() as tdir:
        submitted_file_info = SubmissionInfo(
            submission_id=uuid4().hex,
            file_name="doesnotmatter",
            dataset_id="planets",
            file_extension="json",
        )
        output_path = Path(tdir, submitted_file_info.submission_id, "transform", "planets")
        output_path.mkdir(parents=True)

        planet_contract_data = {
            "planet": "Earth",
            "mass": "5.97",
            "diameter": "12756.0",
            "density": "5514.0",
            "gravity": "9.8",
            "escapeVelocity": "11.2",
            "rotationPeriod": "23.9",
            "lengthOfDay": "24.0",
            "distanceFromSun": "149.6",
            "perihelion": "147.1",
            "aphelion": "152.1",
            "orbitalPeriod": "365.2",
            "orbitalVelocity": "29.8",
            "orbitalInclination": "0.0",
            "orbitalEccentricity": "0.017",
            "obliquityToOrbit": "23.4",
            "meanTemperature": "15.0",
            "surfacePressure": "1.0",
            "numberOfMoons": "1",
            "hasRingSystem": "false",
            "hasGlobalMagneticField": "false",
        }
        planet_contract_df = pl.DataFrame(
            planet_contract_data, {k: pl.Utf8() for k in planet_contract_data}
        )

        planet_contract_df.write_parquet(Path(output_path, "planets.parquet"))

        yield submitted_file_info, tdir


@pytest.fixture(scope="function")
def dodgy_planet_data_after_file_transformation() -> Iterator[Tuple[SubmissionInfo, str]]:
    with tempfile.TemporaryDirectory() as tdir:
        submitted_file_info = SubmissionInfo(
            submission_id=uuid4().hex,
            file_name="doesnotmatter",
            dataset_id="planets",
            file_extension="json",
        )
        output_path = Path(tdir, submitted_file_info.submission_id, "transform", "planets")
        output_path.mkdir(parents=True)

        planet_contract_data = {
            "planet": "EarthEarthEarthEarthEarthEarthEarthEarthEarth",
            "mass": "5.97",
            "diameter": "12756.0",
            "density": "5514.0",
            "gravity": "9.8",
            "escapeVelocity": "11.2",
            "rotationPeriod": "23.9",
            "lengthOfDay": "24.0",
            "distanceFromSun": "149.6",
            "perihelion": "147.1",
            "aphelion": "152.1",
            "orbitalPeriod": "365.2",
            "orbitalVelocity": "29.8",
            "orbitalInclination": "0.0",
            "orbitalEccentricity": "0.017",
            "obliquityToOrbit": "23.4",
            "meanTemperature": "15.0",
            "surfacePressure": "1.0",
            "numberOfMoons": "-1",
            "hasRingSystem": "false",
            "hasGlobalMagneticField": "sometimes",
        }
        planet_contract_df = pl.DataFrame(
            planet_contract_data, {k: pl.Utf8() for k in planet_contract_data}
        )

        planet_contract_df.write_parquet(Path(output_path, "planets.parquet"))

        yield submitted_file_info, tdir


@pytest.fixture(scope="function")
def planets_data_after_data_contract() -> Iterator[Tuple[SubmissionInfo, str]]:
    with tempfile.TemporaryDirectory() as tdir:
        submission_id = uuid4().hex
        submitted_file_info = SubmissionInfo(
            submission_id=submission_id,
            file_name="doesnotmatter",
            dataset_id="planets",
            file_extension="json",
        )
        output_path = Path(tdir, submitted_file_info.submission_id, "contract", "planets")
        output_path.mkdir(parents=True)

        planet_contract_data = {
            "planet": "Earth",
            "mass": 5.97,
            "diameter": 12756.0,
            "density": 5514.0,
            "gravity": 19.8,
            "escapeVelocity": 11.2,
            "rotationPeriod": 23.9,
            "lengthOfDay": 24.0,
            "distanceFromSun": 149.6,
            "perihelion": 147.1,
            "aphelion": 152.1,
            "orbitalPeriod": 1365.2,
            "orbitalVelocity": 29.8,
            "orbitalInclination": 0.0,
            "orbitalEccentricity": 0.017,
            "obliquityToOrbit": 23.4,
            "meanTemperature": 15.0,
            "surfacePressure": 1.0,
            "numberOfMoons": 1,
            "hasRingSystem": False,
            "hasGlobalMagneticField": False,
        }
        planet_contract_df = pl.DataFrame(planet_contract_data, PLANET_POLARS_SCHEMA)

        planet_contract_df.write_parquet(Path(output_path, "planets.parquet"))

        with open(Path(output_path, "_SUCCESS"), "w", encoding="utf-8") as sfile:
            sfile.write("")

        yield submitted_file_info, tdir


@pytest.fixture(scope="function")
def planets_data_after_data_contract_that_break_business_rules() -> Iterator[
    Tuple[SubmissionInfo, str]
]:  # pylint: disable=line-too-long
    with tempfile.TemporaryDirectory() as tdir:
        submission_id = uuid4().hex
        submitted_file_info = SubmissionInfo(
            submission_id=submission_id,
            file_name="doesnotmatter",
            dataset_id="planets",
            file_extension="json",
        )
        output_path = Path(tdir, submitted_file_info.submission_id, "contract", "planets")
        output_path.mkdir(parents=True)

        planet_contract_data = {
            "planet": "Earth",
            "mass": 5.97,
            "diameter": 12756.0,
            "density": 5514.0,
            "gravity": 9.8,
            "escapeVelocity": 11.2,
            "rotationPeriod": 23.9,
            "lengthOfDay": 24.0,
            "distanceFromSun": 149.6,
            "perihelion": 147.1,
            "aphelion": 152.1,
            "orbitalPeriod": 365.2,
            "orbitalVelocity": 29.8,
            "orbitalInclination": 0.0,
            "orbitalEccentricity": 0.017,
            "obliquityToOrbit": 23.4,
            "meanTemperature": 15.0,
            "surfacePressure": 1.0,
            "numberOfMoons": 1,
            "hasRingSystem": False,
            "hasGlobalMagneticField": False,
        }
        planet_contract_df = pl.DataFrame(planet_contract_data, PLANET_POLARS_SCHEMA)

        planet_contract_df.write_parquet(Path(output_path, "planets.parquet"))

        with open(Path(output_path, "_SUCCESS"), "w", encoding="utf-8") as sfile:
            sfile.write("")

        yield submitted_file_info, tdir


@pytest.fixture(scope="function")
def planets_data_after_business_rules() -> Iterator[Tuple[SubmissionInfo, str, SubmissionStatus]]:
    with tempfile.TemporaryDirectory() as tdir:
        shutil.copy(get_test_file_path("planets/planet_ruleset.json"), Path(tdir))
        submission_id = uuid4().hex
        submitted_file_info = SubmissionInfo(
            submission_id=submission_id,
            file_name="doesnotmatter",
            dataset_id="planets",
            file_extension="json",
        )
        output_path = Path(tdir, submitted_file_info.submission_id, "business_rules", "planets")
        output_path.mkdir(parents=True)

        data_post_br = {
            "largest_satellites_data": {
                "data": {
                    "planet": "Earth",
                    "gm": 4902.801,
                    "radius": 1737.5,
                    "density": 5514.0,
                    "magnitude": -12.74,
                    "albedo": 0.12,
                    "OrbitsPlanetWithNiceTemp": True,
                },
                "schema": LARGEST_SATELLITES_SCHEMA,
            },
            "Originalplanets": {
                "data": {
                    "planet": "Earth",
                    "mass": 5.96999979019165,
                    "diameter": 12756.0,
                    "density": 5514.0,
                    "gravity": 19.799999237060547,
                    "escapeVelocity": 11.199999809265137,
                    "rotationPeriod": 23.899999618530273,
                    "lengthOfDay": 24.0,
                    "distanceFromSun": 149.60000610351562,
                    "perihelion": 147.10000610351562,
                    "aphelion": 152.10000610351562,
                    "orbitalPeriod": 1365.199951171875,
                    "orbitalVelocity": 29.799999237060547,
                    "orbitalInclination": 0.0,
                    "orbitalEccentricity": 0.017000000923871994,
                    "obliquityToOrbit": 23.399999618530273,
                    "meanTemperature": 15.0,
                    "surfacePressure": 1.0,
                    "numberOfMoons": 1,
                    "hasRingSystem": False,
                    "hasGlobalMagneticField": False,
                },
                "schema": PLANET_POLARS_SCHEMA,
            },
            "planets": {
                "data": {
                    "planet": "Earth",
                    "mass": 5.96999979019165,
                    "diameter": 12756.0,
                    "density": 5514.0,
                    "gravity": 19.799999237060547,
                    "escapeVelocity": 11.199999809265137,
                    "rotationPeriod": 23.899999618530273,
                    "lengthOfDay": 24.0,
                    "distanceFromSun": 149.60000610351562,
                    "perihelion": 147.10000610351562,
                    "aphelion": 152.10000610351562,
                    "orbitalPeriod": 1365.199951171875,
                    "orbitalVelocity": 29.799999237060547,
                    "orbitalInclination": 0.0,
                    "orbitalEccentricity": 0.017000000923871994,
                    "obliquityToOrbit": 23.399999618530273,
                    "meanTemperature": 15.0,
                    "surfacePressure": 1.0,
                    "numberOfMoons": 1,
                    "hasRingSystem": False,
                    "hasGlobalMagneticField": False,
                    "__rowid__": "8ea32e8a-3587-473e-a5bd-0e2bdece62c6",
                    "gm": 4902.801,
                    "radius": 1737.5,
                    "magnitude": -12.74,
                    "albedo": 0.12,
                    "OrbitsPlanetWithNiceTemp": True,
                },
                "schema": {
                    **PLANET_POLARS_SCHEMA.copy(),
                    **{"__rowid__": pl.Utf8()},
                    **LARGEST_SATELLITES_SCHEMA.copy(),
                },
            },
        }
        for entity_name, data_schema in data_post_br.items():
            planet_contract_df = pl.DataFrame(data_schema["data"], data_schema["schema"])
            planet_contract_df.write_parquet(Path(output_path, f"{entity_name}.parquet"))

        submission_status = SubmissionStatus(False, 1)

        yield submitted_file_info, tdir, submission_status


@pytest.fixture(scope="function")
def error_data_after_business_rules() -> Iterator[Tuple[SubmissionInfo, str]]:
    with tempfile.TemporaryDirectory() as tdir:
        submission_id = uuid4().hex
        submitted_file_info = SubmissionInfo(
            submission_id=submission_id,
            file_name="doesnotmatter",
            dataset_id="planets",
            file_extension="json",
        )
        output_path = Path(tdir, submitted_file_info.submission_id, "errors")
        output_path.mkdir(parents=True)

        error_data = json.loads(
            """[
            {
                "Entity": "planets",
                "Key": "",
                "FailureType": "record",
                "Status": "error",
                "ErrorType": "record",
                "ErrorLocation": "orbitalPeriod",
                "ErrorMessage": "Planet has long orbital period",
                "ErrorCode": "LONG_ORBIT",
                "ReportingField": "orbitalPeriod",
                "Value": "365.20001220703125",
                "Category": "Bad value"
            },
            {
                "Entity": "planets",
                "Key": "",
                "FailureType": "record",
                "Status": "error",
                "ErrorType": "record",
                "ErrorLocation": "gravity",
                "ErrorMessage": "Planet has too strong gravity",
                "ErrorCode": "STRONG_GRAVITY",
                "ReportingField": "gravity",
                "Value": "9.800000190734863",
                "Category": "Bad value"
            }
        ]"""
        )
        output_file_path = output_path / "business_rules_errors.json"
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(error_data, f)

        yield submitted_file_info, tdir


def pl_row_count(df: pl.DataFrame) -> int:
    return df.select(pl.len()).to_dicts()[0]["len"]
