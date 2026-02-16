"""Tests for the core engine."""

import os.path
import warnings

# pylint: disable=protected-access,unused-import,redefined-outer-name
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from dve.common.error_utils import load_all_error_messages
from dve.core_engine.backends.implementations.spark.backend import SparkBackend
from dve.core_engine.backends.implementations.spark.reference_data import SparkRefDataLoader
from dve.core_engine.engine import CoreEngine

from ..conftest import get_test_file_path
from ..fixtures import spark, temp_dir


class TestCoreEngine:
    def test_dummy_planet_run(self, spark: SparkSession, temp_dir: str):
        """Test that we can still run the test example with the dummy planets."""
        config_path = get_test_file_path("planets/planets.dischema.json")
        refdata_loader = SparkRefDataLoader
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            test_instance = CoreEngine.build(
                dataset_config_path=config_path.as_posix(),
                output_prefix=Path(temp_dir),
                backend=SparkBackend(dataset_config_uri=config_path.parent.as_posix(),
                                     spark_session=spark,
                                     reference_data_loader=refdata_loader)
            )

            with test_instance:
                _, errors_uri = test_instance.run_pipeline(
                    entity_locations={
                        "planets": get_test_file_path("planets/planets_demo.csv").as_posix(),
                    },
                )

            critical_messages = [message for message in load_all_error_messages(errors_uri) if message.is_critical]
            assert not critical_messages

        output_files = Path(temp_dir).iterdir()
        check_dirs = []
        for path in output_files:
            full_path = os.path.join(temp_dir, path.name)
            if os.path.isdir(full_path):
                for dir_item in os.listdir(full_path):
                    if dir_item.startswith("part-0000") and dir_item.endswith("parquet"):
                        if path.name not in check_dirs:
                            check_dirs.append(path.name)
        assert sorted(check_dirs) == sorted(["planets", "largest_satellites"])

    def test_dummy_demographics_run(self, spark, temp_dir: str):
        """Test that we can still run the test example with the dummy demographics data."""
        config_path = get_test_file_path("demographics/basic_demographics.dischema.json").as_posix()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            test_instance = CoreEngine.build(
                dataset_config_path=config_path,
                output_prefix=Path(temp_dir),
            )

            with test_instance:
                _, errors_uri = test_instance.run_pipeline(
                    entity_locations={
                        "demographics": get_test_file_path(
                            "demographics/basic_demographics.csv"
                        ).as_posix(),
                    },
                )

            critical_messages = [message for message in load_all_error_messages(errors_uri) if message.is_critical]
            assert not critical_messages

        output_files = Path(temp_dir).iterdir()
        check_dirs = []
        for path in output_files:
            full_path = os.path.join(temp_dir, path.name)
            if os.path.isdir(full_path):
                for dir_item in os.listdir(full_path):
                    if dir_item.startswith("part-0000") and dir_item.endswith("parquet"):
                        if path.name not in check_dirs:
                            check_dirs.append(path.name)
        assert sorted(check_dirs) == sorted(["demographics"])

    def test_dummy_books_run(self, spark, temp_dir: str):
        """Test that we can handle files with more complex nested schemas."""
        config_path = get_test_file_path("books/nested_books.dischema.json").as_posix()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            test_instance = CoreEngine.build(
                dataset_config_path=config_path,
                output_prefix=Path(temp_dir),
            )
            with test_instance:
                _, errors_uri = test_instance.run_pipeline(
                    entity_locations={
                        "header": get_test_file_path("books/nested_books.xml").as_posix(),
                        "nested_books": get_test_file_path("books/nested_books.xml").as_posix(),
                    }
                )

            critical_messages = [message for message in load_all_error_messages(errors_uri) if message.is_critical]
            assert not critical_messages

        output_files = Path(temp_dir).iterdir()
        check_dirs = []
        for path in output_files:
            full_path = os.path.join(temp_dir, path.name)
            if os.path.isdir(full_path):
                for dir_item in os.listdir(full_path):
                    if dir_item.startswith("part-0000") and dir_item.endswith("parquet"):
                        if path.name not in check_dirs:
                            check_dirs.append(path.name)
        assert sorted(check_dirs) == sorted(["nested_books"])
