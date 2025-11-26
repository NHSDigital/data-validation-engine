"""Patches to be applied for tests."""

import os
import re
import tempfile
from pathlib import Path
from typing import Tuple

from duckdb import DuckDBPyConnection, connect
from pyspark.sql import DataFrame, SparkSession

from dve.core_engine.backends.implementations.spark import SparkRefDataLoader
from dve.core_engine.backends.implementations.spark.types import SparkEntities
from dve.core_engine.type_hints import EntityName
from dve.parser.file_handling import get_file_name, iter_prefix

TABLE_CACHE: SparkEntities = {}
"""A cache of already-loaded Spark refdata tables."""
TEST_DATA_ROOT: Path = Path(__file__).parent.parent.joinpath("testdata")
"""The root of the test data."""


def load_entity(
    self: SparkRefDataLoader,
    entity_name: EntityName,
) -> DataFrame:
    """
    Find a reference data table in the test files.

    This is a patch for `SparkRefDataLoader.load_entity`

    """
    entity_config = self.reference_entity_config[entity_name]
    table_name = entity_config.table_name
    database = entity_config.database

    try:
        return TABLE_CACHE[database, table_name]
    except KeyError:
        pass

    # Work out which naming convention we're using (split by dataset ID, or general).
    match = re.search(
        r"^(?:(?P<dataset_type>refdata)_)?(?P<dataset_id>[A-Za-z0-9]+)_(?P<reference_table>\w+)",
        table_name,
    )
    if match is None:  # Bare table name.
        reference_table = table_name
        prefix = TEST_DATA_ROOT
    else:  # Using 'refdata_${dataset_id}_${reference_table}' convention.
        dataset_id = match.group("dataset_id")
        reference_table = match.group("reference_table")
        prefix = TEST_DATA_ROOT.joinpath(dataset_id)

    matches = []
    for uri, _ in iter_prefix(prefix.as_uri(), recursive=True):
        uri = uri.rstrip("/")  # Strip trailing '/' for directory nodes.

        if get_file_name(uri) == f"{reference_table}.parquet":
            matches.append(uri)

    try:
        if not matches:
            raise ValueError(f"No matching refdata tables for {reference_table!r} in {prefix!r}")
        if len(matches) > 1:
            raise ValueError(
                f"Multiple matching refdata tables for {reference_table!r} in {prefix!r}"
            )
    except ValueError:
        if database is None:
            full_table_name = table_name
        else:
            full_table_name = f"{database}.{table_name}"

        try:
            return self.spark.table(full_table_name)
        except Exception:  # pylint: disable=broad-except
            pass
        raise

    table = self.spark.read.parquet(matches[0])
    TABLE_CACHE[database, table_name] = table
    return table


def get_spark_session() -> SparkSession:
    """Get a configured Spark Session. This MUST be called before any other Spark session is created."""
    temp_dir = tempfile.mkdtemp()
    os.environ["PYSPARK_SUBMIT_ARGS"] = " ".join(
        [
            "--packages",
            "com.databricks:spark-xml_2.12:0.16.0,io.delta:delta-core_2.12:2.4.0",
            "pyspark-shell",
        ]
    )
    spark_session = (
        SparkSession.builder.config("spark.sql.warehouse.dir", temp_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    # Apply patch to reference data to load local testdata rather than source from metastore
    #    SparkRefDataLoader.load_entity = load_entity  # type: ignore
    # Create the 'dve' database
    spark_session.sql("CREATE DATABASE IF NOT EXISTS dve")

    return spark_session


def get_ddb_connection() -> Tuple[DuckDBPyConnection, Path]:
    temp_dir = tempfile.mkdtemp()
    db_file = Path(temp_dir, "dve.duckdb")
    return (
        connect(
            database=db_file,
            config={
                "access_mode": "READ_WRITE",
                "default_null_order": "NULLS_LAST",
                "threads": 1,
            },
        ),
        db_file,
    )
