"""Global test fixtures."""

# pylint: disable=redefined-outer-name
import tempfile
from pathlib import Path
from typing import Iterator, Tuple
from uuid import uuid4

import boto3
import pytest
from duckdb import DuckDBPyConnection, connect
from moto import mock_s3  # type: ignore
from pyspark.sql import SparkSession

from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    PYTHON_TYPE_TO_DUCKDB_TYPE,
)
from dve.parser.file_handling.implementations import DBFSFilesystemImplementation
from dve.parser.file_handling.service import (
    add_implementation,
    is_supported,
    remove_prefix,
    scheme_is_supported,
)
from dve.pipeline.utils import unpersist_all_rdds

from .features.patches import get_spark_session


@pytest.fixture(scope="function")
def temp_dir() -> Iterator[str]:
    """A fixture providing a temporary directory."""
    with tempfile.TemporaryDirectory() as temp_dir_str:
        yield temp_dir_str


@pytest.fixture(scope="function")
def temp_dbfs_prefix() -> Iterator[str]:
    """A fixture providing a temporary DBFS prefix as a URI."""
    prefix = f"dbfs:/{uuid4().hex}"
    assert is_supported(prefix)
    try:
        yield prefix
    finally:
        remove_prefix(prefix, True)


@pytest.fixture(scope="function")
def temp_prefix() -> Iterator[str]:
    """A fixture providing a temporary directory as a URI."""
    with tempfile.TemporaryDirectory() as temp_dir_str:
        temp_dir_path = Path(temp_dir_str)
        yield temp_dir_path.as_uri()
        # So shutil doesn't complain if we remove the path ourselves.
        temp_dir_path.mkdir(exist_ok=True)


@pytest.fixture(scope="function")
def temp_s3_prefix() -> Iterator[str]:
    """A fixture providing a temporary S3 prefix as a URI."""
    bucket_name = uuid4().hex

    with mock_s3():
        connection = boto3.resource("s3", region_name="eu-west-2")
        bucket = connection.Bucket(bucket_name)
        bucket.create(CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        yield f"s3://{bucket_name}"

        for obj in bucket.objects.filter(Prefix=""):
            obj.delete()


# `autouse` ensures Spark session is set up with these reqs.
@pytest.fixture(scope="session", autouse=True)
def spark() -> Iterator[SparkSession]:
    """A spark session for the tests."""
    return get_spark_session()


# `autouse` ensures dbfs available even on Windows with no env set.
@pytest.fixture(scope="session", autouse=True)
def dbfs() -> Iterator[None]:
    """Add a DBFS implementation in a temp dir."""
    if scheme_is_supported(DBFSFilesystemImplementation.DBFS_SCHEME):
        yield None
    else:
        with tempfile.TemporaryDirectory() as temp_dir_str:
            temp_dir = Path(temp_dir_str)

            dbfs_impl = DBFSFilesystemImplementation(temp_dir)
            add_implementation(dbfs_impl)
            yield None
            temp_dir.mkdir(exist_ok=True)


@pytest.fixture(scope="function")
def spark_test_database(spark: SparkSession) -> Iterator[str]:
    """Test database at scope function to be utilised by individual unit tests"""
    db_name = uuid4().hex

    spark.sql(f"CREATE DATABASE {db_name};")

    yield db_name

    spark.sql(f"DROP DATABASE {db_name} CASCADE;")
    
    unpersist_all_rdds(spark)
    
    spark.catalog.clearCache()
    spark.sparkContext._jsc.clearJobGroup()
    spark.sparkContext.cancelAllJobs()
    
    


@pytest.fixture()
def temp_ddb_conn() -> Iterator[Tuple[Path, DuckDBPyConnection]]:
    """Temp DuckDB directory for the database"""
    db = uuid4().hex
    with tempfile.TemporaryDirectory(prefix="ddb_audit_testing") as tmp:
        db_file = Path(tmp, db + ".duckdb")
        conn = connect(database=db_file, read_only=False)
        yield db_file, conn
