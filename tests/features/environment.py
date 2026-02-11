from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import shutil
import tempfile
from pathlib import Path
from typing import List, Optional

from behave.model import Scenario  # type: ignore
from behave.runner import Context  # type: ignore
from patches import get_ddb_connection, get_spark_session
from pyspark.sql.session import SparkSession

import dve.pipeline.utils
from dve.core_engine.engine import CoreEngine
from dve.parser.file_handling import add_implementation, scheme_is_supported
from dve.parser.file_handling.implementations import DBFSFilesystemImplementation


def before_all(context: Context):
    """Set up the tests."""
    context.spark_session = get_spark_session()

    context.connection, context.ddb_db_file = get_ddb_connection()

    context.dbfs_root: Optional[tempfile.TemporaryDirectory] = None
    if not scheme_is_supported(DBFSFilesystemImplementation.DBFS_SCHEME):
        context.dbfs_root = tempfile.TemporaryDirectory()

        temp_dir = Path(context.dbfs_root.__enter__())
        dbfs_impl = DBFSFilesystemImplementation(temp_dir)
        add_implementation(dbfs_impl)
    context.process_pool = ProcessPoolExecutor(cpu_count() - 1)


def before_scenario(context: Context, scenario: Scenario):
    """Set up scenarios for behave."""
    if "failing" in scenario.effective_tags:
        scenario.skip("This test is current failing and should be investigated")

    temp_dir = Path(tempfile.mkdtemp())
    temp_dir.mkdir(exist_ok=True)
    context.temp_dir = temp_dir
    context.temp_tables = []


def after_scenario(context: Context, scenario: Scenario):  # pylint: disable=unused-argument
    """Remove the output prefix."""
    engine = getattr(context, "validator", None)
    if engine:
        if isinstance(engine, CoreEngine):
            try:
                engine.__exit__(None, None, None)
            except:
                pass
            context.validator = None

    temp_dir = getattr(context, "temp_dir", None)
    if temp_dir:
        if isinstance(temp_dir, Path):
            try:
                shutil.rmtree(str(temp_dir))
            except:
                pass
            context.temp_dir = None

    spark_session: SparkSession = getattr(context, "spark_session")
    temp_tables: List[str] = getattr(context, "temp_tables")
    for table_name in temp_tables:
        spark_session.sql(f"DROP TABLE {table_name}")
    dve.pipeline.utils._configs = {}


def after_all(context: Context):
    """Tear down the tests."""
    dbfs_root: Optional[tempfile.TemporaryDirectory] = getattr(context, "dbfs_root")
    if dbfs_root:
        dbfs_root.__exit__(None, None, None)

    spark_session: SparkSession = getattr(context, "spark_session")
    spark_session.sql("DROP DATABASE dve CASCADE")

    context.connection.close()
    shutil.rmtree(context.ddb_db_file.parent)
    context.process_pool.shutdown(wait=True, cancel_futures=True)
