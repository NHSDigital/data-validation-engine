# pylint: disable=line-too-long,protected-access,wrong-import-order
# isort: skip_file
"""
Steps which involve actually kicking off the interesting stages
of the pipeline (e.g. data contract / transformations).

"""
# pylint: disable=no-name-in-module
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Callable, Dict, Optional
from uuid import uuid4
from behave import given, then, when  # type: ignore
from behave.model import Row, Table
from behave.runner import Context  # type: ignore
from dve.core_engine.backends.implementations.duckdb.reference_data import DuckDBRefDataLoader
from dve.core_engine.models import SubmissionInfo
from dve.core_engine.type_hints import URI
import duckdb

import context_tools as ctxt
import dve.parser.file_handling.service as fh
from dve.pipeline.utils import SubmissionStatus, load_config

import polars as pl
from pyspark.sql import SparkSession
from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager
from dve.core_engine.backends.implementations.spark.auditing import SparkAuditingManager
from dve.core_engine.backends.implementations.spark.rules import SparkStepImplementations
from dve.core_engine.backends.implementations.spark.reference_data import SparkRefDataLoader
from dve.pipeline.duckdb_pipeline import DDBDVEPipeline
from dve.pipeline.spark_pipeline import SparkDVEPipeline

from utilities import (
    ERROR_DF_FIELDS,
    load_errors_from_service,
    get_test_file_path,
    SERVICE_TO_STORAGE_PATH_MAPPING,
    get_all_errors_df,
)


def setup_spark_pipeline(
    spark: SparkSession,
    dataset_id: str,
    processing_path: Path,
    schema_file_name: Optional[str] = None,
):

    schema_file_name = f"{dataset_id}.dischema.json" if not schema_file_name else schema_file_name
    rules_path = get_test_file_path(f"{dataset_id}/{schema_file_name}").resolve().as_uri()
    # configure reference data
    SparkRefDataLoader.spark = spark
    SparkRefDataLoader.dataset_config_uri = fh.get_parent(rules_path)
    
    return SparkDVEPipeline(
        audit_tables=SparkAuditingManager(
            database="dve",
            spark=spark,
        ),
        job_run_id=12345,
        rules_path=rules_path,
        processed_files_path=processing_path.as_uri(),
        submitted_files_path=processing_path.as_uri(),
        reference_data_loader=SparkRefDataLoader,
        spark=spark,
    )


def setup_duckdb_pipeline(
    connection: duckdb.DuckDBPyConnection,
    dataset_id: str,
    processing_path: Path,
    schema_file_name: Optional[str] = None,
):

    schema_file_name = f"{dataset_id}.dischema.json" if not schema_file_name else schema_file_name
    rules_path = get_test_file_path(f"{dataset_id}/{schema_file_name}").resolve().as_uri()
    # create duckdbpyconnection with dve database file in context.tempdir
    # TODO - doesn't like file scheme - need to provide absolute path
    db_file = Path(processing_path, "dve.duckdb")
    # configure refdata
    DuckDBRefDataLoader.connection = connection
    DuckDBRefDataLoader.dataset_config_uri = fh.get_parent(rules_path)
    return DDBDVEPipeline(
        audit_tables=DDBAuditingManager(
            database_uri=db_file.as_posix(),
            # pool=ThreadPoolExecutor(1),
            connection=connection,
        ),
        job_run_id=12345,
        connection=connection,
        rules_path=rules_path,
        processed_files_path=processing_path.as_posix(),
        submitted_files_path=processing_path.as_posix(),
        reference_data_loader=DuckDBRefDataLoader,
    )


@when("I run the file transformation phase")
def run_file_transformation_step(context: Context):
    """Apply the file transformation stage"""
    pipeline = ctxt.get_pipeline(context)
    _success, failed = pipeline.file_transformation_step(
        pool=ThreadPoolExecutor(1), submissions_to_process=[ctxt.get_submission_info(context)]
    )
    if failed:
        ctxt.set_failed_file_transformation(context, failed[0])


@when("I run the data contract phase")
def apply_data_contract_with_error(context: Context):
    """Apply the data contract stage"""
    pipeline = ctxt.get_pipeline(context)
    pipeline.data_contract_step(
        pool=ThreadPoolExecutor(1), file_transform_results=[ctxt.get_submission_info(context)]
    )


@when("I run the business rules phase")
def apply_business_rules(context: Context):
    """Apply the business rules to the data."""

    pipeline = ctxt.get_pipeline(context)
    sub_info = ctxt.get_submission_info(context)
    proc_rec = pipeline._audit_tables.get_current_processing_info(sub_info.submission_id)
    # add row count here so sub stats calculated
    success, failed, _ = pipeline.business_rule_step(
        pool=ThreadPoolExecutor(1), files=[(sub_info, proc_rec.submission_result == "failed")]
    )
    assert len(success + failed) == 1
    sub_status = (success + failed)[0][1]
    ctxt.set_submission_status(context, sub_status)


@when("I run the error report phase")
def create_error_report(context: Context):
    """Produce error report for submission"""
    pipeline = ctxt.get_pipeline(context)

    try:
        failed_file_transformation = [ctxt.get_failed_file_transformation(context)]
        processed = []
    except AttributeError:
        sub_info = ctxt.get_submission_info(context)
        processed = [(sub_info, ctxt.get_submission_status(context))]
        failed_file_transformation = []

    pipeline.error_report_step(
        pool=ThreadPoolExecutor(1),
        processed=processed,
        failed_file_transformation=failed_file_transformation,
    )


@then("there are {expected_num_errors:d} record rejections from the {service} phase")
@then("there is {expected_num_errors:d} record rejection from the {service} phase")
@then("there are no record rejections from the {service} phase")
def get_record_rejects_from_service(context: Context, service: str, expected_num_errors: int = 0):
    processing_path = ctxt.get_processing_location(context)
    message_df = load_errors_from_service(processing_path, service)
    num_rejections = message_df.filter(pl.col("FailureType").eq("record")).shape[0]
    assert num_rejections == expected_num_errors, f"Got {num_rejections} actual rejections"


@given("A {implementation} pipeline is configured")
@given("A {implementation} pipeline is configured with schema file '{schema_file_name}'")
def add_pipeline_to_ctx(
    context: Context, implementation: str, schema_file_name: Optional[str] = None
):
    pipeline_map: Dict[str, Callable] = {
        "duckdb": partial(setup_duckdb_pipeline, connection=context.connection),
        "spark": partial(setup_spark_pipeline, spark=context.spark_session),
    }
    if not implementation in pipeline_map:
        raise ValueError(f"Selected implementation ({implementation}) not currently supported.")
    pipeline = pipeline_map[implementation](
        dataset_id=ctxt.get_submission_info(context).dataset_id,
        processing_path=ctxt.get_temp_dir(context),
        schema_file_name=schema_file_name,
    )
    ctxt.set_pipeline(context, pipeline)


@given("I submit the {dataset} file {file_name} for processing")
def submit_file_for_processing(context: Context, dataset: str, file_name: str):
    # create submission_info
    sub_info = {
        "submission_id": uuid4().hex,
        "dataset_id": dataset,
        "file_name": file_name,
        "file_extension": Path(file_name).suffix,
    }
    ctxt.set_submission_info(context, SubmissionInfo(**sub_info))  # type: ignore
    # add processing location
    processing_location = Path(
        ctxt.get_temp_dir(context), ctxt.get_submission_info(context).submission_id
    )
    processing_location.mkdir(exist_ok=True)
    ctxt.set_processing_location(context, processing_location)
    # get test data path
    file_path = fh.resolve_location(get_test_file_path(f"{dataset}/{file_name}"))
    target_path = fh.resolve_location(
        Path(ctxt.get_processing_location(context), file_name).as_uri()
    )
    # copy to processing_location
    fh.copy_resource(file_path, target_path)


@when("I submit the {dataset} file {file_name} for processing with metadata")
def submit_file_for_processing_with_meta(context: Context, dataset: str, file_name: str):
    # add processing location
    processing_location = Path(
        ctxt.get_temp_dir(context), ctxt.get_submission_info(context).submission_id
    )
    processing_location.mkdir(exist_ok=True)
    ctxt.set_processing_location(context, processing_location)
    # create submission_info
    table: Optional[Table] = context.table
    if table is None:
        raise ValueError("No table supplied in step")
    sub_info = {
        "submission_id": uuid4().hex,
        "dataset_id": dataset,
        "file_name": Path(file_name).stem,
        "file_extension": Path(file_name).suffix,
    }
    row: Row
    for row in table:
        record: Dict[str, str] = row.as_dict()
        sub_info[record["parameter"]] = record["value"]
    ctxt.set_submission_info(context, SubmissionInfo(**sub_info))
    # get test data path
    file_path = fh.resolve_location(get_test_file_path(f"{dataset}/{file_name}").as_uri())
    target_path = fh.resolve_location(
        Path(ctxt.get_processing_location(context), file_name).as_uri()
    )
    # copy to processing_location
    fh.copy_resource(file_path, target_path)


@then("the {entity_name} entity is stored as a parquet after the {service} phase")
def check_entity_stored_as_parquet_after_phase(context: Context, entity_name: str, service: str):
    _src = Path(
        ctxt.get_processing_location(context),
        SERVICE_TO_STORAGE_PATH_MAPPING.get(service, service),
        entity_name,
    )

    assert fh.get_resource_exists(fh.resolve_location(_src.as_uri())) or (
        _src.exists() and _src.is_dir()
    )


@then('At least one row from "{entity_name}" has generated error code "{error_code}"')
def check_rows_removed_with_error_code(context: Context, entity_name: str, error_code: str):
    err_df = get_all_errors_df(context)

    recs_with_err_code = err_df.filter(
        (pl.col("Entity").eq(entity_name)) & (pl.col("ErrorCode").eq(error_code))
    ).shape[0]
    assert recs_with_err_code >= 1


@then('At least one row from "{entity_name}" has  generated error category "{category}"')
def check_rows_eq_to_category(context: Context, entity_name: str, category: str):
    """Check number error message rows equivalent to a given value against a given category."""
    err_df = get_all_errors_df(context)

    recs_with_err_code = err_df.filter(
        (pl.col("Entity").eq(entity_name)) & (pl.col("Category").eq(category))
    ).shape[0]
    assert recs_with_err_code >= 1
