"""Test SparkDVEPipeline object methods"""
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

from datetime import datetime
import json
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import time
from typing import Dict
from unittest.mock import Mock
from uuid import uuid4

import polars as pl
from pyspark.sql import SparkSession

from dve.common.error_utils import load_feedback_messages
from dve.core_engine.backends.base.auditing import FilterCriteria
from dve.core_engine.backends.implementations.spark.auditing import SparkAuditingManager
from dve.core_engine.backends.implementations.spark.reference_data import SparkRefDataLoader
from dve.core_engine.backends.implementations.spark.rules import SparkStepImplementations
from dve.core_engine.message import UserMessage
from dve.core_engine.models import ProcessingStatusRecord, SubmissionInfo, SubmissionStatisticsRecord
import dve.parser.file_handling as fh
from dve.pipeline.spark_pipeline import SparkDVEPipeline
from dve.pipeline.utils import SubmissionStatus

from ..conftest import get_test_file_path
from ..fixtures import spark, spark_test_database  # pylint: disable=unused-import
from .pipeline_helpers import (  # pylint: disable=unused-import
    PLANETS_RULES_PATH,
    dodgy_planet_data_after_file_transformation,
    error_data_after_business_rules,
    planet_data_after_file_transformation,
    planet_test_files,
    planets_data_after_business_rules,
    planets_data_after_data_contract,
    planets_data_after_data_contract_that_break_business_rules,
)


# def test_audit_received_step(planet_test_files, spark, spark_test_database):
#     with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark) as audit_tables:
#         dve_pipeline = SparkDVEPipeline(
#             processed_files_path=planet_test_files,
#             audit_tables=audit_tables,
#             job_run_id=1,
#             rules_path=None,
#             submitted_files_path=planet_test_files,
#             reference_data_loader=None,
#         )

#         sub_ids: Dict[str, SubmissionInfo] = {}
#         sub_files = dve_pipeline._get_submission_files_for_run()
#         for subs in sub_files:
#             sub_id = uuid4().hex
#             sub_info = dve_pipeline.audit_received_file(sub_id, *subs)
#             audit_tables.add_new_submissions([sub_info], 1)
#             audit_tables.mark_transform([sub_id])
#             sub_ids[sub_id] = sub_info

#     for sub in sub_ids:
#         sub_info = sub_ids[sub]
#         assert isinstance(sub_info, SubmissionInfo)
#         assert (
#             next(
#                 audit_tables._processing_status.conv_to_records(
#                     audit_tables.get_latest_processing_records(
#                         filter_criteria=[FilterCriteria("submission_id", sub_info.submission_id)]
#                     )
#                 )
#             ).processing_status
#             == "file_transformation"
#         )
#         audit_tbl_sub_info = audit_tables.get_submission_info(sub_info.submission_id)
#         assert audit_tbl_sub_info
#         assert audit_tbl_sub_info.file_name_with_ext == sub_info.file_name_with_ext


# def test_file_transformation_step(
#     spark: SparkSession,
#     spark_test_database: str,
#     planet_test_files: str,
# ):  # pylint: disable=redefined-outer-name
#     with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark) as audit_manager:
#         dve_pipeline = SparkDVEPipeline(
#             processed_files_path=planet_test_files,
#             audit_tables=audit_manager,
#             job_run_id=1,
#             rules_path=PLANETS_RULES_PATH,
#             submitted_files_path=planet_test_files,
#             reference_data_loader=None,
#             spark=spark,
#         )
#         sub_id = uuid4().hex

#         submitted_files = list(dve_pipeline._get_submission_files_for_run())[0]

#         submitted_file_info = dve_pipeline.audit_received_file(sub_id, *submitted_files)

#         # todo - probably worth doing more than one submission here

#         output_path = Path(planet_test_files, submitted_file_info.submission_id, "transform")

#         success, failed = dve_pipeline.file_transformation_step(
#             pool=ThreadPoolExecutor(2), submissions_to_process=[submitted_file_info]
#         )

#         assert len(success) == 1
#         assert len(failed) == 0

#     assert output_path.joinpath("planets").exists()

#     assert audit_manager.get_all_data_contract_submissions().count() == 1
#     audit_result = audit_manager.get_all_error_report_submissions()
#     assert len(audit_result[0]) == 0
#     assert len(audit_result[1]) == 0


# def test_apply_data_contract_success(
#     spark: SparkSession, planet_data_after_file_transformation
# ):  # pylint: disable=redefined-outer-name
#     sub_info, processed_file_path = planet_data_after_file_transformation
#     dve_pipeline = SparkDVEPipeline(
#         processed_files_path=processed_file_path,
#         audit_tables=None,
#         job_run_id=1,
#         rules_path=PLANETS_RULES_PATH,
#         submitted_files_path=None,
#         reference_data_loader=None,
#         spark=spark,
#     )
#     sub_status = SubmissionStatus()
#     sub_info, sub_status = dve_pipeline.apply_data_contract(sub_info, sub_status)

#     assert not sub_status.validation_failed

#     assert Path(Path(processed_file_path), sub_info.submission_id, "data_contract", "planets").exists()


def test_apply_data_contract_failed(  # pylint: disable=redefined-outer-name
    spark: SparkSession, dodgy_planet_data_after_file_transformation
):
    sub_info, processed_file_path = dodgy_planet_data_after_file_transformation
    dve_pipeline = SparkDVEPipeline(
        processed_files_path=processed_file_path,
        audit_tables=None,
        job_run_id=1,
        rules_path=PLANETS_RULES_PATH,
        submitted_files_path=None,
        reference_data_loader=None,
        spark=spark,
    )
    sub_status = SubmissionStatus()

    sub_info, sub_status = dve_pipeline.apply_data_contract(sub_info, sub_status)
    assert sub_status.validation_failed

    output_path = Path(processed_file_path) / sub_info.submission_id
    assert Path(output_path, "data_contract", "planets").exists()

    errors_path = Path(output_path, "errors", "data_contract_errors.jsonl")
    assert errors_path.exists()

    expected_errors = [
        {
            "Entity": "planets",
            "Key": "",
            "FailureType": "record",
            "Status": "error",
            "ErrorType": "value_error.any_str.max_length",
            "ErrorLocation": "planet",
            "ErrorMessage": "is invalid",
            "ErrorCode": "BadValue",
            "ReportingField": "planet",
            "Value": "EarthEarthEarthEarthEarthEarthEarthEarthEarth",
            "Category": "Bad value",
        },
        {
            "Entity": "planets",
            "Key": "",
            "FailureType": "record",
            "Status": "error",
            "ErrorType": "value_error.number.not_ge",
            "ErrorLocation": "numberOfMoons",
            "ErrorMessage": "is invalid",
            "ErrorCode": "BadValue",
            "ReportingField": "numberOfMoons",
            "Value": "-1",
            "Category": "Bad value",
        },
        {
            "Entity": "planets",
            "Key": "",
            "FailureType": "record",
            "Status": "error",
            "ErrorType": "type_error.bool",
            "ErrorLocation": "hasGlobalMagneticField",
            "ErrorMessage": "is invalid",
            "ErrorCode": "BadValue",
            "ReportingField": "hasGlobalMagneticField",
            "Value": "sometimes",
            "Category": "Bad value",
        },
    ]
    
    actual_errors = list(load_feedback_messages(errors_path.as_posix()))

    assert actual_errors == [UserMessage(**err) for err in expected_errors]


def test_data_contract_step(
    spark: SparkSession,
    planet_data_after_file_transformation,
    spark_test_database,
):  # pylint: disable=redefined-outer-name
    sub_info, processed_file_path = planet_data_after_file_transformation
    sub_status = SubmissionStatus()
    with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark) as audit_manager:
        dve_pipeline = SparkDVEPipeline(
            processed_files_path=processed_file_path,
            audit_tables=audit_manager,
            job_run_id=1,
            rules_path=PLANETS_RULES_PATH,
            submitted_files_path=None,
            reference_data_loader=None,
        )

        success, failed = dve_pipeline.data_contract_step(
            pool=ThreadPoolExecutor(2), file_transform_results=[(sub_info, sub_status)]
        )

        assert len(success) == 1
        assert not success[0][1].validation_failed
        assert len(failed) == 0

        assert Path(processed_file_path, sub_info.submission_id, "data_contract", "planets").exists()

    assert audit_manager.get_all_business_rule_submissions().count() == 1
    audit_result = audit_manager.get_all_error_report_submissions()
    assert len(audit_result[0]) == 0
    assert len(audit_result[1]) == 0


def test_apply_business_rules_success(
    spark: SparkSession, planets_data_after_data_contract, spark_test_database
):  # pylint: disable=redefined-outer-name
    sub_info, processed_file_path = planets_data_after_data_contract

    SparkRefDataLoader.spark = spark
    SparkRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)

    with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark) as audit_manager:
        dve_pipeline = SparkDVEPipeline(
            processed_files_path=processed_file_path,
            audit_tables=audit_manager,
            job_run_id=1,
            rules_path=PLANETS_RULES_PATH,
            submitted_files_path=None,
            reference_data_loader=SparkRefDataLoader,
            spark=spark,
        )

        _, status = dve_pipeline.apply_business_rules(sub_info, SubmissionStatus())

    assert not status.validation_failed
    assert status.number_of_records == 1

    planets_entity_path = Path(
        Path(processed_file_path), sub_info.submission_id, "business_rules", "planets"
    )
    assert planets_entity_path.exists()
    assert spark.read.parquet(str(planets_entity_path)).count() == 1

    largest_satellites_entity_path = Path(
        Path(processed_file_path), sub_info.submission_id, "business_rules", "largest_satellites"
    )
    assert largest_satellites_entity_path.exists()
    assert spark.read.parquet(str(largest_satellites_entity_path)).count() == 1

    og_planets_entity_path = Path(
        Path(processed_file_path), sub_info.submission_id, "business_rules", "Originalplanets"
    )
    assert og_planets_entity_path.exists()
    assert spark.read.parquet(str(og_planets_entity_path)).count() == 1


def test_apply_business_rules_with_data_errors(  # pylint: disable=redefined-outer-name
    spark: SparkSession,
    planets_data_after_data_contract_that_break_business_rules,
    spark_test_database,
):
    sub_info, processed_file_path = planets_data_after_data_contract_that_break_business_rules

    SparkRefDataLoader.spark = spark
    SparkRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)
    
    with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark) as audit_manager:
        dve_pipeline = SparkDVEPipeline(
            processed_files_path=processed_file_path,
            audit_tables=audit_manager,
            job_run_id=1,
            rules_path=PLANETS_RULES_PATH,
            submitted_files_path=None,
            reference_data_loader=SparkRefDataLoader,
            spark=spark,
        )

        _, status = dve_pipeline.apply_business_rules(sub_info, SubmissionStatus())

    assert status.validation_failed
    assert status.number_of_records == 1

    br_path = Path(
        Path(processed_file_path),
        sub_info.submission_id,
        "business_rules",
    )

    planets_entity_path = br_path / "planets"
    assert planets_entity_path.exists()
    assert spark.read.parquet(str(planets_entity_path)).count() == 0

    largest_satellites_entity_path = br_path / "largest_satellites"
    assert largest_satellites_entity_path.exists()
    assert spark.read.parquet(str(largest_satellites_entity_path)).count() == 1

    og_planets_entity_path = br_path / "Originalplanets"
    assert og_planets_entity_path.exists()
    assert spark.read.parquet(str(og_planets_entity_path)).count() == 1

    errors_path = Path(br_path.parent, "errors", "business_rules_errors.jsonl")
    assert errors_path.exists()

    expected_errors = [
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
            "Category": "Bad value",
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
            "Category": "Bad value",
        },
    ]
    
    actual_errors = list(load_feedback_messages(errors_path.as_posix()))

    assert actual_errors == [UserMessage(**err) for err in expected_errors]


def test_business_rule_step(
    spark: SparkSession,
    planets_data_after_data_contract,
    spark_test_database,
):  # pylint: disable=redefined-outer-name
    sub_info, processed_files_path = planets_data_after_data_contract

    SparkRefDataLoader.spark = spark
    SparkRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)

    with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark) as audit_manager:
        dve_pipeline = SparkDVEPipeline(
            processed_files_path=processed_files_path,
            audit_tables=audit_manager,
            job_run_id=1,
            rules_path=PLANETS_RULES_PATH,
            submitted_files_path=None,
            reference_data_loader=SparkRefDataLoader,
            spark=spark,
        )
        audit_manager.add_new_submissions([sub_info], job_run_id=1)

        successful_files, unsuccessful_files, failed_processing = dve_pipeline.business_rule_step(
            pool=ThreadPoolExecutor(2), files=[(sub_info, SubmissionStatus())]
        )

    assert len(successful_files) == 1
    assert len(unsuccessful_files) == 0
    assert len(failed_processing) == 0

    assert Path(processed_files_path, sub_info.submission_id, "business_rules", "planets").exists()

    audit_result = audit_manager.get_all_error_report_submissions()

    assert len(audit_result[0]) == 1
    assert len(audit_result[1]) == 0


def test_error_report_where_report_is_expected(  # pylint: disable=redefined-outer-name
    spark: SparkSession, error_data_after_business_rules
):
    sub_info, processed_file_path = error_data_after_business_rules

    SparkRefDataLoader.spark = spark

    dve_pipeline = SparkDVEPipeline(
        processed_files_path=processed_file_path,
        audit_tables=None,
        job_run_id=1,
        rules_path=PLANETS_RULES_PATH,
        submitted_files_path=None,
        reference_data_loader=SparkRefDataLoader,
        spark=spark,
    )

    submission_info, status, stats, report_uri = dve_pipeline.error_report(
        sub_info, SubmissionStatus(True, 9)
    )

    assert status.validation_failed

    expected = {
        "submission_id": submission_info.submission_id,
        "record_count": 9,
        "number_record_rejections": 2,
        "number_warnings": 0,
    }

    sub_stats = stats.dict()

    assert all([expected.get(key) == sub_stats.get(key) for key in expected])

    assert Path(report_uri).exists()

    report_records = (
        pl.read_excel(report_uri)
        .filter(pl.col("Data Summary").is_not_null())
        .select(pl.col("Data Summary"), pl.col("_duplicated_0"))
        .rows()
    )
    assert report_records == [
        ("Status", "File has been rejected"),
        ("Submission Id", submission_info.submission_id),
        ("Dataset Id", "planets"),
        ("File Name", "doesnotmatter"),
        ("File Extension", "json"),
        ("Submission Failure", "2"),
        ("Warning", "0"),
    ]

    error_summary_records = sorted(
        [
            OrderedDict(**record)
            for record in (pl.read_excel(report_uri, sheet_name="Error Summary").to_dicts())
        ],
        key=lambda x: x.get("Error Code"),
    )

    assert error_summary_records == sorted(
        [
            OrderedDict(
                **{
                    "Type": "Submission Failure",
                    "Group": "planets",
                    "Data Item Submission Name": "orbitalPeriod",
                    "Category": "Bad value",
                    "Error Code": "LONG_ORBIT",
                    "Count": 1,
                }
            ),
            OrderedDict(
                **{
                    "Type": "Submission Failure",
                    "Group": "planets",
                    "Data Item Submission Name": "gravity",
                    "Category": "Bad value",
                    "Error Code": "STRONG_GRAVITY",
                    "Count": 1,
                }
            ),
        ],
        key=lambda x: x.get("Error Code"),
    )

    error_data_records = [
        OrderedDict(**record)
        for record in (pl.read_excel(report_uri, sheet_name="Error Data").to_dicts())
    ]
    assert error_data_records == [
        OrderedDict(
            **{
                "Group": "planets",
                "Type": "Submission Failure",
                "Error Code": "LONG_ORBIT",
                "Data Item Submission Name": "orbitalPeriod",
                "Errors and Warnings": "Planet has long orbital period",
                "Value": 365.20001220703125,
                "ID": None,
                "Category": "Bad value",
            }
        ),
        OrderedDict(
            **{
                "Group": "planets",
                "Type": "Submission Failure",
                "Error Code": "STRONG_GRAVITY",
                "Data Item Submission Name": "gravity",
                "Errors and Warnings": "Planet has too strong gravity",
                "Value": 9.800000190734863,
                "ID": None,
                "Category": "Bad value",
            }
        ),
    ]


def test_error_report_step(
    spark: SparkSession,
    planets_data_after_business_rules,
    spark_test_database,
):  # pylint: disable=redefined-outer-name
    submitted_file_info, processed_files_path, status = planets_data_after_business_rules

    with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark) as audit_manager:
        dve_pipeline = SparkDVEPipeline(
            processed_files_path=processed_files_path,
            audit_tables=audit_manager,
            job_run_id=1,
            rules_path=None,
            submitted_files_path=None,
            reference_data_loader=None,
            spark=spark,
        )

        reports = dve_pipeline.error_report_step(
            pool=ThreadPoolExecutor(2),
            processed=[(submitted_file_info, status)],
            failed_file_transformation=[],
        )

    assert len(reports) == 1

    audit_result = audit_manager.get_current_processing_info(submitted_file_info.submission_id)
    assert audit_result.processing_status == "success"


def test_cluster_pipeline_run(
    spark: SparkSession, planet_test_files: str, spark_test_database
):  # pylint: disable=redefined-outer-name
    SparkRefDataLoader.spark = spark
    SparkRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)
    audit_manager = SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark)

    dve_pipeline = SparkDVEPipeline(
        processed_files_path=planet_test_files,
        audit_tables=audit_manager,
        job_run_id=1,
        rules_path=PLANETS_RULES_PATH,
        submitted_files_path=planet_test_files,
        reference_data_loader=SparkRefDataLoader,
        spark=spark,
    )

    error_reports = dve_pipeline.cluster_pipeline_run(2)

    # todo - probably need some more assertions/checks here - but for now, it's ok.
    for subinfo, _status, stats, report_uri in error_reports:
        report_processing_result = audit_manager.get_current_processing_info(subinfo.submission_id)

        assert report_processing_result.processing_status == "success"
        assert Path(report_uri).exists()

def test_get_submission_status(spark, spark_test_database):
    with SparkAuditingManager(spark_test_database, ThreadPoolExecutor(1), spark=spark) as audit_manager:
        dve_pipeline = SparkDVEPipeline(
            processed_files_path="a_path",
            audit_tables=audit_manager,
            job_run_id=1,
            rules_path=None,
            submitted_files_path=None,
            reference_data_loader=None,
            spark=spark,
        )
        dve_pipeline._logger = Mock(spec=logging.Logger)
         # add four submissions
        sub_one = SubmissionInfo(
            submission_id="1",
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            submission_method="sftp",
            file_extension="xml",
            file_size=12345,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )
        sub_two = SubmissionInfo(
            submission_id="2",
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            submission_method="sftp",
            file_extension="xml",
            file_size=12345,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )
        
        audit_manager.add_new_submissions([sub_one, sub_two])
        audit_manager.add_processing_records(
            [
                ProcessingStatusRecord(
                    submission_id=sub_one.submission_id, processing_status="error_report", submission_result="validation_failed"
                ),
                ProcessingStatusRecord(
                    submission_id=sub_two.submission_id, processing_status="failed", submission_result="processing_failed"
                ),
            ]
        )
        audit_manager.add_submission_statistics_records([
            SubmissionStatisticsRecord(submission_id=sub_one.submission_id, record_count=5, number_record_rejections=2, number_warnings=3),
        ])
    
    sub_stats_one = dve_pipeline.get_submission_status("test", sub_one.submission_id)
    assert sub_stats_one.submission_result == "validation_failed"
    assert sub_stats_one.validation_failed
    assert not sub_stats_one.processing_failed
    assert sub_stats_one.number_of_records == 5
    sub_stats_two = dve_pipeline.get_submission_status("test", sub_two.submission_id)
    assert sub_stats_two.submission_result == "processing_failed"
    assert not sub_stats_two.validation_failed
    assert sub_stats_two.processing_failed
    sub_stats_3 = dve_pipeline.get_submission_status("test", "3")
    dve_pipeline._logger.warning.assert_called_once_with(
        "Unable to determine status of submission_id: 3 in service test - assuming no issues."
            )
    assert sub_stats_3
