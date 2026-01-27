"""Test DuckDBPipeline object methods"""
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
from pathlib import Path
import shutil
from typing import Dict, Tuple
from uuid import uuid4
from unittest.mock import Mock

import pytest
from duckdb import DuckDBPyConnection

from dve.core_engine.backends.base.auditing import FilterCriteria
from dve.core_engine.backends.utilities import pl_row_count
from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager
from dve.core_engine.backends.implementations.duckdb.reference_data import DuckDBRefDataLoader
from dve.core_engine.models import ProcessingStatusRecord, SubmissionInfo, SubmissionStatisticsRecord
import dve.parser.file_handling as fh
from dve.pipeline.duckdb_pipeline import DDBDVEPipeline
from dve.pipeline.utils import SubmissionStatus

from ..conftest import get_test_file_path
from ..fixtures import temp_ddb_conn  # pylint: disable=unused-import
from .pipeline_helpers import (  # pylint: disable=unused-import
    PLANETS_RULES_PATH,
    planet_data_after_file_transformation,
    planet_test_files,
    planets_data_after_business_rules,
    planets_data_after_data_contract,
)


def test_audit_received_step(
    planet_test_files: str, temp_ddb_conn: Tuple[Path, DuckDBPyConnection]
):  # pylint: disable=redefined-outer-name
    db_file, conn = temp_ddb_conn
    with DDBAuditingManager(db_file.as_uri(), ThreadPoolExecutor(1), conn) as audit_manager:
        dve_pipeline = DDBDVEPipeline(
            processed_files_path=planet_test_files,
            audit_tables=audit_manager,
            job_run_id=1,
            connection=conn,
            rules_path=None,
            submitted_files_path=planet_test_files,
        )

        sub_ids: Dict[str, SubmissionInfo] = {}
        sub_files = dve_pipeline._get_submission_files_for_run()
        for subs in sub_files:
            sub_id = uuid4().hex
            sub_info = dve_pipeline.audit_received_file(sub_id, *subs)
            audit_manager.add_new_submissions([sub_info], 1)
            audit_manager.mark_transform([sub_id])
            sub_ids[sub_id] = sub_info

    for sub_info in sub_ids.values():
        assert isinstance(sub_info, SubmissionInfo)
        assert (
            next(
                audit_manager._processing_status.conv_to_records(
                    audit_manager.get_latest_processing_records(
                        filter_criteria=[FilterCriteria("submission_id", sub_info.submission_id)]
                    )
                )
            ).processing_status
            == "file_transformation"
        )
        audit_tbl_sub_info = audit_manager.get_submission_info(sub_info.submission_id)
        assert audit_tbl_sub_info
        assert audit_tbl_sub_info.file_name_with_ext == sub_info.file_name_with_ext


def test_file_transformation_step(
    planet_test_files: str, temp_ddb_conn: Tuple[Path, DuckDBPyConnection]
):  # pylint: disable=redefined-outer-name
    db_file, conn = temp_ddb_conn
    with DDBAuditingManager(db_file.as_uri(), ThreadPoolExecutor(1), conn) as audit_manager:
        dve_pipeline = DDBDVEPipeline(
            processed_files_path=planet_test_files,
            audit_tables=audit_manager,
            job_run_id=1,
            connection=conn,
            rules_path=get_test_file_path("planets/planets_ddb.dischema.json").as_posix(),
            submitted_files_path=planet_test_files,
        )

        sub_id = uuid4().hex
        submitted_files = list(dve_pipeline._get_submission_files_for_run())[0]
        submitted_files_info = dve_pipeline.audit_received_file(sub_id, *submitted_files)

        output_path = Path(planet_test_files, sub_id, "transform")

        success, failed = dve_pipeline.file_transformation_step(
            pool=ThreadPoolExecutor(2), submissions_to_process=[submitted_files_info]
        )

        assert len(success) == 1
        assert len(failed) == 0

    assert output_path.joinpath("planets").exists()
    assert pl_row_count(audit_manager.get_all_data_contract_submissions().pl()) == 1

    audit_result = audit_manager.get_all_error_report_submissions()
    assert len(audit_result[0]) == 0
    assert len(audit_result[1]) == 0


def test_data_contract_step(
    planet_data_after_file_transformation: Tuple[SubmissionInfo, str],
    temp_ddb_conn: Tuple[Path, DuckDBPyConnection],
):  # pylint: disable=redefined-outer-name
    db_file, conn = temp_ddb_conn
    sub_info, processed_file_path = planet_data_after_file_transformation
    with DDBAuditingManager(db_file.as_uri(), ThreadPoolExecutor(1), conn) as audit_manager:
        dve_pipeline = DDBDVEPipeline(
            processed_files_path=processed_file_path,
            audit_tables=audit_manager,
            job_run_id=1,
            connection=conn,
            rules_path=PLANETS_RULES_PATH,
            submitted_files_path=None,
        )

        success, failed = dve_pipeline.data_contract_step(
            pool=ThreadPoolExecutor(2), file_transform_results=[(sub_info, SubmissionStatus())]
        )

        assert len(success) == 1
        assert not success[0][1].validation_failed
        assert len(failed) == 0
        assert Path(processed_file_path, sub_info.submission_id, "contract", "planets").exists()

    assert pl_row_count(audit_manager.get_all_business_rule_submissions().pl()) == 1

    audit_result = audit_manager.get_all_error_report_submissions()
    assert len(audit_result[0]) == 0
    assert len(audit_result[1]) == 0


def test_business_rule_step(
    planets_data_after_data_contract,
    temp_ddb_conn,
):  # pylint: disable=redefined-outer-name
    db_file, conn = temp_ddb_conn
    sub_info, processed_files_path = planets_data_after_data_contract

    DuckDBRefDataLoader.connection = conn
    DuckDBRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)

    with DDBAuditingManager(db_file.as_uri(), ThreadPoolExecutor(1), conn) as audit_manager:
        dve_pipeline = DDBDVEPipeline(
            processed_files_path=processed_files_path,
            audit_tables=audit_manager,
            job_run_id=1,
            connection=conn,
            rules_path=PLANETS_RULES_PATH,
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )
        audit_manager.add_new_submissions([sub_info], job_run_id=1)

        successful_files, unsuccessful_files, failed_processing = dve_pipeline.business_rule_step(
            pool=ThreadPoolExecutor(2), files=[(sub_info, SubmissionStatus())]
        )

    assert len(successful_files) == 1
    assert not successful_files[0][1].validation_failed
    assert len(unsuccessful_files) == 0
    assert len(failed_processing) == 0

    assert Path(processed_files_path, sub_info.submission_id, "business_rules", "planets").exists()

    audit_result = audit_manager.get_all_error_report_submissions()

    assert len(audit_result[0]) == 1
    assert len(audit_result[1]) == 0


def test_error_report_step(
    planets_data_after_business_rules,
    temp_ddb_conn,
):  # pylint: disable=redefined-outer-name
    db_file, conn = temp_ddb_conn
    submitted_file_info, processed_files_path, status = planets_data_after_business_rules

    DuckDBRefDataLoader.connection = conn
    DuckDBRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)

    with DDBAuditingManager(db_file.as_uri(), ThreadPoolExecutor(1), conn) as audit_manager:
        dve_pipeline = DDBDVEPipeline(
            processed_files_path=processed_files_path,
            audit_tables=audit_manager,
            job_run_id=1,
            connection=conn,
            rules_path=None,
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )

        reports = dve_pipeline.error_report_step(
            pool=ThreadPoolExecutor(2),
            processed=[(submitted_file_info, status)],
            failed_file_transformation=[],
        )

    assert len(reports) == 1

    audit_result = audit_manager.get_current_processing_info(submitted_file_info.submission_id)
    assert audit_result.processing_status == "success"

def test_get_submission_status(temp_ddb_conn):
    db_file, conn = temp_ddb_conn
    with DDBAuditingManager(db_file.as_uri(), connection = conn) as aud:
        dve_pipeline = DDBDVEPipeline(
                processed_files_path="fake_path",
                audit_tables=aud,
                job_run_id=1,
                connection=conn,
                rules_path=None,
                submitted_files_path=None,
                reference_data_loader=DuckDBRefDataLoader,
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
        
        aud.add_new_submissions([sub_one, sub_two])
        aud.add_processing_records(
            [
                ProcessingStatusRecord(
                    submission_id=sub_one.submission_id, processing_status="error_report", submission_result="validation_failed"
                ),
                ProcessingStatusRecord(
                    submission_id=sub_two.submission_id, processing_status="failed", submission_result="processing_failed"
                ),
            ]
        )
        aud.add_submission_statistics_records([
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