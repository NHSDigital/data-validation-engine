import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pytest
from duckdb import ColumnExpression, ConstantExpression, DuckDBPyConnection

from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager
from dve.core_engine.models import ProcessingStatusRecord, SubmissionInfo, SubmissionStatisticsRecord
from dve.pipeline.utils import SubmissionStatus

from .....fixtures import temp_ddb_conn  # pylint: disable=unused-import


@pytest.fixture(scope="function")
def ddb_audit_manager(temp_ddb_conn) -> Iterator[DDBAuditingManager]:
    db_file: Path
    conn: DuckDBPyConnection
    db_file, conn = temp_ddb_conn
    yield DDBAuditingManager(database_uri=db_file.as_uri(), connection=conn)


@pytest.fixture(scope="function")
def ddb_audit_manager_threaded(temp_ddb_conn) -> Iterator[DDBAuditingManager]:
    db_file: Path
    conn: DuckDBPyConnection
    db_file, conn = temp_ddb_conn
    with ThreadPoolExecutor(1) as pool:
        yield DDBAuditingManager(database_uri=db_file.as_uri(), pool=pool, connection=conn)


@pytest.fixture
def dve_metadata_file() -> Iterator[Path]:
    _json_str = """{
    "file_name":  "TESTFILEFY2023-24_TEST",
    "file_extension":  "xml",
    "file_size": 123456789,
    "submitting_org":  "TEST",
    "reporting_period": "FY2023-24_TEST",
    "dataset_id":  "TEST_DATASET",
    "datetime_received":  "2023-10-03T10:53:36.1231998Z"
    }"""
    with tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8") as mta:
        mta.write(_json_str)
        mta.seek(0)
        yield Path(mta.name)


def test_audit_table_add_record(ddb_audit_manager: DDBAuditingManager):
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_FILE",
        submission_method="api",
        file_extension="xml",
        file_size=987654321,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    ddb_audit_manager.add_new_submissions([_sub_info])

    at_entry = list(
        ddb_audit_manager._submission_info.get_relation()
        .filter(ColumnExpression("submission_id") == ConstantExpression(_sub_info.submission_id))
        .pl()
        .iter_rows(named=True)
    )

    assert len(at_entry) == 1

    assert SubmissionInfo(**at_entry[0]) == _sub_info


def test_audit_table_update_status(ddb_audit_manager: DDBAuditingManager):
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_FILE",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    ddb_audit_manager.add_new_submissions([_sub_info])

    assert (
        ddb_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "received"
    )

    ddb_audit_manager.mark_transform([_sub_info.submission_id])

    assert (
        ddb_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "file_transformation"
    )

    ddb_audit_manager.mark_data_contract([_sub_info.submission_id])

    assert (
        ddb_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "data_contract"
    )

    ddb_audit_manager.mark_business_rules([(_sub_info.submission_id, False)])

    assert (
        ddb_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "business_rules"
    )

    ddb_audit_manager.mark_error_report([(_sub_info.submission_id, "validation_failed")])

    assert (
        ddb_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "error_report"
    )

    ddb_audit_manager.mark_finished([(_sub_info.submission_id, "validation_failed")])

    assert (
        ddb_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "success"
    )


def test_add_transfer_ids(ddb_audit_manager: DDBAuditingManager):
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST7",
        dataset_id="TEST_DATASET",
        file_name="TEST_FILE",
        file_extension="xml",
        file_size=345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    ddb_audit_manager.add_new_submissions([_sub_info])

    ddb_audit_manager.add_feedback_transfer_ids([(_sub_info.submission_id, "123")])

    transfer_info = (
        ddb_audit_manager._transfers.get_relation()
        .filter(ColumnExpression("submission_id") == ConstantExpression(_sub_info.submission_id))
        .pl()
        .iter_rows(named=True)
    )

    transfer_info = sorted(transfer_info, key=lambda x: x.get("report_name"))

    assert (
        transfer_info[0].get("report_name") == "error_report"
        and transfer_info[0].get("transfer_id") == "123"
    )


def test_dve_audit_using_thread_pool(ddb_audit_manager_threaded: DDBAuditingManager):
    with ddb_audit_manager_threaded as aud:
        _sub_info = SubmissionInfo(
            submission_id=uuid4().hex,
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            file_extension="xml",
            file_size=987654321,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )

        aud.add_new_submissions([_sub_info])
        while not aud.queue.empty():
            time.sleep(0.2)

        at_entry = list(
            aud._processing_status.get_relation()
            .filter(
                ColumnExpression("submission_id") == ConstantExpression(_sub_info.submission_id)
            )
            .pl()
            .iter_rows(named=True)
        )

        assert len(at_entry) == 1
        aud.mark_transform([_sub_info.submission_id])
        while not aud.queue.empty():
            time.sleep(0.2)

    file_trans = aud.get_all_file_transformation_submissions()
    assert [rw.get("submission_id") for rw in file_trans.pl().iter_rows(named=True)] == [
        _sub_info.submission_id
    ]


@pytest.mark.parametrize(
    "status", ["file_transformation", "data_contract", "business_rules", "error_report"]
)
def test_downstream(ddb_audit_manager: DDBAuditingManager, status):
    """testing that downstream pending returns true when the status in the audit table is the same
    as the status we're passing in"""

    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_FILE",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    ddb_audit_manager.add_new_submissions([_sub_info])

    ddb_audit_manager.add_processing_records(
        [ProcessingStatusRecord(submission_id=_sub_info.submission_id, processing_status=status)]
    )

    assert ddb_audit_manager.downstream_pending(status)


@pytest.mark.parametrize(
    ["downstream", "status"],
    [
        ("data_contract", "file_transformation"),
        ("business_rules", "data_contract"),
        ("error_report", "business_rules"),
    ],
)
def test_downstream_with_downstream(ddb_audit_manager: DDBAuditingManager, downstream, status):
    """testing that downstream_pending returns true when a status of an earlier step is present"""
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_FILE",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    ddb_audit_manager.add_new_submissions([_sub_info])

    ddb_audit_manager.add_processing_records(
        [ProcessingStatusRecord(submission_id=_sub_info.submission_id, processing_status=status)]
    )

    assert ddb_audit_manager.downstream_pending(downstream)


@pytest.mark.parametrize(
    ["status", "upstream"],
    [
        ("data_contract", "file_transformation"),
        ("business_rules", "data_contract"),
        ("error_report", "business_rules"),
    ],
)
def test_downstream_upstream(ddb_audit_manager: DDBAuditingManager, status, upstream):
    """testing that downstream_pending returns false when only a status of an later step is present"""
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_FILE",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    ddb_audit_manager.add_new_submissions([_sub_info])

    ddb_audit_manager.add_processing_records(
        [ProcessingStatusRecord(submission_id=_sub_info.submission_id, processing_status=status)]
    )

    assert not ddb_audit_manager.downstream_pending(upstream)


def test_get_error_report_submissions(ddb_audit_manager_threaded: DDBAuditingManager):
    with ddb_audit_manager_threaded as aud:
        # add three submissions
        sub_1 = SubmissionInfo(
            submission_id="1",
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            submission_method="sftp",
            file_extension="xml",
            file_size=12345,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )
        sub_2 = SubmissionInfo(
            submission_id="2",
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            submission_method="sftp",
            file_extension="xml",
            file_size=12345,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )
        sub_3 = SubmissionInfo(
            submission_id="3",
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            submission_method="sftp",
            file_extension="xml",
            file_size=12345,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )

        aud.add_new_submissions([sub_1, sub_2, sub_3])
        # mark 1 and 3 as error report ready and 2 for data contract
        aud.add_processing_records(
            [
                ProcessingStatusRecord(
                    submission_id=sub_1.submission_id, processing_status="error_report"
                ),
                ProcessingStatusRecord(
                    submission_id=sub_3.submission_id, processing_status="error_report"
                ),
                ProcessingStatusRecord(
                    submission_id=sub_2.submission_id, processing_status="data_contract"
                ),
            ]
        )

    processed, dodgy = aud.get_all_error_report_submissions()

    processed = sorted(processed, key=lambda x: int(x.submission_id))

    expected = [
        SubmissionInfo(
            submission_id=sub_1.submission_id,
            **{fld: val for fld, val in sub_1.dict().items() if fld != "submission_id"},
        ),
        SubmissionInfo(
            submission_id=sub_3.submission_id,
            **{fld: val for fld, val in sub_3.dict().items() if fld != "submission_id"},
        ),
    ]
    assert len(processed) == 2
    assert len(dodgy) == 0
    assert processed == expected

def test_get_submission_status(ddb_audit_manager_threaded: DDBAuditingManager):
    with ddb_audit_manager_threaded as aud:
        # add four submissions
        sub_1 = SubmissionInfo(
            submission_id="1",
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            submission_method="sftp",
            file_extension="xml",
            file_size=12345,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )
        sub_2 = SubmissionInfo(
            submission_id="2",
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="TEST_FILE",
            submission_method="sftp",
            file_extension="xml",
            file_size=12345,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )
        
        sub_3 = SubmissionInfo(
                submission_id="3",
                submitting_org="TEST",
                dataset_id="TEST_DATASET",
                file_name="TEST_FILE",
                submission_method="sftp",
                file_extension="xml",
                file_size=12345,
                datetime_received=datetime(2023, 9, 1, 12, 0, 0),
            )

        sub_4 = SubmissionInfo(
                submission_id="4",
                submitting_org="TEST",
                dataset_id="TEST_DATASET",
                file_name="TEST_FILE",
                submission_method="sftp",
                file_extension="xml",
                file_size=12345,
                datetime_received=datetime(2023, 9, 1, 12, 0, 0),
            )
        
        # mark 1 as failed validation, 2 as failed processing, 3 as null and 4 as successful
        aud.add_new_submissions([sub_1, sub_2, sub_3, sub_4])
        aud.add_processing_records(
            [
                ProcessingStatusRecord(
                    submission_id=sub_1.submission_id, processing_status="error_report", submission_result="validation_failed"
                ),
                ProcessingStatusRecord(
                    submission_id=sub_2.submission_id, processing_status="failed", submission_result="processing_failed"
                ),
                ProcessingStatusRecord(
                    submission_id=sub_3.submission_id, processing_status="business_rules"
                ),
                ProcessingStatusRecord(
                    submission_id=sub_4.submission_id, processing_status="error_report", submission_result="success"
                ),
            ]
        )
        aud.add_submission_statistics_records([
            SubmissionStatisticsRecord(submission_id=sub_1.submission_id, record_count=5, number_record_rejections=2, number_warnings=3),
            SubmissionStatisticsRecord(submission_id=sub_4.submission_id, record_count=20, number_record_rejections=0, number_warnings=1)
        ])
        
        while not aud.queue.empty():
            time.sleep(0.05)
        
        sub_stats_1 = aud.get_submission_status(sub_1.submission_id)
        assert sub_stats_1.submission_result == "validation_failed"
        assert sub_stats_1.validation_failed
        assert not sub_stats_1.processing_failed
        assert sub_stats_1.number_of_records == 5
        sub_stats_2 = aud.get_submission_status(sub_2.submission_id)
        assert sub_stats_2.submission_result == "processing_failed"
        assert not sub_stats_2.validation_failed
        assert sub_stats_2.processing_failed
        sub_stats_3 = aud.get_submission_status(sub_3.submission_id)
        assert not sub_stats_3.validation_failed
        assert not sub_stats_3.processing_failed
        sub_stats_4 = aud.get_submission_status(sub_4.submission_id)
        assert sub_stats_4.submission_result == "success"
        assert not sub_stats_4.validation_failed
        assert not sub_stats_4.processing_failed
        assert sub_stats_4.number_of_records == 20
        assert not aud.get_submission_status("5")