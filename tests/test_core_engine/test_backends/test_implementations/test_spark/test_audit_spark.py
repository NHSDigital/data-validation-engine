import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple
from uuid import uuid4

import pytest
from pyspark.sql.functions import col, lit

from dve.core_engine.backends.implementations.spark.auditing import SparkAuditingManager
from dve.core_engine.models import ProcessingStatusRecord, SubmissionInfo
from dve.pipeline.utils import unpersist_all_rdds
from dve.core_engine.backends.implementations.spark.spark_helpers import PYTHON_TYPE_TO_SPARK_TYPE

from .....conftest import get_test_file_path
from .....fixtures import spark, spark_test_database


@pytest.fixture(scope="function")
def spark_audit_manager(spark, spark_test_database) -> Iterator[SparkAuditingManager]:
    yield SparkAuditingManager(database=spark_test_database,
                               table_format="delta",
                               spark=spark
                               )


@pytest.fixture(scope="function")
def spark_audit_manager_threaded(spark, spark_test_database) -> Iterator[SparkAuditingManager]:
    with ThreadPoolExecutor(1) as pool:
        yield SparkAuditingManager(database=spark_test_database,
                               table_format="delta",
                               spark = spark,
                               pool=pool
                               )


@pytest.fixture
def dve_metadata_file() -> Iterator[Path]:
    _json_str = """{
    "file_name":  "TESTFILE_TEST_20230323T084600",
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


@pytest.fixture
def dve_test_suite_metadata_file():
    return get_test_file_path("spark/samples/test_suite.metadata.json")


def test_audit_table_add_record(spark_audit_manager: SparkAuditingManager):
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_DATASET_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=987654321,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    spark_audit_manager.add_new_submissions([_sub_info])

    at_entry = (
        spark_audit_manager._submission_info.get_df()
        .filter(col("submission_id") == lit(_sub_info.submission_id))
        .collect()
    )

    assert len(at_entry) == 1

    assert SubmissionInfo(**at_entry[0].asDict()) == _sub_info


def test_audit_table_update_status(spark_audit_manager: SparkAuditingManager):
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_DATASET_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    spark_audit_manager.add_new_submissions([_sub_info])

    assert (
        spark_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "received"
    )

    spark_audit_manager.mark_transform([_sub_info.submission_id])

    assert (
        spark_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "file_transformation"
    )

    spark_audit_manager.mark_data_contract([_sub_info.submission_id])

    assert (
        spark_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "data_contract"
    )

    spark_audit_manager.mark_business_rules([(_sub_info.submission_id, False)])

    assert (
        spark_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "business_rules"
    )

    spark_audit_manager.mark_error_report([(_sub_info.submission_id, "failed")])

    assert (
        spark_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "error_report"
    )

    spark_audit_manager.mark_finished([(_sub_info.submission_id, "failed")])

    assert (
        spark_audit_manager.get_current_processing_info(_sub_info.submission_id).processing_status
        == "success"
    )


def test_submission_info_from_metadata_file(dve_metadata_file):
    _sub_id = uuid4().hex
    _sub_meta = dict(SubmissionInfo.from_metadata_file(_sub_id, dve_metadata_file))
    expected = dict(
        submission_id=_sub_id,
        dataset_id="TEST_DATASET",
        submitting_org="TEST",
        file_name="TESTFILE_TEST_20230323T084600",
        file_extension="xml",
        reporting_period="FY2023-24_TEST",
        file_size=123456789,
        datetime_received=datetime(2023, 10, 3, 10, 53, 36, 123199, tzinfo=timezone.utc),
    )

    assert all([expected.get(k) == _sub_meta.get(k) for k in expected])


def test_add_transfer_ids(spark_audit_manager: SparkAuditingManager):
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST7",
        dataset_id="TEST_DATASET",
        file_name="TEST_DATASET_FY2021-22_TEST_20220706T1119",
        file_extension="xml",
        file_size=345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )
    spark_audit_manager.add_new_submissions([_sub_info])

    spark_audit_manager.add_feedback_transfer_ids([(_sub_info.submission_id, "123")])

    transfer_info = (
        spark_audit_manager._transfers.get_df()
        .filter(col("submission_id") == lit(_sub_info.submission_id))
        .collect()
    )

    transfer_info = sorted(transfer_info, key=lambda x: x.report_name)

    assert transfer_info[0].report_name == "error_report" and transfer_info[0].transfer_id == "123"


def test_dve_audit_using_thread_pool(spark_audit_manager_threaded: SparkAuditingManager):
    with spark_audit_manager_threaded as aud:
        _sub_info = SubmissionInfo(
            submission_id=uuid4().hex,
            submitting_org="TEST",
            dataset_id="TEST_DATASET",
            file_name="some_file",
            file_extension="xml",
            file_size=987654321,
            datetime_received=datetime(2023, 9, 1, 12, 0, 0),
        )
        _sub_info.submission_id = uuid4().hex
        aud.add_new_submissions([_sub_info])
        while not aud.queue.empty():
            time.sleep(2)
        assert _sub_info.submission_id

        at_entry = (
            aud._processing_status.get_df()
            .filter(col("submission_id") == lit(_sub_info.submission_id))
            .collect()
        )

        assert len(at_entry) == 1
        aud.mark_transform([_sub_info.submission_id])
        while not aud.queue.empty():
            time.sleep(2)

    file_trans = aud.get_all_file_transformation_submissions()
    assert [rw.submission_id for rw in file_trans.collect()] == [_sub_info.submission_id]


@pytest.mark.parametrize(
    "status", ["file_transformation", "data_contract", "business_rules", "error_report"]
)
def test_downstream(spark_audit_manager: SparkAuditingManager, status):
    """testing that downstream pending returns true when the status in the audit table is the same
    as the status we're passing in"""

    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_DATASET_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )
    _sub_info.submission_id = uuid4().hex

    spark_audit_manager.add_new_submissions([_sub_info])

    spark_audit_manager.add_processing_records(
        [ProcessingStatusRecord(submission_id=_sub_info.submission_id, processing_status=status)]
    )

    assert spark_audit_manager.downstream_pending(status)


@pytest.mark.parametrize(
    ["downstream", "status"],
    [
        ("data_contract", "file_transformation"),
        ("business_rules", "data_contract"),
        ("error_report", "business_rules"),
    ],
)
def test_downstream_with_downstream(spark_audit_manager: SparkAuditingManager, downstream, status):
    """testing that downstream_pending returns true when a status of an earlier step is present"""
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )
    _sub_info.submission_id = uuid4().hex

    spark_audit_manager.add_new_submissions([_sub_info])

    spark_audit_manager.add_processing_records(
        [ProcessingStatusRecord(submission_id=_sub_info.submission_id, processing_status=status)]
    )

    assert spark_audit_manager.downstream_pending(downstream)


@pytest.mark.parametrize(
    ["status", "upstream"],
    [
        ("data_contract", "file_transformation"),
        ("business_rules", "data_contract"),
        ("error_report", "business_rules"),
    ],
)
def test_downstream_upstream(spark_audit_manager: SparkAuditingManager, status, upstream):
    """testing that downstream_pending returns false when only a status of an later step is present"""
    _sub_info = SubmissionInfo(
        submission_id=uuid4().hex,
        submitting_org="TEST",
        dataset_id="TEST_DATASET",
        file_name="TEST_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )
    _sub_info.submission_id = uuid4().hex

    spark_audit_manager.add_new_submissions([_sub_info])

    spark_audit_manager.add_processing_records(
        [ProcessingStatusRecord(submission_id=_sub_info.submission_id, processing_status=status)]
    )

    assert not spark_audit_manager.downstream_pending(upstream)


def test_get_error_report_submissions(spark_audit_manager: SparkAuditingManager):
    # add three submissions
    sub_1 = SubmissionInfo(
        submission_id="1",
        submitting_org="TEST1",
        dataset_id="TEST_DATASET",
        file_name="TEST_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )
    sub_2 = SubmissionInfo(
        submission_id="2",
        submitting_org="TEST2",
        dataset_id="TEST_DATASET2",
        file_name="TEST_DATASET2_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )
    sub_3 = SubmissionInfo(
        submission_id="3",
        submitting_org="TEST3",
        dataset_id="TEST_DATASET3",
        file_name="TEST_DATASET3_FY2021-22_TEST_20220706T1119",
        submission_method="sftp",
        file_extension="xml",
        file_size=12345,
        datetime_received=datetime(2023, 9, 1, 12, 0, 0),
    )

    spark_audit_manager.add_new_submissions([sub_1, sub_2, sub_3])
    # mark 1 and 3 as error report ready and 2 for data contract
    spark_audit_manager.add_processing_records(
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
    processed, dodgy = spark_audit_manager.get_all_error_report_submissions()

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
