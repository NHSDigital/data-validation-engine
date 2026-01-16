"""Unit tests for core engine models."""
from typing import Any, Dict, Tuple
from uuid import uuid4

import pytest

from dve.core_engine.models import SubmissionInfo

CONSTANT_SUBMISSION_ID = uuid4().hex


@pytest.mark.parametrize(
    "testcases",
    [
        # perfect submission info
        (
            {
                "submitted": {
                    "submission_id": CONSTANT_SUBMISSION_ID,
                    "dataset_id": "test0",
                    "file_name": "my_file.csv",
                    "file_extension": "csv",
                },
                "expected": {
                    "submission_id": CONSTANT_SUBMISSION_ID,
                    "dataset_id": "test0",
                    "file_name": "my_file",
                    "file_extension": "csv",
                },
            },
        ),
        # submission with multiple file extensions within the file name
        (
            {
                "submitted": {
                    "submission_id": CONSTANT_SUBMISSION_ID,
                    "dataset_id": "test1",
                    "file_name": "my_file.csv.csv",
                    "file_extension": "csv",
                },
                "expected": {
                    "submission_id": CONSTANT_SUBMISSION_ID,
                    "dataset_id": "test1",
                    "file_name": "my_file.csv",
                    "file_extension": "csv",
                },
            },
        ),
        # submission with multiple file extensions
        (
            {
                "submitted": {
                    "submission_id": CONSTANT_SUBMISSION_ID,
                    "dataset_id": "test2",
                    "file_name": "my_file.xml",
                    "file_extension": "csv.csv.xml",
                },
                "expected": {
                    "submission_id": CONSTANT_SUBMISSION_ID,
                    "dataset_id": "test2",
                    "file_name": "my_file",
                    "file_extension": "xml",
                },
            },
        ),
    ],
)
def test_submission_info(  # pylint: disable=missing-function-docstring
    testcases: Tuple[Dict[str, Dict[str, Any]]],
):
    ignore = [
        "date_updated",
        "time_updated",
        "submission_method",
        "submitting_org",
        "reporting_period_start",
        "reporting_period_end",
        "file_size",
        "datetime_received",
    ]
    for testcase in testcases:
        actual = SubmissionInfo(**testcase["submitted"])
        expected = testcase["expected"]

        assert {k: v for k, v in actual.dict().items() if k not in ignore} == expected
        assert actual.file_name_with_ext == f"{expected['file_name']}.{expected['file_extension']}"


def test_submission_info_eq():  # pylint: disable=missing-function-docstring
    data = {
        "submission_id": uuid4().hex,
        "dataset_id": "test0",
        "file_name": "my_file.csv",
        "file_extension": "csv",
    }
    assert SubmissionInfo(**data) == SubmissionInfo(**data)  # type: ignore
