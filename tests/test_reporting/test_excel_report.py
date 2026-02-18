import datetime
from collections import deque

import polars as pl
import pytest

from dve.core_engine.message import FeedbackMessage
from dve.pipeline.utils import SubmissionStatus
from dve.reporting.error_report import (
    create_error_dataframe,
    generate_report_dataframes,
    get_error_codes,
    populate_error_codes,
)
from dve.reporting.excel_report import ExcelFormat, SummaryItems

from ..conftest import get_test_file_path
from ..fixtures import temp_dir


@pytest.fixture(scope="function")
def planet_error_codes():
    codes = get_test_file_path("planets/error_codes.json")
    return get_error_codes(codes.as_posix())


@pytest.fixture(scope="function")
def planet_errors():
    return deque(
        [
            FeedbackMessage(
                entity="Planet",
                record={
                    "id": 1,
                    "name": None,
                    "mass": 0.33,
                    "diameter": 4879.0,
                    "density": 5427.0,
                    "gravity": 3.7,
                    "escapeVelocity": 4.3,
                    "rotationPeriod": 1407.6,
                    "lengthOfDay": 4222.6,
                    "distanceFromSun": 57.9,
                    "perihelion": 46.0,
                    "aphelion": 69.8,
                    "orbitalPeriod": 88.0,
                    "orbitalVelocity": 47.4,
                    "orbitalInclination": 7.0,
                    "orbitalEccentricity": 0.205,
                    "obliquityToOrbit": 0.034,
                    "meanTemperature": 167.0,
                    "surfacePressure": 0.0,
                    "numberOfMoons": 0,
                    "hasRingSystem": False,
                    "hasGlobalMagneticField": False,
                },
                failure_type="record",
                is_informational=True,
                error_type="Blank Value",
                error_location="name",
                error_message="is blank",
                error_code="001",
                reporting_field="name",
                value=None,
                category="Blank",
            )
        ]
    )


@pytest.fixture(scope="function")
def planet_error_df(planet_errors):
    return create_error_dataframe(planet_errors, {"Planet": "Planet"})


@pytest.fixture(scope="function")
def report_dfs(planet_errors, planet_error_codes):
    return generate_report_dataframes(planet_errors, planet_error_codes, {})


@pytest.fixture(scope="function")
def big_report_dfs(planet_errors, planet_error_codes):
    errors = list(planet_errors) * 31
    return generate_report_dataframes(errors, planet_error_codes, {})


def test_populate_error_codes(temp_dir, planet_error_codes, planet_error_df):
    error_frame = populate_error_codes(planet_error_df, planet_error_codes)

    assert list(error_frame["Error_Code"]) == ["001"]


def test_generate_report_dfs(planet_errors, planet_error_codes):
    error_df, aggregates_df = generate_report_dataframes(planet_errors, planet_error_codes, {})

    assert error_df["Category"].to_list() == ["Blank"]
    assert aggregates_df["Count"].to_list() == [1]


def test_excel_report(report_dfs):
    report = ExcelFormat(*report_dfs)
    summary_items = SummaryItems(
        summary_dict={
            "Sender": "X26",
            "Datetime_sent": datetime.datetime.now(),
            "Datetime_processed": datetime.datetime.now(),
        },
        row_headings=["Submission Failure", "Warning"],
        table_columns=["Planet", "Derived"],
    )
    workbook = report.excel_format(summary_items=summary_items)

    assert workbook.sheetnames == ["Summary", "Error Summary", "Error Data"]
    assert workbook["Summary"]["B2"].value == "Data Summary"

    aggs = workbook["Error Summary"]
    column_headings = [cell.value for cell in aggs["1"]]
    assert column_headings == [
        "Type",
        "Group",
        "Data Item Submission Name",
        "Category",
        "Error Code",
        "Count",
    ]

    details = workbook["Error Data"]
    column_headings = [cell.value for cell in details["1"]]
    assert column_headings == [
        "Group",
        "Type",
        "Error Code",
        "Data Item Submission Name",
        "Errors and Warnings",
        "Value",
        "ID",
        "Category",
    ]


def test_excel_report_overflow(big_report_dfs):
    error_df, aggregate_df = big_report_dfs
    error_dfs = {"MilkyWay": error_df}
    report = ExcelFormat(error_dfs, aggregate_df, overflow=20)
    summary_items = SummaryItems(
        summary_dict={
            "Sender": "X26",
            "Datetime_sent": datetime.datetime.now(),
            "Datetime_processed": datetime.datetime.now(),
        },
        row_headings=["Submission Failure", "Warning"],
        table_columns=["Planet", "Derived"],
    )
    workbook = report.excel_format(
        summary_items=summary_items,
    )
    assert workbook.sheetnames == [
        "Summary",
        "Error Summary",
        "MilkyWay",
        "MilkyWay_2",
    ]


def test_excel_report_empty_dfs():
    """Test that error reports with empty dataframes still produce the correct sheets
    but without anything other than the headers in those sheets"""
    report = ExcelFormat(pl.DataFrame(), pl.DataFrame(), overflow=20)
    summary_items = SummaryItems(
        summary_dict={
            "Sender": "X26",
            "Datetime_sent": datetime.datetime.now(),
            "Datetime_processed": datetime.datetime.now(),
        },
        row_headings=["Submission Failure", "Warning"],
        table_columns=["Planet", "Derived"],
    )
    workbook = report.excel_format(
        summary_items=summary_items,
    )
    assert workbook.sheetnames == ["Summary", "Error Summary", "Error Data"]
    assert not all(cell.value for cell in workbook["Error Data"]["2"])  # no errors
    assert not all(cell.value for cell in workbook["Error Summary"]["2"])  # no aggregates

def test_sub_status_failed_processing():
    """Check that the submission status is used to determine the """
    
    summary_items = SummaryItems(
        submission_status=SubmissionStatus(processing_failed=True),
        summary_dict={
            "Sender": "X26",
            "Datetime_sent": datetime.datetime.now(),
            "Datetime_processed": datetime.datetime.now(),
        },
        row_headings=["Submission Failure", "Warning"],
        table_columns=["Planet", "Derived"],
    )
    assert summary_items.get_submission_status(pl.DataFrame()) == "There was an issue processing the submission. This will be investigated."
    summary_items.submission_status = SubmissionStatus(validation_failed=True)
    assert summary_items.get_submission_status(pl.DataFrame()) == "File has been rejected"
    
