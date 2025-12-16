# pylint: disable=line-too-long,protected-access,wrong-import-order
# isort: skip_file
"""
Steps which involve analysis after the data contract and transformations
are applied.

"""

# pylint: disable=no-name-in-module
from pathlib import Path
from typing import Any, Dict
from behave import then, when  # type: ignore
from behave.model import Row, Table
from behave.runner import Context
import polars as pl  # type: ignore

from context_tools import (
    get_pipeline,
    get_processing_location,
    get_submission_info,
)
from dve.core_engine.models import SubmissionStatisticsRecord
from dve.parser.file_handling.service import (
    get_resource_exists,
    joinuri,
)
from utilities import display_report, read_output_parquet


# TODO: read entity name direct from parquet output in business_rules area of working folder
# use lightweight python to read - pandas/polars?
# how to search for string value equal - contains not available
def get_n_rows_containing_value(
    context: Context, entity_name: str, value: str, column_name: str, stage: str = "business_rules"
) -> int:
    """
    Get the number of rows in an entity (given by `entity_name`) which
    contain `value` in column `column_name`.

    """
    entity = read_output_parquet(get_processing_location(context), entity_name, stage)
    if column_name.upper() not in list(map(str.upper, entity.columns)):
        raise ValueError(f"Entity {entity_name!r} does not contain column {column_name!r}")

    n_rows_containing_value = entity[entity[column_name].astype(str)==value].shape[0]
    return n_rows_containing_value


@then('The rules restrict "{entity_name}" to {n_records:d} qualifying records')
@then('The rules restrict "{entity_name}" to 1 qualifying record')
def n_rows_remaining_in_entity(context: Context, entity_name: str, n_records: int = 1):
    """Ensure an expected number of records are present for a given dataframe."""

    entity = read_output_parquet(get_processing_location(context), entity_name, "business_rules")
    actual_count = entity.shape[0]
    assert actual_count == n_records, f"\n\nDF for {entity_name} has {actual_count} records\n\n"


@then("An error report is produced")
def check_error_report_produced(context: Context):
    """Check that the error report is produced."""
    processing_loc = get_processing_location(context)
    submission_info = get_submission_info(context)

    error_report_uri = Path(
        processing_loc,
        "error_reports",
        f"{submission_info.file_name}_{submission_info.file_extension}.xlsx",
    ).as_uri()

    assert get_resource_exists(error_report_uri)

    # Show error report for information purposes
    display_report(error_report_uri)


@then(
    'The entity "{entity_name}" contains an entry for "{conforming_value}" in column "{column_name}"'
)
def check_conforming_value_is_present(
    context, entity_name: str, conforming_value: str, column_name: str
):
    """Ensure that a value is present in a given entity."""
    n_rows = get_n_rows_containing_value(context, entity_name, conforming_value, column_name)
    assert n_rows > 0


@then(
    'The entity "{entity_name}" does not contain an entry for "{non_conforming_value}" in column "{column_name}"'
)
def check_non_conforming_value_not_present(
    context, entity_name: str, non_conforming_value: str, column_name: str
):
    """Ensure that a value is not present in a given entity."""
    n_rows = get_n_rows_containing_value(context, entity_name, non_conforming_value, column_name)
    assert n_rows == 0


@then("The statistics entry for the submission shows the following information")
def check_stats_record(context):
    sub_info = get_submission_info(context)
    table: Table = context.table
    if table is None:
        raise ValueError("No table supplied in step")
    row: Row
    expected: Dict[str, Any] = {"submission_id": sub_info.submission_id}
    for row in table:
        record: Dict[str, str] = row.as_dict()
        expected[record["parameter"]] = int(record["value"])
    stats = (
        get_pipeline(context)._audit_tables.get_submission_statistics(sub_info.submission_id).dict()
    )
    assert all([val == stats.get(fld) for fld, val in expected.items()])

@then("the error aggregates are persisted")
def check_error_aggregates_persisted(context):
    processing_location = get_processing_location(context)
    agg_file = Path(processing_location, "audit", "error_aggregates.parquet")
    assert agg_file.exists() and agg_file.is_file()
