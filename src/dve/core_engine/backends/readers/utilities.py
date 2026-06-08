"""General utilities for file readers"""

from collections.abc import Iterable
from typing import Optional

from pydantic import BaseModel

from dve.core_engine.backends.exceptions import MessageBearingError
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling.service import open_stream


def check_csv_header_expected(
    resource: URI,
    expected_schema: type[BaseModel],
    all_model_fields: set[str],
    delimiter: Optional[str] = ",",
    quote_char: str = '"',
) -> tuple[set[str], set[str]]:
    """Check the header of a CSV matches the expected fields"""
    with open_stream(resource) as fle:
        header_fields = fle.readline().rstrip().replace(quote_char, "").split(delimiter)
    expected_fields = expected_schema.__fields__.keys()

    missing = set(expected_fields).difference(header_fields)
    additional = set(header_fields).difference(all_model_fields)

    return missing, additional


def raise_message_bearing_error_on_header_differences(
    resource: URI,
    entity_name: EntityName,
    expected_schema: type[BaseModel],
    all_model_fields: set[str],
    field_check_error_code: str,
    field_check_error_message: str,
    delimiter: Optional[str] = ",",
    quote_char: str = '"',
) -> None:
    """
    Raise a FeedbackMessage if differences found between the actual header and the expected
    header or vice versa.
    """
    missing, additional = check_csv_header_expected(
        resource,
        expected_schema,
        all_model_fields,
        delimiter,
        quote_char
    )

    if missing or additional:
        record_details_missing = f"missing fields: {', '.join(missing)};" if missing else ""
        record_details_additional = f"additional fields: {', '.join(additional)};" if additional else ""  # pylint: disable=C0301
        raise MessageBearingError(
            "The CSV header doesn't match what is expected",
            messages=[
                FeedbackMessage(
                    entity="Pre-validation",
                    record={entity_name: f"{record_details_missing}{record_details_additional}"},
                    failure_type="submission",
                    error_location=entity_name,
                    reporting_field="csv_header",
                    error_code=field_check_error_code,
                    error_message=field_check_error_message,
                )
            ],
        )


def get_all_model_fields(models: Iterable[type[BaseModel]]) -> set[str]:
    """Return all field names from all available models"""
    return {field for model in models for field in model.__fields__.keys()}
