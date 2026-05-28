"""General utilities for file readers"""

from typing import Optional

from pydantic import BaseModel

from dve.core_engine.backends.exceptions import MessageBearingError
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling.service import open_stream


def check_csv_header_expected(
    resource: URI,
    expected_schema: type[BaseModel],
    delimiter: Optional[str] = ",",
    quote_char: str = '"',
) -> tuple[set[str], set[str]]:
    """Check the header of a CSV matches the expected fields"""
    with open_stream(resource) as fle:
        header_fields = fle.readline().rstrip().replace(quote_char, "").split(delimiter)
    expected_fields = expected_schema.__fields__.keys()

    missing = set(expected_fields).difference(header_fields)
    additional = set(header_fields).difference(expected_fields)

    return missing, additional


def raise_message_bearing_error_on_header_differences(
    resource: URI,
    entity_name: EntityName,
    expected_schema: type[BaseModel],
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
        resource, expected_schema,
        delimiter,
        quote_char
    )

    if missing or additional:
        raise MessageBearingError(
            "The CSV header doesn't match what is expected",
            messages=[
                FeedbackMessage(
                    entity=entity_name,
                    record={"missing_fields": missing, "additional_fields": additional},
                    failure_type="submission",
                    error_location="Whole File",
                    reporting_field="csv_header",
                    error_code=field_check_error_code,
                    error_message=field_check_error_message,
                )
            ],
        )
