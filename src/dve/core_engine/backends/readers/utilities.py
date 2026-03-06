"""General utilities for file readers"""

from typing import Optional

from pydantic import BaseModel

from dve.core_engine.type_hints import URI
from dve.parser.file_handling.service import open_stream


def check_csv_header_expected(
    resource: URI,
    expected_schema: type[BaseModel],
    delimiter: Optional[str] = ",",
    quote_char: str = '"',
) -> set[str]:
    """Check the header of a CSV matches the expected fields"""
    with open_stream(resource) as fle:
        header_fields = fle.readline().rstrip().replace(quote_char, "").split(delimiter)
    expected_fields = expected_schema.__fields__.keys()
    return set(expected_fields).difference(header_fields)
