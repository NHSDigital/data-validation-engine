"""Utility objects for use with duckdb backend"""

import itertools
from typing import Optional

from pydantic import BaseModel

from dve.core_engine.backends.base.utilities import _split_multiexpr_string
from dve.core_engine.backends.exceptions import MessageBearingError
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import URI
from dve.parser.file_handling import open_stream


def parse_multiple_expressions(expressions) -> list[str]:
    """Break multiple expressions into a list of expressions"""
    if isinstance(expressions, dict):
        return expr_mapping_to_columns(expressions)
    if isinstance(expressions, list):
        return expr_array_to_columns(expressions)
    if isinstance(expressions, str):
        return multiexpr_string_to_columns(expressions)
    return []


def expr_mapping_to_columns(expressions: dict) -> list[str]:
    """Map duckdb expressions to column names"""
    columns = []
    for expression, alias in expressions.items():
        columns.append(f"{expression} as {alias}")
    return columns


def expr_array_to_columns(expressions: list[str]) -> list[str]:
    """Create list of duckdb expressions from list of expressions"""
    return list(
        itertools.chain.from_iterable(
            _split_multiexpr_string(expression) for expression in expressions
        )
    )


def multiexpr_string_to_columns(expressions: str) -> list[str]:
    """Split string containing multiple expressions to list of duck db
    column expressions
    """
    expression_list = _split_multiexpr_string(expressions)
    return expr_array_to_columns(expression_list)

def check_csv_header_expected(
    resource: URI,
    expected_schema: type[BaseModel],
    delimiter: Optional[str] = ",",
    quote_char: str = '"') -> set[str]:
    """Check the header of a CSV matches the expected fields"""
    with open_stream(resource) as fle:
        header_fields = fle.readline().rstrip().replace(quote_char,"").split(delimiter)
    expected_fields = expected_schema.__fields__.keys()
    return set(expected_fields).difference(header_fields)
    
