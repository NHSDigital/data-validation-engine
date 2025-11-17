"""Utility objects for use with duckdb backend"""

import itertools
from dve.core_engine.backends.base.utilities import _split_multiexpr_string


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
            _split_multiexpr_string(expression) 
            for expression in expressions
            )
        )


def multiexpr_string_to_columns(expressions: str) -> list[str]:
    """Split string containing multiple expressions to list of duck db
    column expressions
    """
    expression_list = _split_multiexpr_string(expressions)
    return expr_array_to_columns(expression_list)
