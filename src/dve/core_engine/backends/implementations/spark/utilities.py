"""Some utilities which are useful for implementing Spark transformations."""

import datetime as dt
from collections.abc import Callable
from json import JSONEncoder
from operator import and_, or_
from typing import Any

from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.column import Column
from pyspark.sql.functions import coalesce, lit

from dve.core_engine.backends.base.utilities import _split_multiexpr_string
from dve.core_engine.type_hints import (
    ExpressionArray,
    ExpressionMapping,
    MultiExpression,
    MultipleExpressions,
)


def _apply_operation_to_column_sequence(
    *columns: Column, operation: Callable[[Column, Column], Column]
) -> Column:
    """Apply a boolean operation to a sequence of columns. Null columns are
    treated as false.

    """
    if not columns:
        raise ValueError("Boolean operation requires a sequence of columns.")
    if not all(map(lambda column: isinstance(column, Column), columns)):
        raise TypeError("Unexpected non-column type in `columns`.")

    iterator = iter(columns)
    result = coalesce(next(iterator), lit(False))
    for column in iterator:
        result = operation(result, coalesce(column, lit(False)))

    return result


def all_columns(*columns: Column) -> Column:
    """Equivalent to Python's 'all', but for boolean columns."""
    return _apply_operation_to_column_sequence(*columns, operation=and_)


def any_columns(*columns: Column) -> Column:
    """Equivalent to Python's 'any', but for boolean columns."""
    return _apply_operation_to_column_sequence(*columns, operation=or_)


def expr_mapping_to_columns(expressions: ExpressionMapping) -> list[Column]:
    """Convert a mapping of expression to alias to a list of columns. Where the
    expression requires a tuple of column names, the alias should be a list of
    column names.

    """
    columns = []
    for expression, alias in expressions.items():
        column = sf.expr(expression)
        if isinstance(alias, list):
            column = column.alias(*alias)
        else:
            column = column.alias(alias)
        columns.append(column)
    return columns


def expr_array_to_columns(expressions: ExpressionArray) -> list[Column]:
    """Convert an array of expressions to a list of columns."""
    return list(map(sf.expr, expressions))


def multiexpr_string_to_columns(expressions: MultiExpression) -> list[Column]:
    """Convert multiple SQL expressions in a comma-delimited string to a list
    of columns.

    """
    expression_list = _split_multiexpr_string(expressions)
    return expr_array_to_columns(expression_list)


def parse_multiple_expressions(expressions: MultipleExpressions) -> list[Column]:
    """Parse multiple expressions provided as a mapping or alias to expression,
    an array of expressions, or a string containing multiple comma-delimited
    SQL expressions.

    """
    if isinstance(expressions, dict):
        return expr_mapping_to_columns(expressions)
    if isinstance(expressions, list):
        return expr_array_to_columns(expressions)
    if isinstance(expressions, str):
        return multiexpr_string_to_columns(expressions)
    raise TypeError(type(expressions))


class PydanticCompatibleJSONEncoder(JSONEncoder):
    """A pydantic-compatible JSON encoder."""

    def default(self, o: Any) -> Any:
        """Sets the format for given types for json encoding"""
        if isinstance(o, BaseModel):
            return o.dict()
        if isinstance(o, dt.date):
            return o.isoformat()
        return super().default(o)


def database_exists(spark: SparkSession, database: str) -> bool:
    """
        check if the database exists
    Args:
        spark (SparkSession):
        database (str): database name

    Returns:
        bool:
    """

    return spark.catalog._jcatalog.databaseExists(database)  # type: ignore  #pylint: disable=protected-access


def table_exists(spark: SparkSession, table: str) -> bool:
    """
        check if the spark table exists
    Args:
        spark (SparkSession):
        table (str): this should be fully qualified for proper tables

    Returns:
        bool:
    """

    return spark.catalog._jcatalog.tableExists(table)  # type: ignore  #pylint: disable=protected-access
