"""Types used in Spark implementations."""

# pylint: disable=C0103
from typing import MutableMapping
from typing_extensions import Literal

from duckdb import DuckDBPyRelation

from dve.core_engine.type_hints import EntityName

SQLType = Literal[
    "BIGINT",
    "BIT",
    "BLOB",
    "BOOLEAN",
    "DATE",
    "DECIMAL",
    "DOUBLE",
    "HUGEINT",
    "INTEGER",
    "INTERVAL",
    "REAL",
    "SMALLINT",
    "TIME",
    "UBIGINT",
    "UHUGEINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    "UUID",
    "VARCHAR",
]
"""SQL types recognised in duckdb"""

Source = DuckDBPyRelation
"""The source entity for a join. This will be aliased to the source entity name."""
Target = DuckDBPyRelation
"""The target entity for a join. This will be aliased to the target entity name."""
Joined = DuckDBPyRelation
"""
The joined entity.

This will be able to reference source and target columns by their aliased names.

"""
DuckDBEntities = MutableMapping[EntityName, DuckDBPyRelation]
"""The type of a mapping of entity name to Spark entity."""
