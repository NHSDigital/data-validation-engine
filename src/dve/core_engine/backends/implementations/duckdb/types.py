"""Types used in Spark implementations."""

# pylint: disable=C0103
from collections.abc import MutableMapping

from duckdb import DuckDBPyRelation

from dve.core_engine.type_hints import EntityName

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
