"""Types used in Spark implementations."""

from typing import MutableMapping

from pyspark.sql import DataFrame

from dve.core_engine.type_hints import EntityName

Source = DataFrame
"""The source entity for a join. This will be aliased to the source entity name."""
Target = DataFrame
"""The target entity for a join. This will be aliased to the target entity name."""
Joined = DataFrame
"""
The joined entity.

This will be able to reference source and target columns by their aliased names.

"""
SparkEntities = MutableMapping[EntityName, DataFrame]
"""The type of a mapping of entity name to Spark entity."""
