"""
Generic types specifically for use across backend implementations.

"""

from typing import TypeAlias

from dve.core_engine.backends.base.core import EntityManager
from dve.core_engine.backends.types import EntityType

Entities: TypeAlias = EntityManager[EntityType]
