"""Generic types specifically for backend implementations.

These are defined here because in general this information should not
'leak' outside of the backend.

"""

"""
Generic types specifically for use across backend implementations.

"""

from typing import TypeVar

EntityType = TypeVar("EntityType")  # pylint: disable=invalid-name
"""
The type of an entity in a generic backend implementation.

This will probably be some sort of DataFrame implementation.

"""

StageSuccessful = bool
"""Whether a stage has completed successfully."""

