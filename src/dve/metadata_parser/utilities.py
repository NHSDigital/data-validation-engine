"""Utility functions for the metadata parser."""

from collections.abc import Mapping
from types import ModuleType
from typing import TYPE_CHECKING, Any, Optional, Union

from typing_extensions import Protocol

from dve.metadata_parser import exc

if TYPE_CHECKING:
    from dve.metadata_parser.models import FieldSpecification


class TypeCallable(Protocol):  # pylint: disable=too-few-public-methods
    """A callable which returns a type."""

    def __call__(self, *args, **kwds: Any) -> type: ...


FieldTypeOption = Union[type, TypeCallable, "FieldSpecification"]
"""The types of field type specification within a data contract."""


def chain_get(
    item: str, *mappings: Union[Mapping[str, FieldTypeOption], ModuleType]
) -> FieldTypeOption:
    """Looks for a given item within a number of mappings in the order they
    are passed.

    Args:
     - 'item': String of the item been searched for (e.g. "str", "datetime")
     - *mappings: A sequence of either mapping or something with attribute
       access (e.g. a module) which might map 'item' to a type.

    Raises:
     - `TypeNotFoundError`: raised when the item isn't found in any of the
       mappings

    Returns:
        type: The type object
    """
    for mapping in mappings:
        try:
            result = mapping.get(item)
            if result is None:
                continue
        except AttributeError:
            result = getattr(mapping, item, None)
            if result is None:
                continue
        return result

    raise exc.TypeNotFoundError(f"Callable or type ({item!r}) not found")


def generate_alphanumeric_type_name(max_digits: int, min_digits: Optional[int] = 0) -> str:
    """Generates a dynamic alphanumeric type name based on the max digits and min digits provided"""
    if max_digits is None:
        raise ValueError("Alphanumeric type must have a max digits value defined.")

    if (max_digits == min_digits) or (max_digits is not None and min_digits is None):
        return f"AN{max_digits}"

    if min_digits is None:
        min_digits = 1

    return f"AN{min_digits}_{max_digits}"
