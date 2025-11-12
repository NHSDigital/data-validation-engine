"""Abstract implementation of the file parser."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from inspect import ismethod
from typing import Any, ClassVar, Optional, TypeVar

from pydantic import BaseModel
from typing_extensions import Protocol

from dve.core_engine.backends.exceptions import ReaderLacksEntityTypeSupport
from dve.core_engine.backends.types import EntityName, EntityType
from dve.core_engine.type_hints import URI, ArbitraryFunction, WrapDecorator

T = TypeVar("T")
ET_co = TypeVar("ET_co", covariant=True)
# This needs to be defined outside the class since otherwise mypy expects
# BaseFileReader to be generic:
_ReadFunctions = dict[type[T], "_UnboundReadFunction[T]"]
"""A convenience type indicating a mapping from type to reader function."""
_ENTITY_TYPE_ATTR_NAME = "_read_func_entity_type"
"""The name of the read function's entity type annotation attribute."""


class _UnboundReadFunction(Protocol[ET_co]):  # pylint: disable=too-few-public-methods
    """The protocol required to implement a read function for a new entity type."""

    @staticmethod
    def __call__(  # pylint: disable=bad-staticmethod-argument
        self: "BaseFileReader",  # This is the protocol for an _unbound_ method.
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> ET_co: ...


def read_function(entity_type: T) -> WrapDecorator:
    """A decorator function which tags read function methods within a reader class.
    This is used to add support for different entity types in reader implementations.

    """

    def reader_impl_decorator(func: ArbitraryFunction) -> ArbitraryFunction:
        """Wrap a read function to tag the entity type it implements support for."""
        setattr(func, _ENTITY_TYPE_ATTR_NAME, entity_type)
        return func

    return reader_impl_decorator


class BaseFileReader(ABC):
    """An abstract representation of a reader for some file type."""

    __read_methods__: ClassVar[_ReadFunctions] = {}
    """
    A dictionary mapping implemented entity types to their read functions.

    This enables readers to implement optimised support for specific entity
    types (rather than relying on the data contract having an optimised implementation,
    or on the 'fallback' via a Python iterator.

    This is set and populated in `__init_subclass__` by identifying methods
    decorated with the '@read_function' decorator, and is used in `read_entity_type`.

    """

    def __init_subclass__(cls, *_, **__) -> None:
        """When this class is subclassed, create and populate the `__read_methods__`
        class variable for the subclass.

        """
        cls.__read_methods__ = {}

        for attr_name in dir(cls):
            method = getattr(cls, attr_name, None)
            if not (ismethod(method) or callable(method)):
                continue

            entity_type: Optional[type] = getattr(method, _ENTITY_TYPE_ATTR_NAME, None)
            if entity_type is None:
                continue

            cls.__read_methods__[entity_type] = method  # type: ignore

    @abstractmethod
    def read_to_py_iterator(
        self,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> Iterator[dict[str, Any]]:
        """Iterate through the contents of the resource, yielding dicts
        representing each record.

        NOTE: Simple types should either be returned as strings (if present) or
        `None`. Format validation, casting, and parsing should be done in the
        data contract.

        """
        raise NotImplementedError

    def read_to_entity_type(
        self,
        entity_type: type[EntityType],
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> EntityType:
        """Read to the specified entity type, if supported.

        NOTE: Simple types should either be returned as strings (if present) or
        `None`. Format validation, casting, and parsing should be done in the
        data contract.

        """
        if entity_name == Iterator[dict[str, Any]]:
            return self.read_to_py_iterator(resource, entity_name, schema)  # type: ignore

        try:
            reader_func = self.__read_methods__[entity_type]
        except KeyError as err:
            raise ReaderLacksEntityTypeSupport(entity_type=entity_type) from err

        return reader_func(self, resource, entity_name, schema)

    def write_parquet(
        self,
        entity: EntityType,
        target_location: URI,
        schema: Optional[type[BaseModel]] = None,
        **kwargs,
    ) -> URI:
        """Write entity to parquet.

        NOTE: Simple types should be cast as strings (if present) or None.
        If schema supplied then all simple types will be coerced to strings.

        """
        raise NotImplementedError(f"write_parquet not implemented in {self.__class__}")
