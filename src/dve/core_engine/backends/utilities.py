"""Necessary, otherwise uncategorised backend functionality."""

from datetime import date, datetime
from decimal import Decimal
import sys
from typing import Type

from dataclasses import is_dataclass
from pydantic import BaseModel, create_model

from dve.core_engine.type_hints import Messages
from dve.core_engine.backends.base.utilities import _get_non_heterogenous_type

import polars as pl  # type: ignore
from polars.datatypes.classes import DataTypeClass as PolarsType
from typing import Any, ClassVar, Dict, Set, Union

# We need to rely on a Python typing implementation detail in Python <= 3.7.
if sys.version_info[:2] <= (3, 7):
    # Crimes against typing.
    from typing import _GenericAlias  # type: ignore

    from typing_extensions import Annotated, get_args, get_origin, get_type_hints
else:
    from typing import Annotated, get_args, get_origin, get_type_hints

PYTHON_TYPE_TO_POLARS_TYPE: Dict[type, PolarsType] = {
    # issue with decimal conversion at the moment...
    str: pl.Utf8,  # type: ignore
    int: pl.Int64,  # type: ignore
    bool: pl.Boolean,  # type: ignore
    float: pl.Float64,  # type: ignore
    bytes: pl.Binary,  # type: ignore
    date: pl.Date,  # type: ignore
    datetime: pl.Datetime,  # type: ignore
    Decimal: pl.Utf8,  # type: ignore
}
"""A mapping of Python types to the equivalent Polars types."""


def stringify_type(type_: type) -> type:
    """Stringify an individual type."""
    if isinstance(type_, type):  # A model, return the contents.
        if issubclass(type_, BaseModel):
            return stringify_model(type_)

    is_complex = create_model("", t=(type_, ...)).__fields__["t"].is_complex()
    if not is_complex:  # A non-container type, return string.
        return str

    origin = get_origin(type_)
    if origin is None:  # A non-generic container type, return as-is
        return type_

    type_args = get_args(type_)
    if not type_args:
        return origin

    string_type_args = tuple(map(stringify_type, type_args))
    if sys.version_info[:2] <= (3, 7):
        if not isinstance(origin, _GenericAlias) and isinstance(type_, _GenericAlias):
            return type_.copy_with(string_type_args)
    return origin[string_type_args]


def stringify_model(model: Type[BaseModel]) -> Type[BaseModel]:
    """Stringify a `pydantic` model."""
    fields = {}
    for field_name, field in model.__fields__.items():
        fields[field_name] = (stringify_type(field.annotation), ...)
    return create_model(model.__name__, **fields)  # type: ignore


def dedup_messages(messages: Messages) -> Messages:
    """Deduplicate a message list.

    This is relatively expensive, so should really only be used for file reader messages.

    For file reader messages, this is necessary because in some cases (e.g. with XML files)
    we might be reading the same file more than once for different entities and producing the
    same message. This could lead to some confusion for end users if not addressed.

    """
    return list(dict.fromkeys(messages))

def get_polars_type_from_annotation(type_annotation: Any) -> PolarsType:
    """Get a polars type from a Python type annotation.

    Supported types  are any of the following (this definition is recursive):
    - Supported basic Python types. These are:
      * `str`: pl.Utf8
      * `int`: pl.Int64
      * `bool`: pl.Boolean
      * `float`: pl.Float64
      * `bytes`: pl.Binary
      * `datetime.date`: pl.Date
      * `datetime.datetime`: pl.Datetime
      * `decimal.Decimal`: pl.Decimal with precision of 38 and scale of 18
    - A list of supported types (e.g. `List[str]` or `typing.List[str]`).
      This will return a pl.List type (variable length)
    - A `typing.Optional` type or a `typing.Union` of the type and `None` (e.g.
      `typing.Optional[str]`, `typing.Union[List[str], None]`). This will remove the
      'optional' wrapper and return the inner type
    - A subclass of `typing.TypedDict` with values typed using supported types. This
      will parse the value types as Polars types and return a Polars Struct.
    - A dataclass or `pydantic.main.ModelMetaClass` with values typed using supported types.
      This will parse the field types as Polars types and return a Polars Struct.
    - Any supported type, with a `typing_extensions.Annotated` wrapper.
    - A `decimal.Decimal` wrapped with `typing_extensions.Annotated` with a `DecimalConfig`
      indicating precision and scale. This will return a Polars Decimal
      with the specfied scale and precision.
    - A `pydantic.types.condecimal` created type.

    Any `ClassVar` types within `TypedDict`s, dataclasses, or `pydantic` models will be
    ignored.

    """
    type_origin = get_origin(type_annotation)

    # An `Optional` or `Union` type, check to ensure non-heterogenity.
    if type_origin is Union:
        python_type = _get_non_heterogenous_type(get_args(type_annotation))
        return get_polars_type_from_annotation(python_type)

    # Type hint is e.g. `List[str]`, check to ensure non-heterogenity.
    if type_origin is list or (isinstance(type_origin, type) and issubclass(type_origin, list)):
        element_type = _get_non_heterogenous_type(get_args(type_annotation))
        return pl.List(get_polars_type_from_annotation(element_type))  # type: ignore

    if type_origin is Annotated:
        python_type, *other_args = get_args(type_annotation)  # pylint: disable=unused-variable
        return get_polars_type_from_annotation(python_type)
    # Ensure that we have a concrete type at this point.
    if not isinstance(type_annotation, type):
        raise ValueError(f"Unsupported type annotation {type_annotation!r}")

    if (
        # Type hint is a dict subclass, but not dict. Possibly a `TypedDict`.
        (issubclass(type_annotation, dict) and type_annotation is not dict)
        # Type hint is a dataclass.
        or is_dataclass(type_annotation)
        # Type hint is a `pydantic` model.
        or (type_origin is None and issubclass(type_annotation, BaseModel))
    ):
        fields: Dict[str, PolarsType] = {}
        for field_name, field_annotation in get_type_hints(type_annotation).items():
            # Technically non-string keys are disallowed, but people are bad.
            if not isinstance(field_name, str):
                raise ValueError(
                    f"Dictionary/Dataclass keys must be strings, got {type_annotation!r}"
                )  # pragma: no cover
            if get_origin(field_annotation) is ClassVar:
                continue

            fields[field_name] = get_polars_type_from_annotation(field_annotation)

        if not fields:
            raise ValueError(
                f"No type annotations in dict/dataclass type (got {type_annotation!r})"
            )

        return pl.Struct(fields)  # type: ignore

    if type_annotation is list:
        raise ValueError(
            f"List must have type annotation (e.g. `List[str]`), got {type_annotation!r}"
        )
    if type_annotation is dict or type_origin is dict:
        raise ValueError(f"Dict must be `typing.TypedDict` subclass, got {type_annotation!r}")

    for type_ in type_annotation.mro():
        polars_type = PYTHON_TYPE_TO_POLARS_TYPE.get(type_)
        if polars_type:
            return polars_type
    raise ValueError(f"No equivalent DuckDB type for {type_annotation!r}")
