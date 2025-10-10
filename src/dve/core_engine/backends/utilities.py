"""Necessary, otherwise uncategorised backend functionality."""

import sys
from typing import Type

from pydantic import BaseModel, create_model

from dve.core_engine.type_hints import Messages

# We need to rely on a Python typing implementation detail in Python <= 3.7.
if sys.version_info[:2] <= (3, 7):
    # Crimes against typing.
    from typing import _GenericAlias  # type: ignore

    from typing_extensions import get_args, get_origin
else:
    from typing import get_args, get_origin


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
