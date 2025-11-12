"""Core file readers.

Readers must be imported here to be automatically registered at startup.

"""

import warnings

from dve.core_engine.backends.base.reader import BaseFileReader
from dve.core_engine.backends.readers.csv import CSVFileReader
from dve.core_engine.backends.readers.xml import BasicXMLFileReader, XMLStreamReader

ReaderName = str
"""The name of a reader type."""

CORE_READERS: list[type[BaseFileReader]] = [CSVFileReader, BasicXMLFileReader, XMLStreamReader]
"""A list of core reader types which should be registered."""

_READER_REGISTRY: dict[ReaderName, type[BaseFileReader]] = {}
"""A global registry of supported reader types."""


def register_reader(reader_class: type[BaseFileReader]):
    """Register a reader type, making it accessible to the engine."""
    if not issubclass(reader_class, BaseFileReader):
        raise TypeError(f"Reader type {reader_class} is not 'BaseFileReader' subclass")

    reader_name = reader_class.__name__
    if reader_name in _READER_REGISTRY:
        warnings.warn(f"Reader {reader_name} overwriting existing reader")

    _READER_REGISTRY[reader_name] = reader_class


def get_reader(reader_name: ReaderName) -> type[BaseFileReader]:
    """Get the reader type from the registry by name."""
    return _READER_REGISTRY[reader_name]


for _reader_type in CORE_READERS:
    register_reader(_reader_type)
