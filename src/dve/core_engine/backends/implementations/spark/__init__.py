"""Implementation of the Apache Spark backend."""

from dve.core_engine.backends.readers import register_reader

from .backend import SparkBackend
from .contract import SparkDataContract
from .readers import (
    SparkCSVReader,
    SparkJSONReader,
    SparkXMLReader,
    SparkXMLStreamReader
)
from .reference_data import SparkRefDataLoader
from .rules import SparkStepImplementations

register_reader(SparkCSVReader)
register_reader(SparkJSONReader)
register_reader(SparkXMLReader)
register_reader(SparkXMLStreamReader)


__all__ = [
    "SparkBackend",
    "SparkDataContract",
    "SparkRefDataLoader",
    "SparkStepImplementations",
]
