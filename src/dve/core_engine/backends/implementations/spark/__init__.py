"""Implementation of the Apache Spark backend."""

from dve.core_engine.backends.readers import register_reader

from .backend import SparkBackend
from .contract import SparkDataContract
from .readers import SparkXMLReader
from .reference_data import SparkRefDataLoader
from .rules import SparkStepImplementations

register_reader(SparkXMLReader)

__all__ = [
    "SparkBackend",
    "SparkDataContract",
    "SparkRefDataLoader",
    "SparkStepImplementations",
]
