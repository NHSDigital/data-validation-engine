"""Spark-specific readers."""

from dve.core_engine.backends.implementations.spark.readers.csv import SparkCSVReader
from dve.core_engine.backends.implementations.spark.readers.json import SparkJSONReader
from dve.core_engine.backends.implementations.spark.readers.xml import (
    SparkXMLReader,
    SparkXMLStreamReader,
)

__all__ = [
    "SparkCSVReader",
    "SparkJSONReader",
    "SparkXMLReader",
    "SparkXMLStreamReader",
]
