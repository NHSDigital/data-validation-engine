"""Spark-specific readers."""

from dve.core_engine.backends.implementations.spark.readers.xml import (
    SparkXMLReader,
    SparkXMLStreamReader,
)

__all__ = [
    "SparkXMLReader",
    "SparkXMLStreamReader",
]
