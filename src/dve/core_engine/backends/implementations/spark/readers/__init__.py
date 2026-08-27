"""Spark-specific readers."""

from dve.core_engine.backends.implementations.spark.readers.csv import SparkCSVReader
from dve.core_engine.backends.implementations.spark.readers.json import SparkJSONReader
from dve.core_engine.backends.implementations.spark.readers.parquet import SparkParquetReader
from dve.core_engine.backends.implementations.spark.readers.xml import (
    SparkXMLReader,
    SparkXMLStreamReader,
)

__all__ = [
    "SparkCSVReader",
    "SparkJSONReader",
    "SparkParquetReader",
    "SparkXMLReader",
    "SparkXMLStreamReader",
]
