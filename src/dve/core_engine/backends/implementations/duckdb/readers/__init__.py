"""Readers for use with duckdb backend"""

from .csv import DuckDBCSVReader, DuckDBCSVRepeatingHeaderReader, PolarsToDuckDBCSVReader
from .json import DuckDBJSONReader
from .xml import DuckDBXMLStreamReader

__all__ = [
    "DuckDBCSVReader",
    "DuckDBCSVRepeatingHeaderReader",
    "DuckDBJSONReader",
    "DuckDBXMLStreamReader",
    "PolarsToDuckDBCSVReader",
]
