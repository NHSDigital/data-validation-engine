"""Readers for use with duckdb backend"""

from .csv import DuckDBCSVReader
from .json import DuckDBJSONReader
from .xml import DuckDBXMLStreamReader

__all__ = [
    "DuckDBCSVReader",
    "DuckDBJSONReader",
    "DuckDBXMLStreamReader",
]
