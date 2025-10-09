"""Readers for use with duckdb backend"""

from .csv import DuckDBCSVReader
from .xml import DuckDBXMLStreamReader

__all__ = [
    "DuckDBCSVReader",
    "DuckDBXMLStreamReader",
]
