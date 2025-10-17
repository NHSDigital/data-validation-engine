from dve.core_engine.backends.readers import register_reader

from .contract import DuckDBDataContract
from .readers import DuckDBCSVReader, DuckDBXMLStreamReader
from .reference_data import DuckDBRefDataLoader
from .rules import DuckDBStepImplementations

register_reader(DuckDBCSVReader)
register_reader(DuckDBXMLStreamReader)

__all__ = [
    "DuckDBDataContract",
    "DuckDBRefDataLoader",
    "DuckDBStepImplementations",
]
