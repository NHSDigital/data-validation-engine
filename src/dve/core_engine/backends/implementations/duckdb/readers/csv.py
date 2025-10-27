"""A csv reader to create duckdb relations"""

# pylint: disable=arguments-differ
from typing import Any, Dict, Iterator, Type

from duckdb import DuckDBPyConnection, DuckDBPyRelation, default_connection, read_csv
from pydantic import BaseModel

from dve.core_engine.backends.base.reader import BaseFileReader, read_function
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    duckdb_write_parquet,
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.types import SQLType
from dve.core_engine.type_hints import URI, EntityName


@duckdb_write_parquet
class DuckDBCSVReader(BaseFileReader):
    """A reader for CSV files"""

    # TODO - the read_to_relation should include the schema and determine whether to
    # TODO - stringify or not
    def __init__(
        self,
        header: bool = True,
        delim: str = ",",
        connection: DuckDBPyConnection = None,
    ):
        self.header = header
        self.delim = delim
        self._connection = connection if connection else default_connection

        super().__init__()

    def read_to_py_iterator(
        self, resource: URI, entity_name: EntityName, schema: Type[BaseModel]
    ) -> Iterator[Dict[str, Any]]:
        """Creates an iterable object of rows as dictionaries"""
        yield from self.read_to_relation(resource, entity_name, schema).pl().iter_rows(named=True)

    @read_function(DuckDBPyRelation)
    def read_to_relation(  # pylint: disable=unused-argument
        self, resource: URI, entity_name: EntityName, schema: Type[BaseModel]
    ) -> DuckDBPyRelation:
        """Returns a relation object from the source csv"""
        reader_options: Dict[str, Any] = {
            "header": self.header,
            "delimiter": self.delim,
        }

        ddb_schema: Dict[str, SQLType] = {
            fld.name: str(get_duckdb_type_from_annotation(fld.annotation))  # type: ignore
            for fld in schema.__fields__.values()
        }

        reader_options["columns"] = ddb_schema
        return read_csv(resource, **reader_options)
