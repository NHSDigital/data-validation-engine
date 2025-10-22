"""A csv reader to create duckdb relations"""

# pylint: disable=arguments-differ
from typing import Any, Dict, Iterator, Optional, Type

from duckdb import DuckDBPyRelation, read_json
from pydantic import BaseModel
from typing_extensions import Literal

from dve.core_engine.backends.base.reader import BaseFileReader, read_function
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    duckdb_write_parquet,
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.types import SQLType
from dve.core_engine.type_hints import URI, EntityName


@duckdb_write_parquet
class DuckDBJSONReader(BaseFileReader):
    """A reader for JSON files"""
    
    def __init__(
        self,
        format: Optional[str] = "array"
    ):
        self._format = format

        super().__init__()

    def read_to_py_iterator(
        self, resource: URI, entity_name: EntityName, schema: Type[BaseModel]
    ) -> Iterator[Dict[str, Any]]:
        """Creates an iterable object of rows as dictionaries"""
        return self.read_to_relation(resource, entity_name, schema).pl().iter_rows(named=True)

    @read_function(DuckDBPyRelation)
    def read_to_relation(  # pylint: disable=unused-argument
        self, resource: URI, entity_name: EntityName, schema: Type[BaseModel]
    ) -> DuckDBPyRelation:
        """Returns a relation object from the source json"""

        ddb_schema: Dict[str, SQLType] = {
            fld.name: str(get_duckdb_type_from_annotation(fld.annotation))  # type: ignore
            for fld in schema.__fields__.values()
        }

        return read_json(resource,
                         columns=ddb_schema,
                         format=self._format)
