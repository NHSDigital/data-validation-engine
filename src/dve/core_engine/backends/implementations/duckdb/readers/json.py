"""A csv reader to create duckdb relations"""

# pylint: disable=arguments-differ
from collections.abc import Iterator
from typing import Any, Optional

from duckdb import DuckDBPyRelation, read_json
from pydantic import BaseModel

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
        *,
        json_format: Optional[str] = "array",
        **_,
    ):
        self._json_format = json_format

        super().__init__()

    def read_to_py_iterator(
        self, resource: URI, entity_name: EntityName, schema: type[BaseModel]
    ) -> Iterator[dict[str, Any]]:
        """Creates an iterable object of rows as dictionaries"""
        return self.read_to_relation(resource, entity_name, schema).pl().iter_rows(named=True)

    @read_function(DuckDBPyRelation)
    def read_to_relation(  # pylint: disable=unused-argument
        self, resource: URI, entity_name: EntityName, schema: type[BaseModel]
    ) -> DuckDBPyRelation:
        """Returns a relation object from the source json"""

        ddb_schema: dict[str, SQLType] = {
            fld.name: str(get_duckdb_type_from_annotation(fld.annotation))  # type: ignore
            for fld in schema.__fields__.values()
        }

        return read_json(resource, columns=ddb_schema, format=self._json_format)  # type: ignore
