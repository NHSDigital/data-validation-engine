# mypy: disable-error-code="attr-defined"
"""An xml reader to create duckdb relations"""

from typing import Dict, Optional, Type

import polars as pl
from duckdb import DuckDBPyConnection, DuckDBPyRelation, default_connection
from pydantic import BaseModel

from dve.core_engine.backends.base.reader import read_function
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import duckdb_write_parquet
from dve.core_engine.backends.readers.xml import XMLStreamReader
from dve.core_engine.backends.utilities import get_polars_type_from_annotation, stringify_model
from dve.core_engine.type_hints import URI


@duckdb_write_parquet
class DuckDBXMLStreamReader(XMLStreamReader):
    """A reader for XML files"""

    def __init__(self, ddb_connection: Optional[DuckDBPyConnection] = None, **kwargs):
        self.ddb_connection = ddb_connection if ddb_connection else default_connection
        super().__init__(**kwargs)

    @read_function(DuckDBPyRelation)
    def read_to_relation(self, resource: URI, entity_name: str, schema: Type[BaseModel]):
        """Returns a relation object from the source xml"""
        polars_schema: Dict[str, pl.DataType] = {  # type: ignore
            fld.name: get_polars_type_from_annotation(fld.annotation)
            for fld in stringify_model(schema).__fields__.values()
        }

        _lazy_frame = pl.LazyFrame(
            data=self.read_to_py_iterator(resource, entity_name, schema), schema=polars_schema
        )
        return self.ddb_connection.sql("select * from _lazy_frame")
