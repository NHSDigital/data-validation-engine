"""A reference data loader for duckdb."""

from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation
import pyarrow.ipc as ipc

import dve.parser.file_handling as fh
from dve.core_engine.backends.base.reference_data import (
    BaseRefDataLoader,
    ReferenceConfigUnion,
    ReferenceFile,
    ReferenceTable,
    ReferenceURI,
    mark_refdata_file_extension
)
from dve.core_engine.type_hints import EntityName
from dve.parser.file_handling.implementations.file import (
    LocalFilesystemImplementation,
    file_uri_to_local_path,
)
from dve.parser.file_handling.service import _get_implementation
from dve.parser.type_hints import URI


# pylint: disable=too-few-public-methods
class DuckDBRefDataLoader(BaseRefDataLoader[DuckDBPyRelation]):
    """A reference data loader using already existing DuckDB tables."""

    connection: DuckDBPyConnection
    """The DuckDB connection for the backend."""
    dataset_config_uri: Optional[URI] = None
    """The location of the dischema file"""

    def __init__(
        self,
        reference_entity_config: dict[EntityName, ReferenceConfigUnion],
        **kwargs,
    ) -> None:
        super().__init__(reference_entity_config, self.dataset_config_uri, **kwargs)

        if not self.connection:
            raise AttributeError("DuckDBConnection must be specified")

    def load_table(self, config: ReferenceTable) -> DuckDBPyRelation:
        """Load reference entity from a database table"""
        return self.connection.sql(f"select * from {config.fq_table_name}")         
    
    @mark_refdata_file_extension("parquet")
    def load_parquet_file(self, uri: str) -> DuckDBPyRelation:
        return self.connection.read_parquet(uri)
    
    @mark_refdata_file_extension("arrow")
    def load_arrow_file(self, uri: str) -> DuckDBPyRelation:
        return self.connection.from_arrow(ipc.open_file(uri).read_all())
            
