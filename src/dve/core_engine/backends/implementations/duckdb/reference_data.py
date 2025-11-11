"""A reference data loader for duckdb."""

from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

import dve.parser.file_handling as fh
from dve.core_engine.backends.base.reference_data import (
    BaseRefDataLoader,
    ReferenceConfigUnion,
    ReferenceFile,
    ReferenceTable,
    ReferenceURI,
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
        super().__init__(reference_entity_config, **kwargs)

        if not self.connection:
            raise AttributeError("DuckDBConnection must be specified")

    def load_table(self, config: ReferenceTable) -> DuckDBPyRelation:
        """Load reference entity from a database table"""
        return self.connection.sql(f"select * from {config.fq_table_name}")

    def load_file(self, config: ReferenceFile) -> DuckDBPyRelation:
        "Load reference entity from a relative file path"
        if not self.dataset_config_uri:
            raise AttributeError("dataset_config_uri must be specified if using relative paths")
        target_location = fh.build_relative_uri(self.dataset_config_uri, config.filename)
        if isinstance(_get_implementation(self.dataset_config_uri), LocalFilesystemImplementation):
            target_location = file_uri_to_local_path(target_location).as_posix()
        return self.connection.read_parquet(target_location)

    def load_uri(self, config: ReferenceURI) -> DuckDBPyRelation:
        "Load reference entity from an absolute URI"
        if isinstance(_get_implementation(config.uri), LocalFilesystemImplementation):
            target_location = file_uri_to_local_path(config.uri).as_posix()
        else:
            target_location = config.uri
        return self.connection.read_parquet(target_location)
