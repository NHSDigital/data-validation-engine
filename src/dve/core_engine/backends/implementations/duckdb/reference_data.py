"""A reference data loader for duckdb."""

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from pyarrow import ipc  # type: ignore

from dve.core_engine.backends.base.reference_data import (
    BaseRefDataLoader,
    ReferenceConfig,
    ReferenceTable,
    mark_refdata_file_extension,
)
from dve.core_engine.type_hints import EntityName
from dve.parser.type_hints import URI


# pylint: disable=too-few-public-methods
class DuckDBRefDataLoader(BaseRefDataLoader[DuckDBPyRelation]):
    """A reference data loader using already existing DuckDB tables."""

    def __init__(
        self,
        connection: DuckDBPyConnection,
        reference_data_config: dict[EntityName, ReferenceConfig],
        dataset_config_uri: URI,
        **kwargs,
    ) -> None:
        super().__init__(reference_data_config, dataset_config_uri, **kwargs)

        self.connection = connection

        if not self.connection:
            raise AttributeError("DuckDBConnection must be specified")

    def load_table(self, config: ReferenceTable) -> DuckDBPyRelation:
        """Load reference entity from a database table"""
        return self.connection.sql(f"select * from {config.fq_table_name}")

    @mark_refdata_file_extension("parquet")
    def load_parquet_file(self, uri: str) -> DuckDBPyRelation:
        """Load a parquet file into a duckdb relation"""
        return self.connection.read_parquet(uri)

    @mark_refdata_file_extension("arrow")
    def load_arrow_file(self, uri: str) -> DuckDBPyRelation:
        """Load an arrow ipc file into a duckdb relation"""
        return self.connection.from_arrow(ipc.open_stream(uri).read_all())  # type:ignore
