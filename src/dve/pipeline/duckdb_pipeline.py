"""DuckDB implementation for `Pipeline` object."""

from typing import Optional

from duckdb import DuckDBPyConnection

from dve.core_engine.backends.base.reference_data import BaseRefDataLoader
from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager
from dve.core_engine.backends.implementations.duckdb.contract import DuckDBDataContract
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import duckdb_get_entity_count
from dve.core_engine.backends.implementations.duckdb.rules import DuckDBStepImplemetations
from dve.core_engine.type_hints import URI
from dve.pipeline.pipeline import BaseDVEPipeline


# pylint: disable=abstract-method
@duckdb_get_entity_count
class DDBDVEPipeline(BaseDVEPipeline):
    """
    Modified Pipeline class for running a DVE Pipeline with Spark
    """

    def __init__(
        self,
        audit_tables: DDBAuditingManager,
        job_run_id: int,
        connection: DuckDBPyConnection,
        rules_path: Optional[URI],
        processed_files_path: Optional[URI],
        submitted_files_path: Optional[URI],
        reference_data_loader: Optional[type[BaseRefDataLoader]] = None,
    ):
        self._connection = connection
        super().__init__(
            audit_tables,
            job_run_id,
            DuckDBDataContract(connection=self._connection),
            DuckDBStepImplemetations.register_udfs(connection=self._connection),
            rules_path,
            processed_files_path,
            submitted_files_path,
            reference_data_loader,
        )
