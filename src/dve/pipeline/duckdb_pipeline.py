"""DuckDB implementation for `Pipeline` object."""

from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from dve.core_engine.backends.base.reference_data import BaseRefDataLoader
from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager
from dve.core_engine.backends.implementations.duckdb.contract import DuckDBDataContract
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import duckdb_get_entity_count
from dve.core_engine.backends.implementations.duckdb.rules import DuckDBStepImplementations
from dve.core_engine.models import SubmissionInfo
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
        processed_files_path: URI,
        audit_tables: DDBAuditingManager,
        connection: DuckDBPyConnection,
        rules_path: Optional[URI],
        submitted_files_path: Optional[URI],
        reference_data_loader: Optional[type[BaseRefDataLoader]] = None,
        job_run_id: Optional[int] = None,
    ):
        self._connection = connection
        super().__init__(
            processed_files_path,
            audit_tables,
            DuckDBDataContract(connection=self._connection),
            DuckDBStepImplementations.register_udfs(connection=self._connection),
            rules_path,
            submitted_files_path,
            reference_data_loader,
            job_run_id,
        )

    # pylint: disable=arguments-differ
    def write_file_to_parquet(  # type: ignore
        self, submission_file_uri: URI, submission_info: SubmissionInfo, output: URI
    ):
        return super().write_file_to_parquet(
            submission_file_uri, submission_info, output, DuckDBPyRelation
        )
