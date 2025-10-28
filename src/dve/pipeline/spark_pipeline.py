"""Spark implementation for `Pipeline` object."""

from concurrent.futures import Executor
from typing import List, Optional, Tuple, Type

from pyspark.sql import DataFrame, SparkSession

from dve.core_engine.backends.base.reference_data import BaseRefDataLoader
from dve.core_engine.backends.implementations.spark.auditing import SparkAuditingManager
from dve.core_engine.backends.implementations.spark.contract import SparkDataContract
from dve.core_engine.backends.implementations.spark.rules import SparkStepImplementations
from dve.core_engine.backends.implementations.spark.spark_helpers import spark_get_entity_count
from dve.core_engine.models import SubmissionInfo
from dve.core_engine.type_hints import URI, Failed
from dve.pipeline.pipeline import BaseDVEPipeline
from dve.pipeline.utils import unpersist_all_rdds


# pylint: disable=abstract-method
@spark_get_entity_count
class SparkDVEPipeline(BaseDVEPipeline):
    """
    Polymorphed Pipeline class for running a DVE Pipeline with Spark
    """

    def __init__(
        self,
        audit_tables: SparkAuditingManager,
        job_run_id: int,
        rules_path: Optional[URI],
        processed_files_path: Optional[URI],
        submitted_files_path: Optional[URI],
        reference_data_loader: Optional[Type[BaseRefDataLoader]] = None,
        spark: Optional[SparkSession] = None,
    ):
        self._spark = spark if spark else SparkSession.builder.getOrCreate()
        super().__init__(
            audit_tables,
            job_run_id,
            SparkDataContract(spark_session=self._spark),
            SparkStepImplementations.register_udfs(self._spark),
            rules_path,
            processed_files_path,
            submitted_files_path,
            reference_data_loader,
        )

    # pylint: disable=arguments-differ
    def write_file_to_parquet(  # type: ignore
        self, submission_file_uri: URI, submission_info: SubmissionInfo, output: URI
    ):
        return super().write_file_to_parquet(
            submission_file_uri, submission_info, output, DataFrame
        )

    def business_rule_step(
        self,
        pool: Executor,
        files: List[Tuple[SubmissionInfo, Failed]],
    ):
        successful_files, unsucessful_files, failed_processing = super().business_rule_step(
            pool, files
        )
        unpersist_all_rdds(self._spark)
        return successful_files, unsucessful_files, failed_processing
