"""A 'complete' implementation of the generic backend in Spark."""

import logging
from typing import Any, Optional

from pyspark.sql import DataFrame, SparkSession

from dve.core_engine.backends.base.backend import BaseBackend
from dve.core_engine.backends.base.reference_data import ReferenceConfigUnion
from dve.core_engine.backends.implementations.spark.contract import SparkDataContract
from dve.core_engine.backends.implementations.spark.reference_data import SparkRefDataLoader
from dve.core_engine.backends.implementations.spark.rules import SparkStepImplementations
from dve.core_engine.backends.implementations.spark.spark_helpers import get_type_from_annotation
from dve.core_engine.backends.implementations.spark.types import SparkEntities
from dve.core_engine.constants import RECORD_INDEX_COLUMN_NAME
from dve.core_engine.loggers import get_child_logger, get_logger
from dve.core_engine.models import SubmissionInfo
from dve.core_engine.type_hints import URI, EntityName, EntityParquetLocations
from dve.parser.file_handling import get_resource_exists, joinuri


class SparkBackend(BaseBackend[DataFrame]):
    """An implementation of the DVE backend in Apache Spark."""

    def __init__(
        self,
        dataset_config_uri: Optional[URI] = None,
        contract: Optional[SparkDataContract] = None,
        steps: Optional[SparkStepImplementations] = None,
        logger: Optional[logging.Logger] = None,
        spark_session: Optional[SparkSession] = None,
        **kwargs: Any,
    ) -> None:
        if not logger:
            logger = get_logger("SparkBackend")

        self.spark_session = spark_session or SparkSession.builder.getOrCreate()
        """The Spark session for the backend."""
        self.dataset_config_uri = dataset_config_uri
        """The uri of the dischema specifying the DVE config"""

        if contract is None:
            contract = SparkDataContract(
                logger=get_child_logger("SparkDataContract", logger),
                spark_session=self.spark_session,
            )
        if steps is None:
            steps = SparkStepImplementations.register_udfs(
                logger=get_child_logger("SparkStepImplementations", logger)
            )
        super().__init__(contract, steps, logger, **kwargs)

    def load_reference_data(
        self,
        reference_entity_config: dict[EntityName, ReferenceConfigUnion],
        submission_info: Optional[SubmissionInfo],
    ):
        """Load the reference data as specified in the reference entity config."""
        sub_info_entity: Optional[DataFrame] = None
        if submission_info:
            sub_info_entity = self.convert_submission_info(submission_info)

        reference_data_loader = SparkRefDataLoader(
            spark=self.spark_session,
            reference_data_config=reference_entity_config,
            dataset_config_uri=self.dataset_config_uri,  # type: ignore
        )
        if sub_info_entity is not None:
            reference_data_loader.entity_cache["dve_submission_info"] = sub_info_entity

        return reference_data_loader

    def write_entities_to_parquet(
        self, entities: SparkEntities, cache_prefix: URI
    ) -> EntityParquetLocations:
        locations = {}
        self.logger.info(f"Writing entities to the output location: {cache_prefix}")
        for entity_name, entity in entities.items():
            entity = entity.drop(RECORD_INDEX_COLUMN_NAME)

            self.logger.info(f"Entity: {entity_name}")

            output_uri = joinuri(cache_prefix, "parquet_outputs", entity_name)
            if get_resource_exists(output_uri):
                self.logger.info(f"{output_uri!r} already exists - will be overwritten")

            self.logger.info(f"+ Writing parquet output to {output_uri!r}")
            entity.write.mode("overwrite").parquet(output_uri)
            locations[entity_name] = output_uri

        return locations

    def convert_submission_info(self, submission_info: SubmissionInfo) -> DataFrame:
        return self.spark_session.createDataFrame(  # type: ignore
            [submission_info.dict()], schema=get_type_from_annotation(type(submission_info))
        )
