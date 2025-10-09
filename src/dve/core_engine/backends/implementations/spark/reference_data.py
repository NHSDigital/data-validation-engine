# pylint: disable=no-member
"""A reference data loader for Spark."""

from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession

import dve.parser.file_handling as fh
from dve.core_engine.backends.base.reference_data import (
    BaseRefDataLoader,
    ReferenceConfig,
    ReferenceFile,
    ReferenceTable,
    ReferenceURI,
)
from dve.core_engine.type_hints import EntityName
from dve.parser.type_hints import URI


# pylint: disable=too-few-public-methods
class SparkRefDataLoader(BaseRefDataLoader[DataFrame]):
    """A reference data loader using already existing Apache Spark Tables."""

    spark: SparkSession
    """The Spark session for the backend."""
    dataset_config_uri: Optional[URI] = None
    """The location of the dischema file defining business rules"""

    def __init__(
        self,
        reference_entity_config: Dict[EntityName, ReferenceConfig],
        **kwargs,
    ) -> None:
        super().__init__(reference_entity_config, **kwargs)
        if not self.spark:
            raise AttributeError("Spark session must be provided")

    def load_table(self, config: ReferenceTable) -> DataFrame:
        return self.spark.table(f"{config.fq_table_name}")

    def load_file(self, config: ReferenceFile) -> DataFrame:
        if not self.dataset_config_uri:
            raise AttributeError("dataset_config_uri must be specified if using relative paths")
        target_location = fh.build_relative_uri(self.dataset_config_uri, config.filename)
        return self.spark.read.parquet(target_location)

    def load_uri(self, config: ReferenceURI) -> DataFrame:
        return self.spark.read.parquet(config.uri)
