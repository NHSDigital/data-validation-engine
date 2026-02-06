# pylint: disable=no-member
"""A reference data loader for Spark."""

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

import dve.parser.file_handling as fh
from dve.core_engine.backends.base.reference_data import (
    BaseRefDataLoader,
    ReferenceConfig,
    ReferenceFile,
    ReferenceTable,
    ReferenceURI,
    mark_refdata_file_extension,
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
        reference_entity_config: dict[EntityName, ReferenceConfig],
        **kwargs,
    ) -> None:
        super().__init__(reference_entity_config, self.dataset_config_uri, **kwargs)
        if not self.spark:
            raise AttributeError("Spark session must be provided")

    def load_table(self, config: ReferenceTable) -> DataFrame:
        return self.spark.table(f"{config.fq_table_name}")

    @mark_refdata_file_extension("parquet")
    def load_parquet_file(self, uri:str) -> DataFrame:
        return self.spark.read.parquet(uri)
