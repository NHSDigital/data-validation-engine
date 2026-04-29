# pylint: disable=no-member
"""A reference data loader for Spark."""

from pyspark.sql import DataFrame, SparkSession

from dve.core_engine.backends.base.reference_data import (
    BaseRefDataLoader,
    ReferenceConfig,
    ReferenceTable,
    mark_refdata_file_extension,
)
from dve.core_engine.type_hints import EntityName
from dve.parser.type_hints import URI


# pylint: disable=too-few-public-methods
class SparkRefDataLoader(BaseRefDataLoader[DataFrame]):
    """A reference data loader using already existing Apache Spark Tables.
    reference_entity_config and dataset_config_uri (if config uses relative paths)
    should be supplied using setter methods for the dataset being processed before running."""

    def __init__(
        self,
        spark: SparkSession,
        reference_data_config: dict[EntityName, ReferenceConfig],
        dataset_config_uri: URI,
        **kwargs,
    ) -> None:
        super().__init__(reference_data_config, dataset_config_uri, **kwargs)
        self.spark = spark
        if not self.spark:
            raise AttributeError("Spark session must be provided")

    def load_table(self, config: ReferenceTable) -> DataFrame:
        return self.spark.table(f"{config.fq_table_name}")

    @mark_refdata_file_extension("parquet")
    def load_parquet_file(self, uri: str) -> DataFrame:
        """Load a parquet file into a spark dataframe"""
        return self.spark.read.parquet(uri)
