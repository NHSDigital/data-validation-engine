"""An implementation of the data contract in Apache spark."""

import logging
from collections.abc import Iterator
from itertools import islice
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, lit
from pyspark.sql.types import ArrayType, DataType, MapType, StringType, StructType

from dve.core_engine.backends.base.contract import BaseDataContract, reader_override
from dve.core_engine.backends.base.utilities import generate_error_casting_entity_message
from dve.core_engine.backends.exceptions import (
    FieldCountMismatch,
    NoComplexSchemaSupport,
    SchemaMismatch,
)
from dve.core_engine.backends.implementations.spark.spark_helpers import (
    df_is_empty,
    get_type_from_annotation,
    spark_read_parquet,
    spark_write_parquet,
)
from dve.core_engine.backends.implementations.spark.types import SparkEntities
from dve.core_engine.backends.metadata.contract import DataContractMetadata
from dve.core_engine.backends.readers import CSVFileReader
from dve.core_engine.backends.types import StageSuccessful
from dve.core_engine.constants import ROWID_COLUMN_NAME
from dve.core_engine.type_hints import URI, EntityLocations, EntityName, Messages
from dve.common.error_utils import BackgroundMessageWriter, dump_feedback_errors, dump_processing_errors, get_feedback_errors_uri

COMPLEX_TYPES: set[type[DataType]] = {StructType, ArrayType, MapType}
"""Spark types indicating complex types."""


@spark_write_parquet
@spark_read_parquet
class SparkDataContract(BaseDataContract[DataFrame]):
    """An implementation of a data contract in Spark.

    This uses Spark's RDD functionality to apply the data contract.

    """

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        spark_session: Optional[SparkSession] = None,
        debug: bool = False,
        **kwargs: Any,
    ):
        if spark_session is None:
            spark_session = SparkSession.builder.getOrCreate()

        self.spark_session = spark_session
        """The active Spark session."""
        self.debug = debug
        """A bool indicating whether to enable debug logging."""

        super().__init__(logger, **kwargs)

    def _cache_records(self, dataframe: DataFrame, working_dir: URI) -> URI:
        """Write a chunk of records out to the cache dir, returning the path
        to the parquet file.

        """
        chunk_uri = "/".join((working_dir.rstrip("/"), str(uuid4()))) + ".parquet"
        dataframe.write.parquet(chunk_uri)
        return chunk_uri

    def create_entity_from_py_iterator(
        self, entity_name: EntityName, records: Iterator[dict[str, Any]], schema: type[BaseModel]
    ) -> DataFrame:
        return self.spark_session.createDataFrame(  # type: ignore
            records,
            schema=get_type_from_annotation(schema),
        )

    def apply_data_contract(
        self,
        working_dir: URI,
        entities: SparkEntities,
        entity_locations: EntityLocations,
        contract_metadata: DataContractMetadata,
        key_fields: Optional[dict[str, list[str]]] = None
    ) -> tuple[SparkEntities, Messages, StageSuccessful]:
        self.logger.info("Applying data contracts")
        
        entity_locations = {} if not entity_locations else entity_locations
        feedback_errors_uri = get_feedback_errors_uri(working_dir, "data_contract")

        successful = True
        for entity_name, record_df in entities.items():
            spark_schema = get_type_from_annotation(contract_metadata.schemas[entity_name])

            if df_is_empty(record_df):
                self.logger.warning(f"+ Empty dataframe for {entity_name}")

                entities[entity_name] = self.spark_session.createDataFrame(  # type: ignore
                    [], schema=spark_schema
                ).withColumn(ROWID_COLUMN_NAME, lit(None).cast(StringType()))
                continue

            if self.debug:
                # Note, the count will realise the dataframe, so only do this
                # if debugging.
                self.logger.info(
                    f"+ Applying contract to: {entity_name} ({record_df.count()} rows)"
                )
            else:
                self.logger.info(f"+ Applying contract to: {entity_name}")

            row_validator = contract_metadata.validators[entity_name]
            self.logger.info("+ Loading_rdd")
            self.logger.info("+ Splitting messages and valid_rows")
            validated = (
                record_df.rdd.map(lambda row: row.asDict(True)).map(row_validator)
                # .persist(storageLevel=StorageLevel.MEMORY_AND_DISK)
            )
            with BackgroundMessageWriter(working_dir, "data_contract", key_fields, self.logger) as msg_writer:
                messages = validated.flatMap(lambda row: row[1]).filter(bool).toLocalIterator()
                while True:
                    batch = list(islice(messages, 10000))
                    if not batch:
                        break
                    msg_writer.write_queue.put(batch)

            try:
                record_df = record_df.select(
                    [
                        col(column.name).cast(column.dataType)
                        for column in spark_schema
                        if column.name in record_df.columns
                    ]
                )
            except Exception as err:  # pylint: disable=broad-except
                successful = False
                self.logger.error(f"Error in converting to dataframe: {err}")
                dump_processing_errors(working_dir,
                                       [generate_error_casting_entity_message(entity_name)])
                continue

            if self.debug:
                # Note, the count will realise the dataframe, so only do this
                # if debugging.
                pre_convert_row_count = record_df.count()
                self.logger.info(f"+ Converting to Dataframe: ({pre_convert_row_count} rows)")
            else:
                pre_convert_row_count = 0
                self.logger.info("+ Converting to Dataframe")

            entities[entity_name] = record_df
            # validated.unpersist()

            if self.debug:
                post_convert_row_count = entities[entity_name].count()
                self.logger.info(f"+ Converted to Dataframe: ({post_convert_row_count} rows)")
                if post_convert_row_count != pre_convert_row_count:
                    raise ValueError(
                        f"Row count mismatch for {entity_name}"
                        f" ({pre_convert_row_count} vs {post_convert_row_count})"
                    )
            else:
                self.logger.info("+ Converted to Dataframe")

        return entities, feedback_errors_uri, successful

    @reader_override(CSVFileReader)
    def read_csv_file(
        self,
        reader: CSVFileReader,
        resource: URI,
        entity_name: EntityName,  # pylint: disable=unused-argument
        schema: type[BaseModel],
    ) -> DataFrame:
        """Read a CSV file using Apache Spark."""
        reader_args: dict[str, Any] = {
            "inferSchema": False,
            "header": reader.header,
            "multiLine": True,
            "sep": reader.delimiter,
            "escape": reader.escape_char,
            "quote": reader.quote_char,
            "ignoreLeadingWhiteSpace": reader.trim_cells,
            "ignoreTrailingWhiteSpace": reader.trim_cells,
        }

        spark_schema = get_type_from_annotation(schema)
        if any(type(field.dataType) in COMPLEX_TYPES for field in spark_schema.fields):
            raise NoComplexSchemaSupport("CSV schema cannot contain complex types")

        dataframe = self.spark_session.read.csv(resource, **reader_args)

        schema_names = spark_schema.fieldNames()
        file_names = dataframe.columns
        if reader.header:
            missing_uppercase = set(map(str.upper, schema_names)) - set(map(str.upper, file_names))
            if missing_uppercase:
                missing_fields = {
                    name for name in schema_names if name.upper() in missing_uppercase
                }
                raise SchemaMismatch(
                    "CSV missing field names required by the schema", missing_fields=missing_fields
                )
        else:
            n_expected = len(schema_names)
            n_fields = len(file_names)
            if n_fields != n_expected:
                raise FieldCountMismatch(
                    "CSV does not have named fields the number of fields parsed does "
                    + "not match the schema",
                    n_actual_fields=n_fields,
                    n_expected_fields=n_expected,
                )
            dataframe = dataframe.toDF(*schema_names)

        null_values = list(map(lit, reader.null_values))
        null_values.append(lit(""))
        columns = []
        for column_name in schema_names:
            column = col(column_name)
            column = sf.when(column.isin(null_values), lit(None)).otherwise(column)
            columns.append(column.alias(column_name))

        return dataframe.select(*columns)
