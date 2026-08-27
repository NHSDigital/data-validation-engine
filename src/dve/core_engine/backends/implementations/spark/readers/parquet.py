"""Spark Parquet Readers"""

from collections.abc import Iterator
from typing import Any, Literal, Optional

from pydantic import BaseModel
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, SparkSession

from dve.core_engine.backends.base.reader import read_function
from dve.core_engine.backends.exceptions import EmptyFileError, UnableToParseParquetError
from dve.core_engine.backends.implementations.spark.spark_helpers import (
    spark_record_index,
    spark_write_parquet,
)
from dve.core_engine.backends.readers.parquet import BaseParquetReader
from dve.core_engine.backends.readers.utilities import get_parquet_metadata_row_count
from dve.core_engine.type_hints import URI, EntityName


@spark_record_index
@spark_write_parquet
class SparkParquetReader(BaseParquetReader):
    """
    A Spark parquet reader.

    Specific reader args listed for PySpark 3.5.2:
    https://archive.apache.org/dist/spark/docs/3.5.2/sql-data-sources-parquet.html#data-source-option.
    """

    def __init__(
        self,
        *,
        datetime_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = "EXCEPTION",
        int96_rebase_mode: Literal["EXCEPTION", "CORRECTED", "LEGACY"] = "EXCEPTION",
        spark_session: Optional[SparkSession] = None,
        field_check_error_code: str = "ParquetFieldMismatch",
        field_check_error_message: str = "The submitted fields are invalid",
        **_,
    ):
        self.datetime_rebase_mode = datetime_rebase_mode
        """
        The datetimeRebaseMode option allows to specify the rebasing mode for the values of the
        DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS logical types from the Julian to Proleptic
        Gregorian calendar. Default is `"EXCEPTION"`.
        """
        self.int96_rebase_mode = int96_rebase_mode
        """
        The int96RebaseMode option allows to specify the rebasing mode for INT96 timestamps from
        the Julian to Proleptic Gregorian calendar. Default is `"EXCEPTION"`.
        """
        self.spark_session = spark_session if spark_session else SparkSession.builder.getOrCreate()  # type: ignore  # pylint: disable=C0301
        """Spark Sesssion to utilise."""

        super().__init__(
            hive_partitioning=True,
            field_check_error_code=field_check_error_code,
            field_check_error_message=field_check_error_message,
        )

    def read_to_py_iterator(
        self,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
        all_model_fields: Optional[set[str]] = None,  # pylint: disable=W0613
    ) -> Iterator[dict[URI, Any]]:
        df = self.read_to_dataframe(resource, entity_name, schema)
        yield from (record.asDict(True) for record in df.toLocalIterator())

    @read_function(DataFrame)
    def read_to_dataframe(
        self,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],  # pylint: disable=W0613
        all_model_fields: Optional[set[str]] = None,  # pylint: disable=W0613
    ) -> DataFrame:
        """Read a parquet file into a PySpark DataFrame object."""
        if get_parquet_metadata_row_count(resource) == 0:
            raise EmptyFileError(f"File at {resource} is empty.")

        read_options: dict[str, Any] = {
            "datetimeRebaseMode": self.datetime_rebase_mode,
            "int96RebaseMode": self.int96_rebase_mode,
        }

        try:
            reader = self.spark_session.read.format("parquet").options(**read_options)
            df = self.add_record_index(reader.load(resource))
        except PySparkException as exc:
            raise UnableToParseParquetError(
                entity_name, self.field_check_error_message, self.field_check_error_code
            ) from exc

        return df
