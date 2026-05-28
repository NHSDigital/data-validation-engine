"""A reader implementation using the Databricks Spark CSV reader."""

from collections.abc import Iterator
from typing import Any, Optional

import pyspark.sql.functions as psf
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from dve.core_engine.backends.base.reader import BaseFileReader, read_function
from dve.core_engine.backends.exceptions import EmptyFileError
from dve.core_engine.backends.implementations.spark.spark_helpers import (
    get_type_from_annotation,
    spark_record_index,
    spark_write_parquet,
)
from dve.core_engine.backends.readers.utilities import (
    raise_message_bearing_error_on_header_differences,
)
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling import get_content_length


@spark_record_index
@spark_write_parquet
class SparkCSVReader(BaseFileReader):
    """A Spark reader for CSV files."""

    # pylint: disable=R0902
    def __init__(
        self,
        *,
        delimiter: str = ",",
        escape_char: str = "\\",
        quote_char: str = '"',
        header: bool = True,
        multi_line: bool = False,
        encoding: str = "utf-8-sig",
        null_empty_strings: bool = False,
        spark_session: Optional[SparkSession] = None,
        field_check: bool = False,
        field_check_error_code: str = "ExpectedVsActualFieldMismatch",
        field_check_error_message: str = "The submitted header is missing fields",
        **_,
    ) -> None:

        self.delimiter = delimiter
        self.escape_char = escape_char
        self.encoding = encoding
        self.quote_char = quote_char
        self.header = header
        self.multi_line = multi_line
        self.null_empty_strings = null_empty_strings
        self.spark_session = spark_session if spark_session else SparkSession.builder.getOrCreate()  # type: ignore  # pylint: disable=C0301
        self.field_check = field_check
        self.field_check_error_code = field_check_error_code
        self.field_check_error_message = field_check_error_message

        super().__init__()

    def perform_field_check(
        self, resource: URI, entity_name: str, expected_schema: type[BaseModel]
    ):
        """Check that the header of the CSV aligns with the provided model"""
        if not self.header:
            raise ValueError("Cannot perform field check without a CSV header")

        raise_message_bearing_error_on_header_differences(
            resource,
            entity_name,
            expected_schema,
            self.field_check_error_code,
            self.field_check_error_message,
            self.delimiter,
            self.quote_char,
        )

    def read_to_py_iterator(
        self, resource: URI, entity_name: EntityName, schema: type[BaseModel]
    ) -> Iterator[dict[URI, Any]]:
        df = self.read_to_dataframe(resource, entity_name, schema)
        yield from (record.asDict(True) for record in df.toLocalIterator())

    @read_function(DataFrame)
    def read_to_dataframe(
        self,
        resource: URI,
        entity_name: EntityName,  # pylint: disable=unused-argument
        schema: type[BaseModel],
    ) -> DataFrame:
        """Read a CSV file directly to a Spark DataFrame."""
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource} is empty.")

        if self.field_check:
            self.perform_field_check(resource, entity_name, schema)

        spark_schema: StructType = get_type_from_annotation(schema)
        kwargs = {
            "sep": self.delimiter,
            "header": self.header,
            "escape": self.escape_char,
            "quote": self.quote_char,
            "multiLine": self.multi_line,
        }

        df = self.add_record_index(
            self.spark_session.read.format("csv")
            .options(**kwargs)  # type: ignore
            .load(resource, schema=spark_schema)
        )

        if self.null_empty_strings:
            df = df.select(
                *[psf.trim(psf.col(c.name)).alias(c.name) for c in spark_schema.fields]
            ).replace("", None)

        return df
