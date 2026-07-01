"""A reader implementation using the Databricks Spark CSV reader."""

from collections.abc import Iterator
from typing import Any, Optional

import pyspark.sql.functions as psf
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from dve.core_engine.backends.base.reader import read_function
from dve.core_engine.backends.exceptions import EmptyFileError
from dve.core_engine.backends.implementations.spark.spark_helpers import (
    get_type_from_annotation,
    spark_record_index,
    spark_write_parquet,
)
from dve.core_engine.backends.readers.csv import CSVFileReader
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling import get_content_length


@spark_record_index
@spark_write_parquet
class SparkCSVReader(CSVFileReader):
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

        self.multi_line = multi_line
        self.null_empty_strings = null_empty_strings
        self.spark_session = spark_session if spark_session else SparkSession.builder.getOrCreate()  # type: ignore  # pylint: disable=C0301

        super().__init__(
            delimiter=delimiter,
            escape_char=escape_char,
            encoding=encoding,
            quote_char=quote_char,
            header=header,
            field_check=field_check,
            field_check_error_code=field_check_error_code,
            field_check_error_message=field_check_error_message,
        )

    def read_to_py_iterator(
        self,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
        all_model_fields: Optional[set[str]] = None,
    ) -> Iterator[dict[URI, Any]]:
        df = self.read_to_dataframe(resource, entity_name, schema, all_model_fields)
        yield from (record.asDict(True) for record in df.toLocalIterator())

    @read_function(DataFrame)
    def read_to_dataframe(
        self,
        resource: URI,
        entity_name: EntityName,  # pylint: disable=unused-argument
        schema: type[BaseModel],
        all_model_fields: Optional[set[str]] = None,
    ) -> DataFrame:
        """Read a CSV file directly to a Spark DataFrame."""
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource} is empty.")

        if self.field_check:
            self.perform_field_check(resource, entity_name, schema, all_model_fields)

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
