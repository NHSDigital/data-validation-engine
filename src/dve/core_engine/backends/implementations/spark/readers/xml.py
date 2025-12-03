"""A reader implementation using the Databricks Spark XML reader."""

import re
from collections.abc import Collection, Iterable, Iterator
from typing import Any, Optional

from pydantic import BaseModel
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.column import Column
from pyspark.sql.types import StringType, StructField, StructType
from typing_extensions import Literal

from dve.core_engine.backends.base.reader import read_function
from dve.core_engine.backends.exceptions import EmptyFileError, MessageBearingError
from dve.core_engine.backends.implementations.spark.spark_helpers import (
    df_is_empty,
    get_type_from_annotation,
    spark_write_parquet,
)
from dve.core_engine.backends.readers.xml import BasicXMLFileReader, XMLStreamReader
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling import get_content_length
from dve.parser.file_handling.service import open_stream

SparkXMLMode = Literal["PERMISSIVE", "FAILFAST", "DROPMALFORMED"]
"""The mode to use when parsing XML files with Spark."""


@spark_write_parquet
class SparkXMLStreamReader(XMLStreamReader):
    """An XML stream reader that adds a method to read to a dataframe"""

    spark = None

    @read_function(DataFrame)
    def read_to_dataframe(
        self,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> DataFrame:
        """Stream an XML file into a Spark data frame"""
        if not self.spark:
            self.spark = SparkSession.builder.getOrCreate()  # type: ignore
        spark_schema = get_type_from_annotation(schema)
        return self.spark.createDataFrame(  # type: ignore
            list(self.read_to_py_iterator(resource, entity_name, schema)),
            schema=spark_schema,
        )


@spark_write_parquet
class SparkXMLReader(BasicXMLFileReader):  # pylint: disable=too-many-instance-attributes
    """A reader for XML files built atop Spark-XML."""

    def __init__(
        self,
        *,
        record_tag: str,
        root_tag: Optional[str] = None,
        spark_session: Optional[SparkSession] = None,
        sampling_ratio: int = 1,
        exclude_attribute: bool = True,
        mode: SparkXMLMode = "PERMISSIVE",
        infer_schema: bool = False,
        ignore_namespace: bool = True,
        null_values: Collection[str] = frozenset(("NULL", "null", "")),
        sanitise_multiline: bool = True,
        namespace=None,
        trim_cells=True,
        xsd_location: Optional[URI] = None,
        xsd_error_code: Optional[str] = None,
        xsd_error_message: Optional[str] = None,
        rules_location: Optional[URI] = None,
        **_,
    ) -> None:

        super().__init__(
            record_tag=record_tag,
            root_tag=root_tag,
            trim_cells=trim_cells,
            null_values=null_values,
            sanitise_multiline=sanitise_multiline,
            xsd_location=xsd_location,
            xsd_error_code=xsd_error_code,
            xsd_error_message=xsd_error_message,
            rules_location=rules_location,
        )

        self.spark_session = spark_session or SparkSession.builder.getOrCreate()  # type: ignore
        self.sampling_ratio = sampling_ratio
        self.exclude_attribute = exclude_attribute
        self.mode = mode
        self.infer_schema = infer_schema
        self.ignore_namespace = ignore_namespace
        self.sanitise_multiline = sanitise_multiline
        self.namespace = namespace

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
        """Read an XML file directly to a Spark DataFrame using the Databricks
        XML reader package.

        """
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource} is empty.")

        if self.xsd_location:
            msg = self._run_xmllint(file_uri=resource)
            if msg:
                raise MessageBearingError(
                    "Submitted file failed XSD validation.",
                    messages=[msg],
                )

        spark_schema: StructType = get_type_from_annotation(schema)
        kwargs = {
            "rowTag": self.record_tag,
            "samplingRatio": self.sampling_ratio,
            "excludeAttribute": self.exclude_attribute,
            "mode": self.mode,
            "inferSchema": self.infer_schema,
            "ignoreNamespace": self.ignore_namespace,
            "nullValue": None,
            "treatEmptyValuesAsNulls": True,
            "ignoreSurroundingSpaces": self.trim_cells,
            "withIgnoreSurroundingSpaces": self.trim_cells,
        }
        read_schema: Optional[StructType] = spark_schema
        if self.root_tag:
            kwargs["rowTag"] = self.root_tag
            # Need to let Spark infer the schema and then check if it
            # fits.
            read_schema = None
        if self.namespace:
            kwargs["rowTag"] = f"{self.namespace}:{kwargs['rowTag']}"

        try:
            df = (
                self.spark_session.read.format("xml")
                .options(**kwargs)  # type: ignore
                .load(resource, schema=read_schema)
            )
            if df_is_empty(df):
                with open_stream(resource, "r") as stream:
                    head = stream.read(1000)
                results = re.search(rf"<(\w+):{self.record_tag}", head)
                if results:
                    namespace = results.groups()[0]
                    kwargs["rowTag"] = f"{namespace}:{self.record_tag}"
                    df = (
                        self.spark_session.read.format("xml")
                        .options(**kwargs)  # type: ignore
                        .load(resource, schema=read_schema)
                    )
            if self.root_tag and df.columns:
                try:
                    df = df.select(sf.explode(self.record_tag)).select("col.*")
                except AnalysisException:
                    df = df.select(f"{self.record_tag}.*")

        except Exception as exc:
            raise ValueError(f"Failed to read XML file at {resource}") from exc

        df = self._add_missing_columns(df, spark_schema)
        df = self._sanitise_columns(df)
        return df

    def _add_missing_columns(self, df: DataFrame, fields: Iterable[StructField]) -> DataFrame:
        for field in fields:
            if field.name not in df.columns:
                df = df.withColumn(field.name, sf.lit(None).cast(field.dataType))
        return df

    def _sanitise_columns(self, df: DataFrame) -> DataFrame:
        for col in df.schema:
            name = col.name
            if col.dataType == StringType():
                for value in self.null_values:
                    df = df.withColumn(name, self._replace(sf.col(name), value))
            if self.sanitise_multiline and col.dataType == StringType():
                df = df.withColumn(name, sf.regexp_replace(sf.col(name), "\\s*\n\\s*", " "))
            if self.trim_cells and col.dataType == StringType():
                df = df.withColumn(name, sf.trim(sf.col(name)))
        return df

    @staticmethod
    def _replace(column: Column, value: str) -> Column:
        return sf.when(column != sf.lit(value), column).otherwise(sf.lit(None))
