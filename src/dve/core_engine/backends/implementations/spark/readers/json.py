"""A reader implementation using the Databricks Spark XML reader."""


from typing import Any, Dict, Iterator, Optional, Type

from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


from dve.core_engine.backends.base.reader import BaseFileReader, read_function
from dve.core_engine.backends.exceptions import EmptyFileError
from dve.core_engine.backends.implementations.spark.spark_helpers import (
    get_type_from_annotation,
    spark_write_parquet,
)
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling import get_content_length


@spark_write_parquet
class SparkJSONReader(BaseFileReader):
    """A Spark reader for JSON files."""

    def __init__(
        self,
        *,
        encoding: Optional[str] = "utf-8",
        multi_line: Optional[bool] = True,
        spark_session: Optional[SparkSession] = None
    ) -> None:
        
        self.encoding = encoding
        self.multi_line = multi_line
        self.spark_session = spark_session if spark_session else SparkSession.builder.getOrCreate()
        
        super().__init__()

    def read_to_py_iterator(
        self, resource: URI, entity_name: EntityName, schema: Type[BaseModel]
    ) -> Iterator[Dict[URI, Any]]:
        df = self.read_to_dataframe(resource, entity_name, schema)
        yield from (record.asDict(True) for record in df.toLocalIterator())

    @read_function(DataFrame)
    def read_to_dataframe(
        self,
        resource: URI,
        entity_name: EntityName,  # pylint: disable=unused-argument
        schema: Type[BaseModel],
    ) -> DataFrame:
        """Read an JSON file directly to a Spark DataFrame.

        """
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource} is empty.")

        spark_schema: StructType = get_type_from_annotation(schema)
        kwargs = {
            "encoding": self.encoding,
            "multiline": self.multi_line,
            
        }
        
        return (
            self.spark_session.read.format("json")
            .options(**kwargs)  # type: ignore
            .load(resource, schema=spark_schema)
        )

