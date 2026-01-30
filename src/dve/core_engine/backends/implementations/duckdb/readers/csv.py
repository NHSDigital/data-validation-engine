"""A csv reader to create duckdb relations"""

# pylint: disable=arguments-differ
from collections.abc import Iterator
from typing import Any, Optional

import duckdb as ddb
import polars as pl
from duckdb import DuckDBPyConnection, DuckDBPyRelation, default_connection, read_csv
from pydantic import BaseModel

from dve.core_engine.backends.base.reader import BaseFileReader, read_function
from dve.core_engine.backends.exceptions import EmptyFileError, MessageBearingError
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    duckdb_write_parquet,
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.types import SQLType
from dve.core_engine.backends.readers.utilities import check_csv_header_expected
from dve.core_engine.backends.utilities import get_polars_type_from_annotation
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling import get_content_length


@duckdb_write_parquet
class DuckDBCSVReader(BaseFileReader):
    """A reader for CSV files including the ability to compare the passed model
    to the file header, if it exists.

    field_check: flag to compare submitted file header to the accompanying pydantic model
    field_check_error_code: The error code to provide if the file header doesn't contain
                            the expected fields
    field_check_error_message: The error message to provide if the file header doesn't contain
                               the expected fields"""

    # TODO - the read_to_relation should include the schema and determine whether to
    # TODO - stringify or not
    def __init__(
        self,
        *,
        header: bool = True,
        delim: str = ",",
        quotechar: str = '"',
        connection: Optional[DuckDBPyConnection] = None,
        field_check: bool = False,
        field_check_error_code: Optional[str] = "ExpectedVsActualFieldMismatch",
        field_check_error_message: Optional[str] = "The submitted header is missing fields",
        **_,
    ):
        self.header = header
        self.delim = delim
        self.quotechar = quotechar
        self._connection = connection if connection else default_connection
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

        if missing := check_csv_header_expected(resource, expected_schema, self.delim):
            raise MessageBearingError(
                "The CSV header doesn't match what is expected",
                messages=[
                    FeedbackMessage(
                        entity=entity_name,
                        record=None,
                        failure_type="submission",
                        error_location="Whole File",
                        error_code=self.field_check_error_code,
                        error_message=f"{self.field_check_error_message} - missing fields: {missing}",  # pylint: disable=line-too-long
                    )
                ],
            )

    def read_to_py_iterator(
        self, resource: URI, entity_name: EntityName, schema: type[BaseModel]
    ) -> Iterator[dict[str, Any]]:
        """Creates an iterable object of rows as dictionaries"""
        yield from self.read_to_relation(resource, entity_name, schema).pl().iter_rows(named=True)

    @read_function(DuckDBPyRelation)
    def read_to_relation(  # pylint: disable=unused-argument
        self, resource: URI, entity_name: EntityName, schema: type[BaseModel]
    ) -> DuckDBPyRelation:
        """Returns a relation object from the source csv"""
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource} is empty.")

        if self.field_check:
            self.perform_field_check(resource, entity_name, schema)

        reader_options: dict[str, Any] = {
            "header": self.header,
            "delimiter": self.delim,
            "quotechar": self.quotechar,
        }

        ddb_schema: dict[str, SQLType] = {
            fld.name: str(get_duckdb_type_from_annotation(fld.annotation))  # type: ignore
            for fld in schema.__fields__.values()
        }

        reader_options["columns"] = ddb_schema
        return read_csv(resource, **reader_options)


class PolarsToDuckDBCSVReader(DuckDBCSVReader):
    """
    Utilises the polars lazy csv reader which is then converted into a DuckDBPyRelation object.

    The primary reason this reader exists is due to the limitation within duckdb csv reader and
    it not being able to read partial content from a csv (i.e. select a, b NOT y).
    """

    @read_function(DuckDBPyRelation)
    def read_to_relation(  # pylint: disable=unused-argument
        self, resource: URI, entity_name: EntityName, schema: type[BaseModel]
    ) -> DuckDBPyRelation:
        """Returns a relation object from the source csv"""
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource} is empty.")

        if self.field_check:
            self.perform_field_check(resource, entity_name, schema)

        reader_options: dict[str, Any] = {
            "has_header": self.header,
            "separator": self.delim,
            "quote_char": self.quotechar,
        }

        polars_types = {
            fld.name: get_polars_type_from_annotation(fld.annotation)  # type: ignore
            for fld in schema.__fields__.values()
        }
        reader_options["dtypes"] = polars_types

        # there is a raise_if_empty arg for 0.18+. Future reference when upgrading. Makes L85
        # redundant
        df = pl.scan_csv(resource, **reader_options).select(list(polars_types.keys()))  # type: ignore  # pylint: disable=W0612

        return ddb.sql("SELECT * FROM df")


class DuckDBCSVRepeatingHeaderReader(PolarsToDuckDBCSVReader):
    """A Reader for files with a `.csv` extension and where there are repeating "header" values
    within the file. Header in this case is not the column names at the top of a csv, rather a
    collection of unique records that would usually be structured in another entity. However, due
    to the fact that `csv` is a semi-structured data format, you cannot define complex entities,
    hence the values are then repeated on all rows.

    Example of a repeating header data may look like this...

    | headerCol1 | headerCol2 | headerCol3 | nonHeaderCol1 | nonHeaderCol2 |
    | ---------- | ---------- | ---------- | ------------- | ------------- |
    | shop 1     | clothes    | 2025-01-01 | jeans         | 20.39         |
    | shop 1     | clothes    | 2025-01-01 | shirt         | 14.99         |

    This reader will just pull out the distinct values from the header column. Where there are
    more/less than one distinct value per column, the reader will produce a
    `NonDistinctHeaderError`.

    So using the example above, the expected entity would look like this...
    | headerCol1 | headerCol2 | headerCol3 |
    | ---------- | ---------- | ---------- |
    | shop1      | clothes    | 2025-01-01 |
    """

    def __init__(
        self,
        *args,
        non_unique_header_error_code: Optional[str] = "NonUniqueHeader",
        non_unique_header_error_message: Optional[str] = None,
        **kwargs,
    ):
        self._non_unique_header_code = non_unique_header_error_code
        self._non_unique_header_message = non_unique_header_error_message
        super().__init__(*args, **kwargs)

    @read_function(DuckDBPyRelation)
    def read_to_relation(  # pylint: disable=unused-argument
        self, resource: URI, entity_name: EntityName, schema: type[BaseModel]
    ) -> DuckDBPyRelation:
        entity = super().read_to_relation(resource=resource, entity_name=entity_name, schema=schema)
        entity = entity.distinct()
        no_records = entity.shape[0]

        if no_records != 1:
            rows = entity.pl().to_dicts()
            differing_values = [
                f"{key}: {', '.join(sorted(str(val) for val in values))}"
                for key, *values in zip(rows[0], *map(dict.values, rows))  # type: ignore
                if len(set(values)) > 1
            ]
            raise MessageBearingError(
                "More than one set of Headers found in CSV file",
                messages=[
                    FeedbackMessage(
                        record={entity_name: differing_values},
                        entity="Pre-validation",
                        failure_type="submission",
                        error_message=(
                            f"Found {no_records} distinct combination of header values."
                            if not self._non_unique_header_message
                            else self._non_unique_header_message
                        ),
                        error_location=entity_name,
                        category="Bad file",
                        error_code=self._non_unique_header_code,
                    )
                ],
            )

        return entity
