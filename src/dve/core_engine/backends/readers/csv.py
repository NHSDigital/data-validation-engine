# mypy: disable-error-code="attr-defined"
"""Core Python-based CSV reader."""

import csv
from collections.abc import Collection, Iterator
from functools import partial
from typing import IO, Any, Optional

import polars as pl
from pydantic.main import BaseModel

from dve.core_engine.backends.base.reader import BaseFileReader
from dve.core_engine.backends.exceptions import (
    EmptyFileError,
    FieldCountMismatch,
    MissingHeaderError,
)
from dve.core_engine.backends.utilities import get_polars_type_from_annotation, stringify_model
from dve.core_engine.type_hints import EntityName
from dve.parser.file_handling import get_content_length, open_stream
from dve.parser.file_handling.implementations.file import file_uri_to_local_path
from dve.parser.file_handling.service import LocalFilesystemImplementation, _get_implementation
from dve.parser.type_hints import URI


class CSVFileReader(BaseFileReader):
    """A base reader for CSV files."""

    def __init__(
        self,
        *,
        delimiter: str = ",",
        escape_char: str = "\\",
        quote_char: str = '"',
        header: bool = True,
        trim_cells: bool = True,
        null_values: Collection[str] = frozenset({"NULL", "null", ""}),
        encoding: str = "utf-8-sig",
        **_,
    ):
        """Init function for the base CSV reader.

        Args:
         - `delimiter`: the delimiter for the CSV file. This separates the fields.
           Default: `,`
         - `escape_char`: the character used to 'escape' the delimiter in unquoted
           fields. Implementations may also use this character to escape the quote
           character within quoted fields. Default: `\\`
         - `quote_char`: the character used to quote fields. Default: `"`
         - `header`: a boolean value indicating whether to treat the first row of
           the file as a header. Default: `True`
         - `trim_cells`: a boolean value indicating whether to strip whitespace
           from fields. Default: `True`
         - `null_values`: a container of values to replace with null if encountered
           in a cell. Default: `{'', 'null', 'NULL'}`
         - `encoding`: encoding of the CSV file. Default: `utf-8-sig`
         - `**extra_args`: extra, implementation specific parser arguments.

        """
        self.delimiter = delimiter
        """The delimiter for the CSV file. This separates the fields."""
        self.escape_char = escape_char
        """
        The character used to 'escape' the delimiter in unquoted fields. Implementations
        may also use this character to escape the quote character within quoted fields.

        """
        self.quote_char = quote_char
        """The character used to quote fields."""
        self.header = header
        """
        A boolean value indicating whether to treat the first row of the file
        as a header.

        """
        self.trim_cells = trim_cells
        """A boolean value indicating whether to strip whitespace from fields."""
        self.null_values = null_values
        """A container of values to replace with null if encountered in a cell."""
        self.encoding = encoding
        """Encoding of the CSV file."""

    def _get_reader_args(self) -> dict[str, Any]:
        reader_args: dict[str, Any] = {
            "delimiter": self.delimiter,
            "escapechar": self.escape_char,
            "quotechar": self.quote_char,
        }
        return reader_args

    def _parse_n_fields(self, stream: IO[str]) -> int:
        """Peek the first row from a CSV stream and return the number of fields
        in the row.

        NOTE: This seeks the stream back to its original position.

        """
        cursor_position = stream.tell()
        reader = csv.reader(stream, **self._get_reader_args())
        try:
            row = next(reader)
        except StopIteration:  # pragma: no cover
            row = []

        stream.seek(cursor_position)
        return len(row)

    def _parse_field_names(self, stream: IO[str]) -> list[str]:
        """Peek the provided field names from the CSV, returning a list of
        field names as strings.

        NOTE: This does not seek the stream back to its original position,
        since we pass the field names straight to the reader.

        """
        reader = csv.DictReader(stream, **self._get_reader_args())
        # This will almost always be a list of strings, but empty files
        # can lead to null headers.
        parsed_field_names = reader.fieldnames
        if parsed_field_names is None:  # pragma: no cover
            raise MissingHeaderError("Header could not be parsed from CSV file")

        if not all(
            map(lambda string: isinstance(string, str), parsed_field_names)
        ):  # pragma: no cover
            # Should never hit this, poor stdlib typing.
            raise ValueError("Some fieldnames read from CSV are non-string types")

        return list(parsed_field_names)

    def _get_field_names(
        self,
        stream: IO[str],
        field_names: list[str],
    ) -> list[str]:
        """Get field names to be used by the reader."""
        # CSV already expected to have named fields.
        if self.header:
            # Ensure that parsed names are mapped onto the expected field names
            # (e.g. handle case sensitivity).
            # Do this here so that we only do it once per file rather than once
            # per row.
            current_field_names = self._parse_field_names(stream)

            if field_names is None:
                return current_field_names
            expected_names = {name.upper(): name for name in field_names}

            mapped_field_names = []
            for field_name in current_field_names:
                field_name = expected_names.get(field_name.upper(), field_name)
                mapped_field_names.append(field_name)

            return mapped_field_names

        # No header in the file, make up our own.
        n_fields = self._parse_n_fields(stream)
        n_expected = len(field_names)
        if n_fields != n_expected:
            raise FieldCountMismatch(
                "CSV does not have named fields the number of fields parsed does "
                + "not match the schema",
                n_actual_fields=n_fields,
                n_expected_fields=n_expected,
            )
        return field_names

    def _coerce(
        self, row: dict[str, Optional[str]], field_names: list[str]
    ) -> dict[str, Optional[str]]:
        """Coerce a parsed row into the indended shape, nulling values
        which are expected to be parsed as nulls.

        """
        new_row = {}
        for field_name in field_names:
            value = row.get(field_name, None)

            if value is not None:
                if self.trim_cells:
                    value = value.strip()
                if value in self.null_values:
                    value = None

            new_row[field_name] = value
        return new_row

    def read_to_py_iterator(
        self,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> Iterator[dict[str, Any]]:
        """Reads the data to an iterator of dictionaries"""
        if get_content_length(resource) == 0:
            raise EmptyFileError(f"File at {resource!r} is empty")

        field_names = list(schema.__fields__.keys())
        with open_stream(resource, "r", self.encoding) as stream:
            reader = csv.DictReader(
                stream,
                fieldnames=self._get_field_names(stream, field_names),
                **self._get_reader_args(),
            )

            coerce_func = partial(self._coerce, field_names=field_names)
            yield from map(coerce_func, reader)

    def write_parquet(  # type: ignore
        self,
        entity: Iterator[dict[str, Any]],
        target_location: URI,
        schema: Optional[type[BaseModel]] = None,
        **kwargs,
    ) -> EntityName:
        """Writes the data of the given entity to a parquet file"""
        # polars misinterprets local file schemes and creates a file: folder.
        # parse it as Path and uri to resolve
        if isinstance(_get_implementation(target_location), LocalFilesystemImplementation):
            target_location = file_uri_to_local_path(target_location).as_posix()
        if schema:
            polars_schema: dict[str, pl.DataType] = {  # type: ignore
                fld.name: get_polars_type_from_annotation(fld.annotation)
                for fld in stringify_model(schema).__fields__.values()
            }

            pl.LazyFrame(data=entity, schema=polars_schema).sink_parquet(
                path=target_location, compression="snappy"
            )
        else:
            pl.LazyFrame(data=entity).sink_parquet(path=target_location, compression="snappy")
        return target_location
