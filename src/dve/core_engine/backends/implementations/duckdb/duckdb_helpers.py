# pylint: disable=protected-access
# ignore: type[attr-defined]

"""Helper objects for duckdb data contract implementation"""
from dataclasses import is_dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar, Union
from urllib.parse import urlparse

import duckdb.typing as ddbtyp
import numpy as np
import polars as pl  # type: ignore
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb.typing import DuckDBPyType
from pandas import DataFrame
from polars.datatypes.classes import DataTypeClass as PolarsType
from pydantic import BaseModel
from typing_extensions import Annotated, get_args, get_origin, get_type_hints

from dve.core_engine.backends.base.utilities import _get_non_heterogenous_type
from dve.core_engine.type_hints import URI
from dve.parser.file_handling.service import LocalFilesystemImplementation, _get_implementation


class DDBDecimal:
    """DuckDB Decimal type"""

    TYPE_TEXT = "DECIMAL"

    def __init__(self, width: int = 18, scale: int = 3):
        self._width = width
        self._scale = scale

    def __str__(self):
        return DDBDecimal.TYPE_TEXT + f"({self._width},{self._scale})"

    def __call__(self):
        return self.__str__()


class DDBList:
    """DuckDB List type"""

    TYPE_TEXT = "[]"

    def __init__(self, element_type: DuckDBPyType):
        self._element_type = element_type

    def __str__(self):
        return str(self._element_type) + DDBList.TYPE_TEXT

    def __call__(self):
        return self.__str__()


class DDBStruct:
    """DuckDB StructType"""

    TYPE_TEXT = "STRUCT"

    def __init__(self, sub_elements: dict[str, DuckDBPyType]):
        self._sub_elements = {**sub_elements}

    def add_element(self, field_name: str, data_type: DuckDBPyType):
        """Add another element to the struct"""
        self._sub_elements.update({field_name: data_type})

    def __str__(self):
        return (
            DDBStruct.TYPE_TEXT
            + "("
            + ", ".join(f"{fld} {dtype}" for fld, dtype in self._sub_elements.items())
            + ")"
        )

    def __call__(self):
        return self.__str__()


PYTHON_TYPE_TO_DUCKDB_TYPE: dict[type, DuckDBPyType] = {
    str: ddbtyp.VARCHAR,
    int: ddbtyp.BIGINT,
    bool: ddbtyp.BOOLEAN,
    float: ddbtyp.FLOAT,
    bytes: ddbtyp.BLOB,
    date: ddbtyp.DATE,
    datetime: ddbtyp.TIMESTAMP,
    Decimal: DDBDecimal()(),
}
"""A mapping of Python types to the equivalent DuckDB types."""

PYTHON_TYPE_TO_POLARS_TYPE: dict[type, PolarsType] = {
    # issue with decimal conversion at the moment...
    str: pl.Utf8,  # type: ignore
    int: pl.Int64,  # type: ignore
    bool: pl.Boolean,  # type: ignore
    float: pl.Float64,  # type: ignore
    bytes: pl.Binary,  # type: ignore
    date: pl.Date,  # type: ignore
    datetime: pl.Datetime,  # type: ignore
    Decimal: pl.Utf8,  # type: ignore
}
"""A mapping of Python types to the equivalent Polars types."""


def table_exists(connection: DuckDBPyConnection, table_name: str) -> bool:
    """check if a table exists in a given DuckDBPyConnection"""
    return table_name in map(lambda x: x[0], connection.sql("SHOW TABLES").fetchall())


def relation_is_empty(relation: DuckDBPyRelation) -> bool:
    """Check if a duckdb relation is empty"""
    if relation.limit(1).count("*"):
        return False
    return True


def get_duckdb_type_from_annotation(type_annotation: Any) -> DuckDBPyType:
    """Get a duckdb type from a Python type annotation.

    Supported types are any of the following (this definition is recursive):
    - Supported basic Python types. These are:
      * `str`: VARCHAR
      * `int`: BIGINT
      * `bool`: BOOLEAN
      * `float`: FLOAT
      * `bytes`: BLOB
      * `datetime.date`: DATE
      * `datetime.datetime`: TIMESTAMP
      * `decimal.Decimal`: DECIMAL with precision of 18 and scale of 3
    - A list of supported types (e.g. `List[str]` or `typing.List[str]`).
      This will return a duckdb LIST type (variable length)
    - A `typing.Optional` type or a `typing.Union` of the type and `None` (e.g.
      `typing.Optional[str]`, `typing.Union[List[str], None]`). This will remove the
      'optional' wrapper and return the inner type
    - A subclass of `typing.TypedDict` with values typed using supported types. This
      will parse the value types as Polars types and return a duckdb STRUCT.
    - A dataclass or `pydantic.main.ModelMetaClass` with values typed using supported types.
      This will parse the field types as Polars types and return a duckdb STRUCT.
    - Any supported type, with a `typing_extensions.Annotated` wrapper.

    Any `ClassVar` types within `TypedDict`s, dataclasses, or `pydantic` models will be
    ignored.

    """
    type_origin = get_origin(type_annotation)

    # An `Optional` or `Union` type, check to ensure non-heterogenity.
    if type_origin is Union:
        python_type = _get_non_heterogenous_type(get_args(type_annotation))
        return get_duckdb_type_from_annotation(python_type)

    # Type hint is e.g. `List[str]`, check to ensure non-heterogenity.
    if type_origin is list or (isinstance(type_origin, type) and issubclass(type_origin, list)):
        element_type = _get_non_heterogenous_type(get_args(type_annotation))
        return DDBList(get_duckdb_type_from_annotation(element_type))()

    if type_origin is Annotated:
        python_type, *other_args = get_args(type_annotation)  # pylint: disable=unused-variable
        return get_duckdb_type_from_annotation(python_type)
    # Ensure that we have a concrete type at this point.
    if not isinstance(type_annotation, type):
        raise ValueError(f"Unsupported type annotation {type_annotation!r}")

    if (
        # Type hint is a dict subclass, but not dict. Possibly a `TypedDict`.
        (issubclass(type_annotation, dict) and type_annotation is not dict)
        # Type hint is a dataclass.
        or is_dataclass(type_annotation)
        # Type hint is a `pydantic` model.
        or (type_origin is None and issubclass(type_annotation, BaseModel))
    ):
        fields: dict[str, DuckDBPyType] = {}
        for field_name, field_annotation in get_type_hints(type_annotation).items():
            # Technically non-string keys are disallowed, but people are bad.
            if not isinstance(field_name, str):
                raise ValueError(
                    f"Dictionary/Dataclass keys must be strings, got {type_annotation!r}"
                )  # pragma: no cover
            if get_origin(field_annotation) is ClassVar:
                continue

            fields[field_name] = get_duckdb_type_from_annotation(field_annotation)

        if not fields:
            raise ValueError(
                f"No type annotations in dict/dataclass type (got {type_annotation!r})"
            )

        return DDBStruct(fields)()

    if type_annotation is list:
        raise ValueError(
            f"List must have type annotation (e.g. `List[str]`), got {type_annotation!r}"
        )
    if type_annotation is dict or type_origin is dict:
        raise ValueError(f"dict must be `typing.TypedDict` subclass, got {type_annotation!r}")

    for type_ in type_annotation.mro():
        duck_type = PYTHON_TYPE_TO_DUCKDB_TYPE.get(type_)
        if duck_type:
            return duck_type
    raise ValueError(f"No equivalent DuckDB type for {type_annotation!r}")


def get_polars_type_from_annotation(type_annotation: Any) -> PolarsType:
    """Get a polars type from a Python type annotation.

    Supported types  are any of the following (this definition is recursive):
    - Supported basic Python types. These are:
      * `str`: pl.Utf8
      * `int`: pl.Int64
      * `bool`: pl.Boolean
      * `float`: pl.Float64
      * `bytes`: pl.Binary
      * `datetime.date`: pl.Date
      * `datetime.datetime`: pl.Datetime
      * `decimal.Decimal`: pl.Decimal with precision of 38 and scale of 18
    - A list of supported types (e.g. `List[str]` or `typing.List[str]`).
      This will return a pl.List type (variable length)
    - A `typing.Optional` type or a `typing.Union` of the type and `None` (e.g.
      `typing.Optional[str]`, `typing.Union[List[str], None]`). This will remove the
      'optional' wrapper and return the inner type
    - A subclass of `typing.TypedDict` with values typed using supported types. This
      will parse the value types as Polars types and return a Polars Struct.
    - A dataclass or `pydantic.main.ModelMetaClass` with values typed using supported types.
      This will parse the field types as Polars types and return a Polars Struct.
    - Any supported type, with a `typing_extensions.Annotated` wrapper.
    - A `decimal.Decimal` wrapped with `typing_extensions.Annotated` with a `DecimalConfig`
      indicating precision and scale. This will return a Polars Decimal
      with the specfied scale and precision.
    - A `pydantic.types.condecimal` created type.

    Any `ClassVar` types within `TypedDict`s, dataclasses, or `pydantic` models will be
    ignored.

    """
    type_origin = get_origin(type_annotation)

    # An `Optional` or `Union` type, check to ensure non-heterogenity.
    if type_origin is Union:
        python_type = _get_non_heterogenous_type(get_args(type_annotation))
        return get_polars_type_from_annotation(python_type)

    # Type hint is e.g. `List[str]`, check to ensure non-heterogenity.
    if type_origin is list or (isinstance(type_origin, type) and issubclass(type_origin, list)):
        element_type = _get_non_heterogenous_type(get_args(type_annotation))
        return pl.List(get_polars_type_from_annotation(element_type))  # type: ignore

    if type_origin is Annotated:
        python_type, *other_args = get_args(type_annotation)  # pylint: disable=unused-variable
        return get_polars_type_from_annotation(python_type)
    # Ensure that we have a concrete type at this point.
    if not isinstance(type_annotation, type):
        raise ValueError(f"Unsupported type annotation {type_annotation!r}")

    if (
        # Type hint is a dict subclass, but not dict. Possibly a `TypedDict`.
        (issubclass(type_annotation, dict) and type_annotation is not dict)
        # Type hint is a dataclass.
        or is_dataclass(type_annotation)
        # Type hint is a `pydantic` model.
        or (type_origin is None and issubclass(type_annotation, BaseModel))
    ):
        fields: dict[str, PolarsType] = {}
        for field_name, field_annotation in get_type_hints(type_annotation).items():
            # Technically non-string keys are disallowed, but people are bad.
            if not isinstance(field_name, str):
                raise ValueError(
                    f"Dictionary/Dataclass keys must be strings, got {type_annotation!r}"
                )  # pragma: no cover
            if get_origin(field_annotation) is ClassVar:
                continue

            fields[field_name] = get_polars_type_from_annotation(field_annotation)

        if not fields:
            raise ValueError(
                f"No type annotations in dict/dataclass type (got {type_annotation!r})"
            )

        return pl.Struct(fields)  # type: ignore

    if type_annotation is list:
        raise ValueError(
            f"List must have type annotation (e.g. `List[str]`), got {type_annotation!r}"
        )
    if type_annotation is dict or type_origin is dict:
        raise ValueError(f"dict must be `typing.TypedDict` subclass, got {type_annotation!r}")

    for type_ in type_annotation.mro():
        polars_type = PYTHON_TYPE_TO_POLARS_TYPE.get(type_)
        if polars_type:
            return polars_type
    raise ValueError(f"No equivalent DuckDB type for {type_annotation!r}")


def coerce_inferred_numpy_array_to_list(pandas_df: DataFrame) -> DataFrame:
    """Function to modify numpy inferred array when cnverting from duckdb relation to
    pandas dataframe - these cause issues with pydantic models
    (ie. numpy array can't be viewed as a list)

    pandas_df (DataFrame): The dataframe to type check and covert where needed
    """
    pandas_df = pandas_df.replace({np.nan: None})
    for col in pandas_df.columns:
        if isinstance(pandas_df[col].iloc[0], (np.ndarray,)):
            pandas_df[col] = pandas_df[col].apply(lambda x: x.tolist() if x is not None else x)
    return pandas_df


def _ddb_read_parquet(
    self, path: URI, **kwargs  # pylint: disable=unused-argument
) -> DuckDBPyRelation:
    """Read entity from a parquet file. Due to different behaviours in writing parquet
    files to single files/ mutiple files in a directory and duckdb being inflexible
    checks whether a directory supplied (in which case points at parquet files within)
    or a single file, in which case retains supplied path.
    """
    if isinstance(_get_implementation(path), LocalFilesystemImplementation):
        path = urlparse(path).path

    if Path(path).is_dir():
        if not path.endswith("/"):
            path += "/"
        # is a directory - provide edit glob to include all parquet files
        return self._connection.read_parquet(file_glob=f"{path}*.parquet")
    return self._connection.read_parquet(file_glob=path)


def _ddb_write_parquet(  # pylint: disable=unused-argument
    self, entity: DuckDBPyRelation, target_location: URI, **kwargs
) -> URI:
    """Method to write parquet files from type cast entities
    following data contract application
    """
    if isinstance(_get_implementation(target_location), LocalFilesystemImplementation):
        Path(target_location).parent.mkdir(parents=True, exist_ok=True)

    entity.to_parquet(file_name=target_location, compression="snappy", **kwargs)
    return target_location


def duckdb_read_parquet(cls):
    """Class decorator to add read_parquet method for duckdb implementations"""
    cls.read_parquet = _ddb_read_parquet
    return cls


def duckdb_write_parquet(cls):
    """Class decorator to add write_parquet method for duckdb implementations"""
    cls.write_parquet = _ddb_write_parquet
    return cls


@staticmethod  # type: ignore
def _duckdb_get_entity_count(entity: DuckDBPyRelation) -> int:
    """Method to obtain entity count from a persisted parquet entity"""
    return entity.shape[0]


def duckdb_get_entity_count(cls):
    """Class decorator to count records in an entity supplied"""
    cls.get_entity_count = _duckdb_get_entity_count
    return cls


def get_all_registered_udfs(connection: DuckDBPyConnection) -> set[str]:
    """Function to supply the names of a registered functions stored in the supplied
    duckdb connection. Creates the temp table used to store registered functions (if not exists).
    """
    connection.sql("CREATE TEMP TABLE IF NOT EXISTS dve_udfs (function_name VARCHAR)")
    return {rw[0] for rw in connection.sql("SELECT * FROM dve_udfs").fetchall()}
