"""An implementation of the data contract in Duck DB."""

# pylint: disable=R0903
import logging
from collections.abc import Iterator
from typing import Any, Optional
from uuid import uuid4

import pandas as pd
import polars as pl
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb.typing import DuckDBPyType
from polars.datatypes.classes import DataTypeClass as PolarsType
from pydantic import BaseModel
from pydantic.fields import ModelField

from dve.core_engine.backends.base.contract import BaseDataContract
from dve.core_engine.backends.base.utilities import generate_error_casting_entity_message
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    coerce_inferred_numpy_array_to_list,
    duckdb_read_parquet,
    duckdb_write_parquet,
    get_duckdb_type_from_annotation,
    get_polars_type_from_annotation,
    relation_is_empty,
)
from dve.core_engine.backends.implementations.duckdb.types import DuckDBEntities
from dve.core_engine.backends.metadata.contract import DataContractMetadata
from dve.core_engine.backends.types import StageSuccessful
from dve.core_engine.backends.utilities import stringify_model
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import URI, Messages
from dve.core_engine.validation import RowValidator


class PandasApplyHelper:
    """A helper for using RowValidator object with pandas dataframe"""

    def __init__(self, row_validator: RowValidator):
        self.row_validator = row_validator
        self.errors: list[FeedbackMessage] = []

    def __call__(self, row: pd.Series):
        self.errors.extend(self.row_validator(row.to_dict())[1])  # type: ignore
        return row  # no op


@duckdb_write_parquet
@duckdb_read_parquet
class DuckDBDataContract(BaseDataContract[DuckDBPyRelation]):
    """An implementation of a data contract in DuckDB.

    This utilises the conversion from relation to pandas dataframe to apply the data contract.

    """

    def __init__(
        self,
        connection: DuckDBPyConnection,
        logger: Optional[logging.Logger] = None,
        debug: bool = False,
        **kwargs: Any,
    ):
        self.debug = debug
        self._connection = connection
        """A bool indicating whether to enable debug logging."""

        super().__init__(logger, **kwargs)

    @property
    def connection(self) -> DuckDBPyConnection:
        """The duckdb connection"""
        return self._connection

    def _cache_records(self, relation: DuckDBPyRelation, cache_prefix: URI) -> URI:
        chunk_uri = "/".join((cache_prefix.rstrip("/"), str(uuid4()))) + ".parquet"
        self.write_parquet(entity=relation, target_location=chunk_uri)
        return chunk_uri

    def create_entity_from_py_iterator(  # pylint: disable=unused-argument
        self, entity_name: URI, records: Iterator[dict[URI, Any]], schema: type[BaseModel]
    ) -> DuckDBPyRelation:
        """Create DuckDB Relation from iterator of records"""
        polars_schema: dict[str, PolarsType] = {
            fld.name: get_polars_type_from_annotation(fld.type_)
            for fld in stringify_model(schema).__fields__.values()
        }
        _lazy_df = pl.LazyFrame(records, polars_schema)  # type: ignore # pylint: disable=unused-variable
        return self._connection.sql("select * from _lazy_df")

    @staticmethod
    def generate_ddb_cast_statement(
        column_name: str, dtype: DuckDBPyType, null_flag: bool = False
    ) -> str:
        """Helper method to generate sql statements for casting datatypes (permissively).
        Current duckdb python API doesn't play well with this currently.
        """
        if not null_flag:
            return f"try_cast({column_name} AS {dtype}) AS {column_name}"
        return f"cast(NULL AS {dtype}) AS {column_name}"

    def apply_data_contract(
        self, entities: DuckDBEntities, contract_metadata: DataContractMetadata
    ) -> tuple[DuckDBEntities, Messages, StageSuccessful]:
        """Apply the data contract to the duckdb relations"""
        self.logger.info("Applying data contracts")
        all_messages: Messages = []

        successful = True
        for entity_name, relation in entities.items():
            # get dtypes for all fields -> python data types or use with relation
            entity_fields: dict[str, ModelField] = contract_metadata.schemas[entity_name].__fields__
            ddb_schema: dict[str, DuckDBPyType] = {
                fld.name: get_duckdb_type_from_annotation(fld.annotation)
                for fld in entity_fields.values()
            }
            polars_schema: dict[str, PolarsType] = {
                fld.name: get_polars_type_from_annotation(fld.annotation)
                for fld in entity_fields.values()
            }
            if relation_is_empty(relation):
                self.logger.warning(f"+ Empty relation for {entity_name}")
                empty_df = pl.DataFrame([], schema=polars_schema)  # type: ignore # pylint: disable=W0612
                relation = self._connection.sql("select * from empty_df")
                continue

            self.logger.info(f"+ Applying contract to: {entity_name}")

            row_validator = contract_metadata.validators[entity_name]
            application_helper = PandasApplyHelper(row_validator)
            self.logger.info("+ Applying data contract")
            coerce_inferred_numpy_array_to_list(relation.df()).apply(
                application_helper, axis=1
            )  # pandas uses eager evaluation so potential memory issue here?
            all_messages.extend(application_helper.errors)

            casting_statements = [
                (
                    self.generate_ddb_cast_statement(column, dtype)
                    if column in relation.columns
                    else self.generate_ddb_cast_statement(column, dtype, null_flag=True)
                )
                for column, dtype in ddb_schema.items()
            ]
            try:
                relation = relation.project(", ".join(casting_statements))
            except Exception as err:  # pylint: disable=broad-except
                successful = False
                self.logger.error(f"Error in casting relation: {err}")
                all_messages.append(generate_error_casting_entity_message(entity_name))
                continue

            if self.debug:
                # count will force evaluation - only done in debug
                pre_convert_row_count = relation.count("*").fetchone()[0]  # type: ignore
                self.logger.info(f"+ Converting to parquet: ({pre_convert_row_count} rows)")
            else:
                pre_convert_row_count = 0
                self.logger.info("+ Converting to parquet")

            entities[entity_name] = relation
            if self.debug:
                post_convert_row_count = entities[entity_name].count("*").fetchone()[0]  # type: ignore # pylint:disable=line-too-long
                self.logger.info(f"+ Converted to parquet: ({post_convert_row_count} rows)")
                if post_convert_row_count != pre_convert_row_count:
                    raise ValueError(
                        f"Row count mismatch for {entity_name}"
                        f" ({pre_convert_row_count} vs {post_convert_row_count})"
                    )
            else:
                self.logger.info("+ Converted to parquet")

        return entities, all_messages, successful
