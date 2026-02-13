"""An implementation of the data contract in Duck DB."""

# pylint: disable=R0903
import logging
from collections.abc import Iterator
from functools import partial
from typing import Any, Optional
from uuid import uuid4

import pandas as pd
import polars as pl
import pyarrow.parquet as pq  # type: ignore
from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb.typing import DuckDBPyType
from polars.datatypes.classes import DataTypeClass as PolarsType
from pydantic import BaseModel
from pydantic.fields import ModelField

import dve.parser.file_handling as fh
from dve.common.error_utils import (
    BackgroundMessageWriter,
    dump_processing_errors,
    get_feedback_errors_uri,
)
from dve.core_engine.backends.base.contract import BaseDataContract
from dve.core_engine.backends.base.utilities import (
    check_if_parquet_file,
    generate_error_casting_entity_message,
)
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    duckdb_read_parquet,
    duckdb_write_parquet,
    get_duckdb_type_from_annotation,
    relation_is_empty,
)
from dve.core_engine.backends.implementations.duckdb.types import DuckDBEntities
from dve.core_engine.backends.metadata.contract import DataContractMetadata
from dve.core_engine.backends.types import StageSuccessful
from dve.core_engine.backends.utilities import get_polars_type_from_annotation, stringify_model
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import URI, EntityLocations
from dve.core_engine.validation import RowValidator, apply_row_validator_helper


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

    This utilises pyarrow to distibute parquet data across python processes and
    a background process to write error messages.

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

    def _cache_records(self, relation: DuckDBPyRelation, working_dir: URI) -> URI:
        chunk_uri = "/".join((working_dir.rstrip("/"), str(uuid4()))) + ".parquet"
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
            return f'try_cast("{column_name}" AS {dtype}) AS "{column_name}"'
        return f'cast(NULL AS {dtype}) AS "{column_name}"'

    # pylint: disable=R0914
    def apply_data_contract(
        self,
        working_dir: URI,
        entities: DuckDBEntities,
        entity_locations: EntityLocations,
        contract_metadata: DataContractMetadata,
        key_fields: Optional[dict[str, list[str]]] = None,
    ) -> tuple[DuckDBEntities, URI, StageSuccessful]:
        """Apply the data contract to the duckdb relations"""
        self.logger.info("Applying data contracts")
        feedback_errors_uri: URI = get_feedback_errors_uri(working_dir, "data_contract")

        # check if entities are valid parquet - if not, convert
        for entity, entity_loc in entity_locations.items():
            if not check_if_parquet_file(entity_loc):
                parquet_uri = self.write_parquet(
                    entities[entity], fh.joinuri(fh.get_parent(entity_loc), f"{entity}.parquet")
                )
                entity_locations[entity] = parquet_uri

        successful = True

        with BackgroundMessageWriter(
            working_dir, "data_contract", key_fields=key_fields
        ) as msg_writer:
            for entity_name, relation in entities.items():
                # get dtypes for all fields -> python data types or use with relation
                entity_fields: dict[str, ModelField] = contract_metadata.schemas[
                    entity_name
                ].__fields__
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

                row_validator_helper = partial(
                    apply_row_validator_helper,
                    row_validator=contract_metadata.validators[entity_name],
                )

                batches = pq.ParquetFile(entity_locations[entity_name]).iter_batches(10000)
                msg_count = 0
                for batch in batches:
                    if msgs := row_validator_helper(arrow_batch=batch):
                        msg_writer.write_queue.put(msgs)
                        msg_count += len(msgs)

                self.logger.info(f"Data contract found {msg_count} issues in {entity_name}")

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
                    dump_processing_errors(
                        working_dir,
                        "data_contract",
                        [generate_error_casting_entity_message(entity_name)],
                    )
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

        return entities, feedback_errors_uri, successful
