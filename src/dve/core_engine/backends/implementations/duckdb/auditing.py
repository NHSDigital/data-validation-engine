"""Auditing definitions for duckdb backend"""
from typing import Any, Dict, Iterable, List, Optional, Type, Union

import polars as pl
from duckdb import ColumnExpression, DuckDBPyConnection, DuckDBPyRelation, StarExpression, connect
from polars.datatypes.classes import DataTypeClass as PolarsType

from dve.core_engine.backends.base.auditing import (
    BaseAuditingManager,
    BaseAuditor,
    FilterCriteria,
    OrderCriteria,
)
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    PYTHON_TYPE_TO_DUCKDB_TYPE,
    table_exists,
)
from dve.core_engine.backends.utilities import PYTHON_TYPE_TO_POLARS_TYPE
from dve.core_engine.models import (
    AuditRecord,
    ProcessingStatusRecord,
    SubmissionInfo,
    SubmissionStatisticsRecord,
    TransferRecord,
)
from dve.core_engine.type_hints import URI, ExecutorType


class DDBAuditor(BaseAuditor[DuckDBPyRelation]):
    """An auditor implemented using the python duckdb package"""

    def __init__(
        self,
        record_type: Type[AuditRecord],
        database_uri: URI,
        name: str,
        connection: Optional[DuckDBPyConnection] = None,
    ):

        self._db = database_uri
        self._connection: DuckDBPyConnection = (
            connection
            if connection
            else connect(
                database=database_uri,
                config={
                    "access_mode": "READ_WRITE",
                    "default_null_order": "NULLS_LAST",
                    "threads": 1,
                },
            )
        )
        super().__init__(name=name, record_type=record_type)
        if not table_exists(self._connection, self._name):

            self._connection.sql(self.ddb_create_table_sql)

    @property
    def ddb_create_table_sql(self) -> str:
        """Generate create table sql script for auditor"""
        _sql_expression = f"CREATE TABLE {self._name} ("
        _sql_expression += ", ".join(
            [f"{fld} {PYTHON_TYPE_TO_DUCKDB_TYPE.get(dtype)}" for fld, dtype in self.schema.items()]
        )
        _sql_expression += ")"
        return _sql_expression

    @property
    def polars_schema(self) -> Dict[str, PolarsType]:
        """Get polars dataframe schema for auditor"""
        return {
            fld: PYTHON_TYPE_TO_POLARS_TYPE.get(dtype, pl.Utf8)  # type: ignore
            for fld, dtype in self.schema.items()
        }

    def get_relation(self) -> DuckDBPyRelation:
        """Get a relation to interact with the auditor duckdb table"""
        return self._connection.table(self._name)

    def combine_filters(self, filter_criteria: List[FilterCriteria]) -> str:
        """Combine multiple filters to apply"""
        return " AND ".join([self.normalise_filter(filt) for filt in filter_criteria])

    @staticmethod
    def normalise_field(field: str) -> ColumnExpression:  # type: ignore
        """Convert field to duckdb expression"""
        return ColumnExpression(field)

    @staticmethod
    def normalise_order(order_condition: OrderCriteria) -> str:
        """Convert order criteria to duckdb expression"""
        return order_condition.to_sql()

    @staticmethod
    def normalise_filter(filter_condition: FilterCriteria) -> str:
        """Convert filter criteria to duckdb expression"""
        return filter_condition.to_sql()

    def conv_to_records(self, recs: DuckDBPyRelation) -> Iterable[AuditRecord]:
        """Convert the relation to an iterable of the related audit record"""
        return (self._record_type(**rec) for rec in recs.pl().iter_rows(named=True))

    def conv_to_entity(self, recs: List[AuditRecord]) -> DuckDBPyRelation:
        """Convert a list of audit records to a relation"""
        # pylint: disable=W0612
        rec_df = pl.DataFrame(  # type: ignore
            [rec.dict() for rec in recs],
            schema=self.polars_schema,
        )
        return self._connection.sql("select * from rec_df")

    def add_records(self, records: Iterable[Dict[str, Any]]) -> None:
        """Add records to the underlying duckdb table"""
        # pylint: disable=W0612
        data_pl_df = pl.DataFrame(  # type: ignore
            records,
            schema=self.polars_schema,
        )

        self._connection.sql(
            f"""INSERT INTO {self._name} ({', '.join(self.polars_schema)})
                                 SELECT {', '.join(self.polars_schema)} from data_pl_df"""
        )

    def retrieve_records(
        self,
        filter_criteria: Optional[List[FilterCriteria]] = None,
        data: Optional[DuckDBPyRelation] = None,
    ) -> DuckDBPyRelation:
        """Get records from the underlying duckdb table"""
        rel = self.get_relation() if not data else data
        if filter_criteria:
            rel = rel.filter(self.combine_filters(filter_criteria))
        return rel

    def get_most_recent_records(
        self,
        order_criteria: List[OrderCriteria],
        partition_fields: Optional[List[str]] = None,
        pre_filter_criteria: Optional[List[FilterCriteria]] = None,
    ) -> DuckDBPyRelation:
        """Get most recent records, based on the order and partitioning,
        from the underlying duckdb table"""
        ordering = " AND ".join([self.normalise_order(fld) for fld in order_criteria])
        rel = self.get_relation()
        if pre_filter_criteria:
            rel = rel.filter(self.combine_filters(pre_filter_criteria))
        if partition_fields:
            rel = (
                rel.select(
                    "*, row_number() OVER (PARTITION BY {} ORDER BY {}) as RN".format(  # pylint: disable=C0209
                        ",".join(partition_fields),
                        ",".join([self.normalise_order(ordr) for ordr in order_criteria]),
                    )
                )
                .filter("RN = 1")
                .select(StarExpression(exclude=["RN"]))
            )
        else:
            rel = rel.order(ordering).limit(1)
        return rel


class DDBAuditingManager(BaseAuditingManager[DDBAuditor, DuckDBPyRelation]):
    """Auditing manager for duckdb implementaion"""

    def __init__(
        self,
        database_uri: URI,
        pool: Optional[ExecutorType] = None,
        connection: Optional[DuckDBPyRelation] = None,
    ):
        self._database_uri = database_uri
        self._connection = (
            connection
            if connection
            else connect(
                database=database_uri,
                config={
                    "access_mode": "READ_WRITE",
                    "default_null_order": "NULLS_LAST",
                    "threads": 1,
                },
            )
        )
        self._pool = pool
        super().__init__(
            processing_status=DDBAuditor(
                record_type=ProcessingStatusRecord,
                database_uri=self._database_uri,
                name="processing_status",
                connection=self._connection,  # type: ignore
            ),
            submission_info=DDBAuditor(
                record_type=SubmissionInfo,
                database_uri=self._database_uri,
                name="submission_info",
                connection=self._connection,  # type: ignore
            ),
            submission_statistics=DDBAuditor(
                record_type=SubmissionStatisticsRecord,
                database_uri=self._database_uri,
                name="submission_statistics",
                connection=self._connection,  # type: ignore
            ),
            transfers=DDBAuditor(
                record_type=TransferRecord,
                database_uri=self._database_uri,
                name="transfers",
                connection=self._connection,  # type: ignore
            ),
            pool=self._pool,
        )

    def combine_auditor_information(
        self, left: Union[DDBAuditor, DuckDBPyRelation], right: Union[DDBAuditor, DuckDBPyRelation]
    ) -> DuckDBPyRelation:
        if isinstance(left, DDBAuditor):
            left = left.get_relation()
        left = left.set_alias("lhs")
        if isinstance(right, DDBAuditor):
            right = right.get_relation()
        right = right.set_alias("rhs")
        return left.join(right, condition="submission_id", how="inner").select(
            *[f"lhs.{fld}" for fld in left.columns],
            *[f"rhs.{fld}" for fld in right.columns if not fld in left.columns],
        )

    @staticmethod
    def conv_to_iterable(recs: Union[DDBAuditor, DuckDBPyRelation]) -> Iterable[Dict[str, Any]]:
        recs_rel: DuckDBPyRelation = recs.get_relation() if isinstance(recs, DDBAuditor) else recs
        return recs_rel.pl().iter_rows(named=True)
