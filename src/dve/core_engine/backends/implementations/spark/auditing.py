"""Auditing definitions for spark backend"""
import operator
from functools import reduce
from typing import Any, Dict, Iterable, List, Optional, Type, Union

from pyspark.sql import Column, DataFrame, DataFrameWriter, SparkSession
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import StructField, StructType
from pyspark.sql.window import Window
from typing_extensions import Literal

from dve.core_engine.backends.base.auditing import (
    BaseAuditingManager,
    BaseAuditor,
    FilterCriteria,
    OrderCriteria,
)
from dve.core_engine.backends.implementations.spark.spark_helpers import get_type_from_annotation
from dve.core_engine.backends.implementations.spark.utilities import database_exists, table_exists
from dve.core_engine.models import (
    AuditRecord,
    ProcessingStatusRecord,
    SubmissionInfo,
    SubmissionStatisticsRecord,
    TransferRecord,
)
from dve.core_engine.type_hints import ExecutorType

SparkTableFormat = Literal["delta", "parquet"]

AUDIT_PARTITION_COLS: Dict[str, List[str]] = {
    "submission_info": ["date_updated"],
    "transfers": ["date_updated"],
    "processing_status": ["date_updated"],
    "submission_statistics": ["date_updated"],
}


class SparkAuditor(BaseAuditor[DataFrame]):
    """An auditor using underlying spark tables"""

    def __init__(
        self,
        record_type: Type[AuditRecord],
        database: str,
        name: str,
        table_format: Optional[SparkTableFormat] = "delta",
        spark: Optional[SparkSession] = None,
    ):
        self._db = database
        self._table_format = table_format
        self._partition_cols: List[str] = AUDIT_PARTITION_COLS.get(name, [])
        self._spark = spark if spark else SparkSession.builder.getOrCreate()
        super().__init__(name=name, record_type=record_type)
        if not table_exists(self._spark, f"{self._db}.{self._name}"):
            self._spark.createDataFrame([], self.spark_schema).write.saveAsTable(
                name=f"{self._db}.{self._name}",
                format=self._table_format,
                partitionBy=self._partition_cols,
            )

    @property
    def fq_name(self) -> str:
        """The fully qualified audit table name"""
        return f"{self._db}.{self._name}"

    @property
    def spark_schema(self) -> StructType:
        """The spark schema of the audit table"""
        return StructType(
            [
                StructField(fld, get_type_from_annotation(dtype))
                for fld, dtype in self.schema.items()
            ]
        )

    def get_df(self) -> DataFrame:
        """Retrieve a spark dataframe reference to the underlying audit table"""
        if self._table_format == "parquet":
            self._spark.catalog.refreshTable(self.fq_name)
        return self._spark.table(self.fq_name)

    def combine_filters(self, filter_criteria: List[FilterCriteria]) -> Column:
        """Combine multiple filters to apply"""
        return reduce(lambda x, y: x & y, [self.normalise_filter(filt) for filt in filter_criteria])

    @staticmethod
    def normalise_field(field: str) -> Column:
        """Convert field to spark column expression"""
        return col(field)

    @staticmethod
    def normalise_order(order_condition: OrderCriteria) -> Column:
        """Convert order criteria to spark column expression"""
        return (
            col(order_condition.field_name).desc()
            if order_condition.descending
            else col(order_condition.field_name)
        )

    @staticmethod
    def normalise_filter(filter_condition: FilterCriteria) -> Column:
        """Convert filter criteria to spark column expression"""
        if filter_condition.operator_ is operator.contains:
            return col(filter_condition.field).isin(filter_condition.comparison_value)
        return filter_condition.operator_(
            col(filter_condition.field), lit(filter_condition.comparison_value)
        )

    def conv_to_records(self, recs: DataFrame) -> Iterable[AuditRecord]:
        """Convert the dataframe to an iterable of the related audit record"""
        return (self._record_type(**rec.asDict()) for rec in recs.toLocalIterator())

    def conv_to_entity(self, recs: List[AuditRecord]) -> DataFrame:
        """Convert the dataframe to an iterable of the related audit record"""
        return self._spark.createDataFrame(  # type: ignore
            [rec.dict() for rec in recs], schema=self.spark_schema
        )

    def add_records(self, records: Iterable[Dict[str, Any]]):
        _df_writer: DataFrameWriter = (
            self._spark.createDataFrame(records, schema=self.spark_schema)  # type: ignore
            .coalesce(1)
            .write.format(self._table_format)  # type: ignore
            .mode("append")
        )
        if self._partition_cols:
            _df_writer = _df_writer.partitionBy(*self._partition_cols)
        _df_writer.saveAsTable(self.fq_name)

    def retrieve_records(
        self,
        filter_criteria: Optional[List[FilterCriteria]] = None,
        data: Optional[DataFrame] = None,
    ) -> DataFrame:
        df = self.get_df() if not data else data
        if filter_criteria:
            df = df.filter(self.combine_filters(filter_criteria))
        return df

    def get_most_recent_records(
        self,
        order_criteria: List[OrderCriteria],
        partition_fields: Optional[List[str]] = None,
        pre_filter_criteria: Optional[List[FilterCriteria]] = None,
    ) -> DataFrame:
        ordering = [self.normalise_order(fld) for fld in order_criteria]
        df = self.get_df()
        if pre_filter_criteria:
            df = df.filter(self.combine_filters(pre_filter_criteria))
        if partition_fields:
            window = Window.partitionBy(
                [self.normalise_field(fld) for fld in partition_fields]
            ).orderBy(
                ordering  # type: ignore
            )
            df = df.withColumn("RN", row_number().over(window)).filter("RN = 1").drop("RN")
        else:
            df = df.orderBy(ordering).limit(1)  # type: ignore

        return df


class SparkAuditingManager(BaseAuditingManager[SparkAuditor, DataFrame]):
    """Auditing manager for spark implementaion"""

    def __init__(
        self,
        database: str,
        pool: Optional[ExecutorType] = None,
        spark: Optional[SparkSession] = None,
        table_format: Optional[SparkTableFormat] = "delta",
    ):
        self._database = database
        self._spark = spark if spark else SparkSession.builder.getOrCreate()
        if not database_exists(self._spark, self._database):
            self._spark.sql(f"CREATE DATABASE {self._database}")
        self._table_format = table_format
        self._pool = pool
        super().__init__(
            processing_status=SparkAuditor(
                record_type=ProcessingStatusRecord,
                database=self._database,
                name="processing_status",
                table_format=self._table_format,
                spark=self._spark,
            ),
            submission_info=SparkAuditor(
                record_type=SubmissionInfo,
                database=self._database,
                name="submission_info",
                table_format=self._table_format,
                spark=self._spark,
            ),
            submission_statistics=SparkAuditor(
                record_type=SubmissionStatisticsRecord,
                database=self._database,
                name="submission_statistics",
                table_format=self._table_format,
                spark=self._spark,
            ),
            transfers=SparkAuditor(
                record_type=TransferRecord,
                database=self._database,
                name="transfers",
                table_format=self._table_format,
                spark=self._spark,
            ),
            pool=self._pool,
        )

    def combine_auditor_information(
        self, left: Union[SparkAuditor, DataFrame], right: Union[SparkAuditor, DataFrame]
    ) -> DataFrame:
        if isinstance(left, SparkAuditor):
            left = left.get_df()
        left = left.alias("lhs")
        if isinstance(right, SparkAuditor):
            right = right.get_df()
        right = right.alias("rhs")
        return left.join(right, on="submission_id", how="inner").select(
            "lhs.*", *[f"rhs.{fld}" for fld in right.columns if not fld in left.columns]
        )

    @staticmethod
    def conv_to_iterable(recs: Union[SparkAuditor, DataFrame]) -> Iterable[Dict[str, Any]]:
        recs_df: DataFrame = recs.get_df() if isinstance(recs, SparkAuditor) else recs
        return iter([rw.asDict() for rw in recs_df.toLocalIterator()])
