"""Core Parquet Reader"""

from dve.core_engine.backends.base.reader import BaseFileReader


class BaseParquetReader(BaseFileReader):
    """A base reader for parquet files"""

    def __init__(
        self,
        *,
        hive_partitioning: bool = False,
        field_check_error_code: str = "ParquetFieldMismatch",
        field_check_error_message: str = "The submitted header is invalid",
        **_,
    ):
        """Init function for the base parquet reader"""
        self.hive_partioning = hive_partitioning
        """Infer statistics and schema from Hive partitioned URL and use them to prune reads."""
        self.field_check_error_code = field_check_error_code
        """Error code to raise when fields are missing or unexpected"""
        self.field_check_error_message = field_check_error_message
        """Error message to raise when fields are missing or unexpected"""
