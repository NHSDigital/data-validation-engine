"""Error report generation"""

import datetime as dt
import json
from functools import partial
from multiprocessing import Pool, cpu_count
from typing import Deque, Dict, List, Tuple, Union

import polars as pl
from polars import DataFrame, LazyFrame, Utf8, col, count  # type: ignore

from dve.core_engine.message import FeedbackMessage
from dve.parser.file_handling.service import open_stream

ERROR_SCHEMA = {
    "Table": Utf8(),
    "Type": Utf8(),
    "Error_Code": Utf8(),
    "Data_Item": Utf8(),
    "Error": Utf8(),
    "Value": Utf8(),
    "ID": Utf8(),
    "Category": Utf8(),
}
"""Schema for the polars error dataframe"""
AGGREGATE_SCHEMA = {
    "Type": Utf8(),
    "Table": Utf8(),
    "Data_Item": Utf8(),
    "Category": Utf8(),
    "Error_Code": Utf8(),
    "Count": pl.UInt32(),  # type: ignore
}
"""Schema for the polars aggregates dataframe"""


def get_error_codes(error_code_path: str) -> LazyFrame:
    """Returns an error code dataframe from a json file on any supported filesystem"""
    with open_stream(error_code_path) as stream:
        error_codes = json.load(stream)
    df_lists: Dict[str, List[str]] = {"Category": [], "Data_Item": [], "Error_Code": []}
    for field, code in error_codes.items():
        for category in ("Blank", "Wrong format", "Bad value"):
            df_lists["Category"].append(category)
            df_lists["Data_Item"].append(field)
            df_lists["Error_Code"].append(code)

    return pl.DataFrame(df_lists).lazy()  # type: ignore


def conditional_cast(value, primary_keys: List[str], value_separator: str) -> Union[List[str], str]:
    """Determines what to do with a value coming back from the error list"""
    if isinstance(value, list):
        casts = [
            conditional_cast(val, primary_keys, value_separator) for val in value
        ]  # type: ignore
        return value_separator.join(
            [f"{pk}: {id}" if pk else "" for pk, id in zip(primary_keys, casts)]
        )
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return ""
    return str(value)


def _convert_inner_dict(error: FeedbackMessage, key_fields):
    return {
        key: (
            str(conditional_cast(value, key_fields.get(error.entity, ""), " -- "))
            if value is not None
            else None
        )
        for key, value in error.to_dict(
            key_fields.get(error.entity),
            max_number_of_values=10,
            value_separator=" -- ",
            record_converter=None,
        ).items()
    }


def create_error_dataframe(errors: Deque[FeedbackMessage], key_fields):
    """Creates a Lazyframe from a Deque of feedback messages and their key fields"""
    if not errors:
        return DataFrame({}, schema=ERROR_SCHEMA)

    first = errors[0].to_dict(key_field=key_fields.get(errors[0].entity))
    schema = [(column, pl.Utf8()) for column in first.keys()]  # type: ignore

    with Pool(cpu_count() - 1) as pool:
        df = pl.LazyFrame(  # type: ignore
            pool.imap_unordered(
                partial(_convert_inner_dict, key_fields=key_fields),
                errors,
                chunksize=5_000,
            ),
            schema=schema,
        )

    df = df.with_columns(
        pl.when(pl.col("Status") == pl.lit("error"))
        .then(pl.lit("Submission Failure"))
        .otherwise(pl.lit("Warning"))
        .alias("error_type")
    )
    df = df.select(
        col("Entity").alias("Table"),
        col("error_type").alias("Type"),
        col("ErrorCode").alias("Error_Code"),
        col("ReportingField").alias("Data_Item"),
        col("ErrorMessage").alias("Error"),
        col("Value"),
        col("Key").alias("ID"),
        col("Category"),
    )
    return df.sort("Type", descending=False).collect()  # type: ignore


def populate_error_codes(df: DataFrame, error_codes: LazyFrame) -> DataFrame:
    """Populates the data contract error codes for the error report"""
    df = df.join(error_codes.collect(), on=["Data_Item", "Category"], how="left", suffix="_0")

    return df.with_columns(
        pl.coalesce("Error_Code", "Error_Code_0").alias("Error_Code")  # type: ignore
    ).drop("Error_Code_0")


def calculate_aggregates(error_frame: DataFrame) -> DataFrame:
    """Calculates the aggregates for the error report"""
    if error_frame.is_empty():
        return DataFrame({}, schema=AGGREGATE_SCHEMA)
    aggregates = (
        error_frame.group_by(
            [
                pl.col("Table"),
                pl.col("Type"),
                pl.col("Data_Item"),
                pl.col("Error_Code"),
                pl.col("Category"),
            ]
        )
        .agg(pl.len())
        .select(  # type: ignore
            pl.col("Type"),
            pl.col("Table"),
            pl.col("Data_Item"),
            pl.col("Category"),
            pl.col("Error_Code"),
            pl.col("len").alias("Count"),
        )
        .sort(pl.col("Type"), pl.col("Count"), descending=[False, True])
    )
    return aggregates


def generate_report_dataframes(
    errors,
    contract_error_codes,
    key_fields,
    populate_codes: bool = True,
) -> Tuple[pl.DataFrame, pl.DataFrame]:  # type: ignore
    """Generates the error detail and aggregates dataframes"""
    error_df = create_error_dataframe(errors, key_fields)

    if populate_codes and not error_df.is_empty():
        error_df = populate_error_codes(error_df, contract_error_codes)
    aggregates = calculate_aggregates(error_df)
    return error_df, aggregates
