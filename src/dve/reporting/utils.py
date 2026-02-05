"""Utilities to support reporting"""

import json
from typing import Optional

import polars as pl

import dve.parser.file_handling as fh
from dve.core_engine.backends.utilities import pl_row_count
from dve.core_engine.exceptions import CriticalProcessingError
from dve.core_engine.type_hints import URI, Messages
from dve.reporting.error_report import conditional_cast


def dump_feedback_errors(
    working_folder: URI,
    step_name: str,
    messages: Messages,
    key_fields: Optional[dict[str, list[str]]] = None,
):
    """Write out captured feedback error messages."""
    if not working_folder:
        raise AttributeError("processed files path not passed")

    if not key_fields:
        key_fields = {}

    errors = fh.joinuri(working_folder, "errors", f"{step_name}_errors.json")
    processed = []

    for message in messages:
        if message.original_entity is not None:
            primary_keys = key_fields.get(message.original_entity, [])
        elif message.entity is not None:
            primary_keys = key_fields.get(message.entity, [])
        else:
            primary_keys = []

        error = message.to_dict(
            key_field=primary_keys,
            value_separator=" -- ",
            max_number_of_values=10,
            record_converter=None,
        )
        error["Key"] = conditional_cast(error["Key"], primary_keys, value_separator=" -- ")
        processed.append(error)

    with fh.open_stream(errors, "a") as f:
        json.dump(
            processed,
            f,
            default=str,
        )


def dump_processing_errors(
    working_folder: URI, step_name: str, errors: list[CriticalProcessingError]
):
    """Write out critical processing errors"""
    if not working_folder:
        raise AttributeError("processed files path not passed")
    if not step_name:
        raise AttributeError("step name not passed")
    if not errors:
        raise AttributeError("errors list not passed")

    error_file: URI = fh.joinuri(working_folder, "processing_errors", "processing_errors.json")
    processed = []

    for error in errors:
        processed.append(
            {
                "step_name": step_name,
                "error_location": "processing",
                "error_level": "integrity",
                "error_message": error.error_message,
                "error_traceback": error.messages,
            }
        )

    with fh.open_stream(error_file, "a") as f:
        json.dump(
            processed,
            f,
            default=str,
        )


def extract_and_pivot_keys(
    df: pl.DataFrame, key_field: str = "Key"   # type: ignore
) -> pl.DataFrame:  # type: ignore
    """
    Extract key pair values from a key fields column (str) and pivot the keys into new columns.

    Where no keys exist for a given field, the unmodified dataframe will be returned and instances
    of a mixture of actual keys and non valid values (null, None & "") a new column will not be
    generated.

    Args:
        df (pl.DataFrame): dataframe to manipulate
        key_field (str): name of column to extract key, value pairs from

    Returns:
        pl.DataFrame: Polars DataFrame with pivoted keys
    """
    original_columns = df.columns
    index_columns = [c for c in original_columns if c != key_field]

    if pl_row_count(
        df.select(key_field)
        .filter(
            (pl.col(key_field).str.lengths() > 0)  # type: ignore
            & ~(pl.col(key_field).eq("None"))  # type: ignore
        )
    ) == 0:
        return df

    return (
        df
        .with_columns(pl.col(key_field).str.extract_all(r"(\w+): (\w+)"))  # type: ignore
        .explode(key_field)
        .with_columns(
            pl.col(key_field).str.split_exact(":", 1)  # type: ignore
            .struct.rename_fields(["pivot_key", "pivot_values"])
            .alias("ids")
        )
        .unnest("ids")
        .select(
            *[pl.col(c) for c in original_columns],  # type: ignore
            (pl.col("pivot_key") + pl.lit("_Identifier")).alias("pivot_key"),  # type: ignore
            (pl.col("pivot_values").str.strip(" ")).alias("pivot_values"),  # type: ignore
        )
        .pivot(
            values="pivot_values",
            index=index_columns,
            columns="pivot_key"
        )
        .drop(["null"])
    )
