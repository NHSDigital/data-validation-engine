"""Utilities for the behave tests."""

import json
from functools import partial
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlparse

import context_tools as ctxt
import polars as pl
from pandas import read_excel, DataFrame, read_parquet
from behave.runner import Context  # type: ignore

from dve.parser.file_handling.service import open_stream
from dve.parser.type_hints import URI

ERROR_DF_FIELDS: List[str] = [
    "Entity",
    "Key",
    "ErrorCode",
    "FailureType",
    "Status",
    "ErrorType",
    "ErrorLocation",
    "ErrorMessage",
    "ReportingField",
    "Category",
]
SERVICE_TO_STORAGE_PATH_MAPPING: Dict[str, str] = {
    "file_transformation": "transform",
}


def get_test_file_path(test_file_name: str) -> Path:
    """Get the path to a specific test file."""
    return Path(__file__).parent.parent.parent.joinpath("testdata", test_file_name)


# use lightweight python to read - polars?
# handle whether directory of parquets or single parquet file
def read_output_parquet(processing_folder: Path, entity_name: str, service: str) -> DataFrame:
    _src = Path(processing_folder,
                SERVICE_TO_STORAGE_PATH_MAPPING.get(service,service),
                entity_name)
    return read_parquet(path=_src.as_posix())

def load_errors_from_service(processing_folder: Path, service: str) -> pl.DataFrame:
    err_location = Path(
        processing_folder,
        "errors",
        f"{SERVICE_TO_STORAGE_PATH_MAPPING.get(service, service)}_errors.jsonl",
    )
    msgs = []
    try:
        with open(err_location) as errs:
            msgs = [json.loads(err) for err in errs.readlines()]
    except FileNotFoundError:
        pass

    return pl.DataFrame(msgs, schema={fld: pl.Utf8() for fld in ERROR_DF_FIELDS})


def display_report(uri: URI):
    """
    Helper to display contents of text/report files. Useful to increase verbosity
    in some test steps in event of errors.

    Has additional newlines to handle `behave`'s output formatting.

    """
    print("\n\n")
    print("Displaying contents of file", uri)
    df = read_excel(uri, sheet_name="Error Data")
    print(df)
    print("".rjust(100, "="))
    print("\n\n")


def get_all_errors_df(context: Context) -> pl.DataFrame:
    processing_loc = ctxt.get_processing_location(context)
    services = ["file_transformation", "data_contract", "business_rules"]
    get_msgs = partial(load_errors_from_service, processing_folder=processing_loc)
    err_df = pl.concat(map(lambda x: get_msgs(service=x), services))

    return err_df
