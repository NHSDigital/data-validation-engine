"""test utility functions & objects in dve.reporting module"""

import tempfile
from pathlib import Path

import polars as pl

from dve.core_engine.exceptions import CriticalProcessingError
from dve.reporting.utils import dump_processing_errors

# pylint: disable=C0116


def test_dump_processing_errors():
    perror_schema = {
        "step_name": pl.Utf8(),
        "error_location": pl.Utf8(),
        "error_level": pl.Utf8(),
        "error_message": pl.Utf8(),
        "error_stacktrace": pl.List(pl.Utf8()),
    }
    with tempfile.TemporaryDirectory() as temp_dir:
        dump_processing_errors(
            temp_dir,
            "test_step",
            [CriticalProcessingError("test error message")]
        )

        output_path = Path(temp_dir, "processing_errors")

        assert output_path.exists()
        assert len(list(output_path.iterdir())) == 1

        expected_df = pl.DataFrame(
            [
                {
                    "step_name": "test_step",
                    "error_location": "processing",
                    "error_level": "integrity",
                    "error_message": "test error message",
                    "error_stacktrace": None,
                },
            ],
            perror_schema
        )
        error_df = pl.read_json(
            Path(output_path, "processing_errors.json")
        )
        cols_to_check = ["step_name", "error_location", "error_level", "error_message"]

        assert error_df.select(pl.col(k) for k in cols_to_check).equals(expected_df.select(pl.col(k) for k in cols_to_check))
