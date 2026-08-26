"""Reader utils"""

import tempfile
from pathlib import Path

import polars as pl
import pytest
from pydantic import BaseModel


class TestParquetModel(BaseModel):
    col1: str
    col2: int

@pytest.fixture(scope="function")
def test_parquet():
    with tempfile.TemporaryDirectory() as tdir:
        df = pl.DataFrame(
            [{"col1": "abc", "col2": 123}]
        )
        write_path = Path(tdir, "test_parquet.parquet")
        df.write_parquet(write_path)

        yield write_path, df.schema
