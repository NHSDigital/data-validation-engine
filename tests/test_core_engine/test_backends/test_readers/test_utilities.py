import datetime as dt
from pathlib import Path
import tempfile
from uuid import uuid4

import pytest
from pydantic import BaseModel, create_model

from dve.core_engine.backends.readers.utilities import check_csv_header_expected

@pytest.mark.parametrize(
    ["header_row", "delim", "schema", "expected"],
    [
        (
            "field1,field2,field3",
            ",",
            {"field1": (str, ...), "field2": (int, ...), "field3": (float, 1.2)},
            set(),
        ),
        (
            "field2,field3,field1",
            ",",
            {"field1": (str, ...), "field2": (int, ...), "field3": (float, 1.2)},
            set(),
        ),
        (
            "str_field|int_field|date_field|",
            ",",
            {"str_field": (str, ...), "int_field": (int, ...), "date_field": (dt.date, dt.date.today())},
            {"str_field","int_field","date_field"},
        ),
        (
            '"str_field"|"int_field"|"date_field"',
            "|",
            {"str_field": (str, ...), "int_field": (int, ...), "date_field": (dt.date, dt.date.today())},
            set(),
        ),
        (
            'str_field,int_field,date_field\n',
            ",",
            {"str_field": (str, ...), "int_field": (int, ...), "date_field": (dt.date, dt.date.today())},
            set(),
        ),
        
    ],
)
def test_check_csv_header_expected(
    header_row: str, delim: str, schema: type[BaseModel], expected: set[str]
):
    mdl = create_model("TestModel", **schema)
    with tempfile.TemporaryDirectory() as tmpdir:
        fle = Path(tmpdir).joinpath(f"test_file_{uuid4().hex}.csv")
        fle.open("w+").write(header_row)
        res = check_csv_header_expected(fle.as_posix(), mdl, delim)
    assert res == expected