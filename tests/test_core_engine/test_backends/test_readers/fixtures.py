

from datetime import date, datetime
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterator
from pydantic import BaseModel
import pytest
import polars as pl
from lxml import etree as ET

## Models
class SimpleModel(BaseModel):
    id_field: int
    varchar_field: str
    bigint_field: int
    date_field: date
    timestamp_field: datetime


class SimpleHeaderModel(BaseModel):
    header_1: str
    header_2: str


class VerySimpleModel(BaseModel):
    test_col: str

@pytest.fixture
def temp_dir():
    with TemporaryDirectory(prefix="reader_testing") as temp_dir:
        yield Path(temp_dir)

## CSV

@pytest.fixture(scope="function")
def temp_csv_file(temp_dir: Path):
    header: str = "id_field,varchar_field,bigint_field,date_field,timestamp_field"
    typed_data = [
        [1, "hi", 1, date(2023, 1, 3), datetime(2023, 1, 3, 12, 0, 3)],
        [2, "bye", 2, date(2023, 3, 7), datetime(2023, 5, 9, 15, 21, 53)],
    ]

    with open(temp_dir.joinpath("dummy.csv"), mode="w") as csv_file:
        csv_file.write(header + "\n")
        for rw in typed_data:
            csv_file.write(",".join([str(val) for val in rw]) + "\n")

    yield temp_dir.joinpath("dummy.csv"), header, typed_data, SimpleModel


@pytest.fixture(scope="function")
def temp_csv_file_additional_fields(temp_dir: Path) -> Iterator[str]:
    test_df = pl.DataFrame({"test_col": ["fine"], "test_col2": ["wow"]})
    file_uri = temp_dir.joinpath("test_additional_fields.csv").as_posix()
    test_df.write_csv(
        file_uri,
        include_header=True,
        quote_style="always"
    )

    yield file_uri


@pytest.fixture(scope="function")
def temp_csv_file_missing_fields(temp_dir: Path) -> Iterator[str]:
    test_df = pl.DataFrame({"header_1": ["fine"]})
    file_uri = temp_dir.joinpath("test_missing_fields.csv").as_posix()
    test_df.write_csv(
        file_uri,
        include_header=True,
        quote_style="always"
    )

    yield file_uri


@pytest.fixture
def temp_empty_csv_file(temp_dir: Path):
    with open(temp_dir.joinpath("empty.csv"), mode="w"):
        pass

    yield temp_dir.joinpath("empty.csv"), SimpleModel

@pytest.fixture
def temp_csv_with_null_strings(temp_dir: Path):
    test_df = pl.DataFrame({"test_col": ["fine", " ", "    "]})
    file_uri = temp_dir.joinpath("test_empty_string1.csv").as_posix()
    test_df.write_csv(
        file_uri,
        include_header=True,
        quote_style="always"
    )
    yield file_uri, VerySimpleModel

@pytest.fixture
def temp_csv_with_null_records(temp_dir: Path):
    data = {"id_field": ["1", "2" ,"", ""],
                "varchar_field": ["fine", "    ", "  ", " "],
                "bigint_field": ["", "3", "", "     "],
                "date_field": [date(2023, 1, 3),date(2023, 1, 5), None, None],
                "timestamp_field": [None, datetime(2023, 1, 3, 12, 0, 3), None, None]}
    test_df = pl.DataFrame(data)
    file_uri = temp_dir.joinpath("test_remove_null_recs1.csv").as_posix()
    test_df.write_csv(
        file_uri,
        include_header=True,
        quote_style="always"
    )
    yield file_uri, SimpleModel
    
## JSON

@pytest.fixture
def temp_json_file(temp_dir: Path):
    field_names: list[str] = ["id_field","varchar_field","bigint_field","date_field","timestamp_field"]
    typed_data = [
        [1,"hi", 1, date(2023, 1, 3), datetime(2023, 1, 3, 12, 0, 3)],
        [2,"bye", 2, date(2023, 3, 7), datetime(2023, 5, 9, 15, 21, 53)],
    ]
    
    test_data = [dict(zip(field_names, rw)) for rw in typed_data]

    with open(temp_dir.joinpath("test.json"), mode="w") as json_file:
        json.dump(test_data, json_file, default=str)

    yield temp_dir.joinpath("test.json"), test_data, SimpleModel

@pytest.fixture
def temp_json_file_w_null_recs(temp_dir: Path):
    field_names: list[str] = ["id_field","varchar_field","bigint_field","date_field","timestamp_field"]
    typed_data = [
        [1,"hi", 1, date(2023, 1, 3), datetime(2023, 1, 3, 12, 0, 3)],
        [None, None, None, None],
        [3,"bye", 3, date(2023, 3, 7), datetime(2023, 5, 9, 15, 21, 53)],
        [None, None, None, None]
    ]
    
    test_data = [dict(zip(field_names, rw)) for rw in typed_data]

    with open(temp_dir.joinpath("test.json"), mode="w") as json_file:
        json.dump(test_data, json_file, default=str)

    yield temp_dir.joinpath("test.json"), test_data, SimpleModel

## XML

@pytest.fixture
def temp_xml_file(temp_dir: Path):
    header_data: list[dict[str, str]] = [{
        "school_name": "Meadow Fields",
        "category": "Primary",
        "headteacher": "Mrs Smith",
    }]
    class_data: list[dict[str, dict[str, str]]] = [{
        "year_1": {"class_size": "10", "teacher": "Mrs Armitage"},
        "year_2": {"class_size": "12", "teacher": "Mr Barney"},
    }]

    class HeaderModel(BaseModel):
        school_name: str
        category: str
        headteacher: str

    class ClassInfo(BaseModel):
        class_size: int
        teacher: str

    class ClassDataModel(BaseModel):
        year_1: ClassInfo
        year_2: ClassInfo

    root = ET.Element("root")
    header = ET.SubElement(root, "Header")
    for nm, val in header_data[0].items():
        _tag = ET.SubElement(header, nm)
        _tag.text = val

    for dta in class_data:
        data = ET.SubElement(root, "ClassData")
        for nm, val in dta.items():
            _parent_tag = ET.SubElement(data, nm)
            for sub_nm, sub_val in val.items():
                _child_tag = ET.SubElement(_parent_tag, sub_nm)
                _child_tag.text = sub_val

    with open(temp_dir.joinpath("test.xml"), mode="wb") as xml_fle:
        xml_fle.write(ET.tostring(root))

    yield temp_dir.joinpath("test.xml"), HeaderModel, header_data, ClassDataModel, class_data

@pytest.fixture
def temp_xml_file_w_null_recs(temp_dir: Path):
    header_data: list[dict[str, str]] = [{
        "school_name": "Meadow Fields",
        "category": "Primary",
        "headteacher": "Mrs Smith",
    }]
    class_data: list[ dict[str, Any]] = [
        {"year_group": 1, "class_size": "10", "teacher": "Mrs Armitage"},
        {"year_group": 2, "class_size": "12", "teacher": "Mr Barney"},
        {"year_group": None, "class_size": None, "teacher": None}]

    class HeaderModel(BaseModel):
        school_name: str
        category: str
        headteacher: str

    class ClassInfo(BaseModel):
        year_group: int
        class_size: int
        teacher: str

    root = ET.Element("root")
    header = ET.SubElement(root, "Header")
    for nm, val in header_data[0].items():
        _tag = ET.SubElement(header, nm)
        _tag.text = val

    for dta in class_data:
        data = ET.SubElement(root, "ClassData")
        for nm, val in dta.items():
            _child_tag = ET.SubElement(data, nm)
            _child_tag.text = str(val) if val else None

    with open(temp_dir.joinpath("test_with_nulls.xml"), mode="wb") as xml_fle:
        xml_fle.write(ET.tostring(root))

    yield temp_dir.joinpath("test_with_nulls.xml"), HeaderModel, header_data, ClassInfo, class_data