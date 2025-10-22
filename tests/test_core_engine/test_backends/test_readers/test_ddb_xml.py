from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

import pytest
from duckdb import DuckDBPyRelation, default_connection
from lxml import etree as ET
from pydantic import BaseModel

from dve.core_engine.backends.implementations.duckdb.readers.xml import DuckDBXMLStreamReader


@pytest.fixture
def temp_dir():
    with TemporaryDirectory(prefix="ddb_test_xml_reader") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_xml_file(temp_dir: Path):
    header_data: Dict[str, str] = {
        "school_name": "Meadow Fields",
        "category": "Primary",
        "headteacher": "Mrs Smith",
    }
    class_data: Dict[str, Dict[str, str]] = {
        "year_1": {"class_size": "10", "teacher": "Mrs Armitage"},
        "year_2": {"class_size": "12", "teacher": "Mr Barney"},
    }

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
    for nm, val in header_data.items():
        _tag = ET.SubElement(header, nm)
        _tag.text = val

    data = ET.SubElement(root, "ClassData")
    for nm, val in class_data.items():
        _parent_tag = ET.SubElement(data, nm)
        for sub_nm, sub_val in val.items():
            _child_tag = ET.SubElement(_parent_tag, sub_nm)
            _child_tag.text = sub_val

    with open(temp_dir.joinpath("test.xml"), mode="wb") as xml_fle:
        xml_fle.write(ET.tostring(root))

    yield temp_dir.joinpath("test.xml"), HeaderModel, header_data, ClassDataModel, class_data


def test_ddb_xml_reader_all_str(temp_xml_file):
    uri, header_model, header_data, class_data_model, class_data = temp_xml_file
    ddb_conn = default_connection
    header_reader = DuckDBXMLStreamReader(
        ddb_connection=ddb_conn, root_tag="root", record_tag="Header"
    )
    class_reader = DuckDBXMLStreamReader(
        ddb_connection=ddb_conn, root_tag="root", record_tag="ClassData"
    )
    header_rel: DuckDBPyRelation = header_reader.read_to_relation(
        uri.as_uri(), "header", header_model
    )
    class_rel: DuckDBPyRelation = class_reader.read_to_relation(
        uri.as_uri(), "class_data", class_data_model
    )
    assert header_rel.count("*").fetchone()[0] == 1
    assert header_rel.df().to_dict("records")[0] == header_data
    assert class_rel.count("*").fetchone()[0] == 1
    assert class_rel.df().to_dict("records")[0] == class_data


def test_ddb_xml_reader_write_parquet(temp_xml_file):
    uri, header_model, header_data, class_data_model, class_data = temp_xml_file
    ddb_conn = default_connection
    header_reader = DuckDBXMLStreamReader(
        ddb_connection=ddb_conn, root_tag="root", record_tag="Header"
    )
    class_reader = DuckDBXMLStreamReader(
        ddb_connection=ddb_conn, root_tag="root", record_tag="ClassData"
    )
    header_rel: DuckDBPyRelation = header_reader.read_to_relation(
        uri.as_uri(), "header", header_model
    )
    class_rel: DuckDBPyRelation = class_reader.read_to_relation(
        uri.as_uri(), "class_data", class_data_model
    )
    target_header_loc: Path = uri.parent.joinpath("header_parquet.parquet").as_posix()
    target_class_loc: Path = uri.parent.joinpath("class_parquet.parquet").as_posix()
    header_reader.write_parquet(entity=header_rel, target_location=target_header_loc)
    class_reader.write_parquet(entity=class_rel, target_location=target_class_loc)
    header_parquet_rel: DuckDBPyRelation = header_reader.ddb_connection.read_parquet(
        target_header_loc
    )
    class_parquet_rel: DuckDBPyRelation = class_reader.ddb_connection.read_parquet(target_class_loc)
    assert header_parquet_rel.df().to_dict(orient="records") == header_rel.df().to_dict(
        orient="records"
    )
    assert class_parquet_rel.df().to_dict(orient="records") == class_rel.df().to_dict(
        orient="records"
    )
