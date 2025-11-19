"""Shared fixtures for the transformations tests."""

# pylint: disable=redefined-outer-name
import json
from datetime import date, datetime, time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Iterator, List, Tuple

import duckdb
import lxml.etree as ET
import pytest
from duckdb import (
    ColumnExpression,
    ConstantExpression,
    DuckDBPyConnection,
    DuckDBPyRelation,
    FunctionExpression,
    StarExpression,
)
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col
from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dve.core_engine.backends.implementations.duckdb.readers.csv import SQLType
from dve.core_engine.type_hints import URI
from tests.conftest import get_test_file_path


@pytest.fixture(scope="function")
def planets_df() -> Iterator[DataFrame]:
    """The planets data, in a DataFrame."""
    planets_uri = get_test_file_path("planets/planets.csv").absolute().as_uri()
    yield SparkSession.builder.getOrCreate().read.csv(planets_uri, header=True)


@pytest.fixture(scope="function")
def satellites_df() -> Iterator[DataFrame]:
    """The satellites data, in a DataFrame."""
    satellites_uri = get_test_file_path("planets/satellites_demo.csv").absolute().as_uri()
    satellites = SparkSession.builder.getOrCreate().read.csv(satellites_uri, header=True)
    satellites.withColumn("gm", sf.split(col("gm"), "±").getItem(0).cast("double"))
    yield satellites


@pytest.fixture(scope="function")
def largest_satellites_df(satellites_df: DataFrame) -> Iterator[DataFrame]:
    """The largest satellites for each planet from `satellites_df`."""
    max_only = satellites_df.groupBy("planet").agg(sf.expr("max_by(name, gm)").alias("name"))
    yield satellites_df.join(max_only, on=["planet", "name"], how="left_semi")


@pytest.fixture(scope="function")
def value_literal_1_header() -> Iterator[DataFrame]:
    """
    A DataFrame with integer column 'Value' containing a single row with a
    literal value of 1.

    """
    yield SparkSession.builder.getOrCreate().sql("SELECT 1 AS Value")


@pytest.fixture(scope="function")
def duckdb_connection() -> Iterator[DuckDBPyConnection]:
    yield duckdb.connect()


@pytest.fixture
def temp_duckdb_dir():
    with TemporaryDirectory(prefix="ddb_test_") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def temp_csv_file(temp_duckdb_dir: Path):
    header: str = "ID,varchar_field,bigint_field,date_field,timestamp_field,time_field"
    typed_data = [
        [1, "hi", 3, date(2023, 1, 3), datetime(2023, 1, 3, 12, 0, 3), time(12, 0, 0)],
        [2, "bye", 4, date(2023, 3, 7), datetime(2023, 5, 9, 15, 21, 53), time(13, 0 ,0)],
    ]

    class SimpleModel(BaseModel):
        ID: int
        varchar_field: str
        bigint_field: int
        date_field: date
        timestamp_field: datetime
        time_field: time

    with open(temp_duckdb_dir.joinpath("dummy.csv"), mode="w") as csv_file:
        csv_file.write(header + "\n")
        for rw in typed_data:
            csv_file.write(",".join([str(val) for val in rw]) + "\n")

    yield temp_duckdb_dir.joinpath("dummy.csv"), header, typed_data, SimpleModel


@pytest.fixture
def temp_xml_file(temp_dir: Path):
    header_data: Dict[str, str] = {
        "school_name": "Meadow Fields",
        "category": "Primary",
        "headteacher": "Mrs Smith",
    }
    class_data: List[Dict[str, Any]] = [
        {
            "year": 1,
            "class_info": {
                "class_size": 10,
                "teacher": "Mrs Armitage",
                "date_updated": date(2023, 11, 5),
                "class_houses": ["houseA", "houseB"],
            },
        },
        {
            "year": 2,
            "class_info": {
                "class_size": 12,
                "teacher": "Mr Barney",
                "date_updated": date(2023, 11, 6),
                "class_houses": ["houseC"],
            },
        },
    ]

    class HeaderModel(BaseModel):
        school_name: str
        category: str
        headteacher: str

    class ClassInfo(BaseModel):
        class_size: int
        teacher: str
        date_updated: date
        class_houses: List[str]

    class ClassDataModel(BaseModel):
        year: int
        class_info: ClassInfo

    root = ET.Element("root")
    header = ET.SubElement(root, "Header")
    for nm, val in header_data.items():
        _tag = ET.SubElement(header, nm)
        _tag.text = str(val)

    for rw in class_data:
        data = ET.SubElement(root, "ClassData")
        for nm, val in rw.items():
            _parent_tag = ET.SubElement(data, nm)
            if isinstance(val, dict):
                for sub_nm, sub_val in val.items():
                    if isinstance(sub_val, list):
                        for itm in sub_val:
                            _child_tag = ET.SubElement(_parent_tag, sub_nm)
                            _child_tag.text = str(itm)
                        continue
                    _child_tag = ET.SubElement(_parent_tag, sub_nm)
                    _child_tag.text = str(sub_val)
                continue
            if isinstance(val, list):
                for itm in val:
                    _child_tag = ET.SubElement(_parent_tag, nm)
                    _child_tag.text = itm
                continue
            _parent_tag.text = str(val)

    with open(Path(temp_dir).joinpath("test.xml"), mode="wb") as xml_fle:
        xml_fle.write(ET.tostring(root))

    yield Path(temp_dir).joinpath("test.xml"), HeaderModel, header_data, ClassDataModel, class_data


@pytest.fixture(scope="function")
def planets_rel(duckdb_connection: DuckDBPyConnection) -> Iterator[DuckDBPyRelation]:
    """The planets data, in a DataFrame."""
    planets_uri = get_test_file_path("planets/planets.csv").as_posix()
    yield duckdb_connection.read_csv(planets_uri, header=True)


@pytest.fixture(scope="function")
def satellites_rel(duckdb_connection: DuckDBPyConnection) -> Iterator[DuckDBPyRelation]:
    """The satellites data, in a DataFrame."""
    satellites_uri = get_test_file_path("planets/satellites_demo.csv").as_posix()
    satellites = duckdb_connection.read_csv(satellites_uri, header=True)
    satellites.select(
        StarExpression(exclude=["gm"]),
        FunctionExpression(
            "array_extract",
            FunctionExpression("str_split", ColumnExpression("gm"), ConstantExpression("±")),
            duckdb.ConstantExpression(1),
        )
        .cast("double")
        .alias("gm"),
    )
    yield satellites


@pytest.fixture(scope="function")
def largest_satellites_rel(satellites_rel: DuckDBPyRelation) -> Iterator[DuckDBPyRelation]:
    """The largest satellites for each planet from `satellites_df`."""
    max_only = satellites_rel.aggregate("planet, max_by(name, gm) as name").set_alias("mx")
    yield satellites_rel.set_alias("sat").join(
        max_only, condition="sat.planet = mx.planet and sat.name = mx.name", how="semi"
    )


@pytest.fixture(scope="function")
def simple_all_string_parquet(temp_dir) -> Iterator[Tuple[URI, str, List[Dict[str, Any]]]]:
    contract_meta = json.dumps(
        {
            "contract": {
                "datasets": {
                    "simple_model": {
                        "fields": {
                            "id": "int",
                            "datefield": "date",
                            "strfield": "str",
                            "datetimefield": "datetime",
                        },
                        "reader_config": {
                            ".csv": {
                                "reader": "DuckDBCSVReader",
                                "parameters": {"header": True, "delim": ","},
                            }
                        },
                        "key_field": "id",
                    },
                },
            }
        }
    )

    _spark: SparkSession = SparkSession.builder.getOrCreate()
    data: List[Dict[str, Any]] = [
        dict(
            id=1,
            datefield=str(date(2020, 9, 20)),
            strfield="hi",
            datetimefield=str(datetime(2020, 9, 20, 12, 34, 56)),
        ),
        dict(
            id=2,
            datefield=str(date(2020, 9, 21)),
            strfield="hi",
            datetimefield=str(datetime(2020, 9, 21, 12, 34, 56)),
        ),
    ]

    output_location: URI = str(Path(temp_dir).joinpath("simple_all_string_parquet")) + "/"

    _df: DataFrame = _spark.createDataFrame(
        data,
        schema=StructType(
            [
                StructField("id", StringType()),
                StructField("datefield", StringType()),
                StructField("strfield", StringType()),
                StructField("datetimefield", StringType()),
            ]
        ),
    )
    _df.coalesce(1).write.format("parquet").save(output_location)
    yield output_location, contract_meta, data


@pytest.fixture(scope="function")
def nested_all_string_parquet(temp_dir) -> Iterator[Tuple[URI, str, List[Dict[str, Any]]]]:
    contract_meta = json.dumps(
        {
            "contract": {
                "schemas": {
                    "SubField": {
                        "fields": {
                            "id": "int",
                            "substrfield": "str",
                            "subarrayfield": {"type": "date", "is_array": True},
                        },
                        "mandatory_fields": ["id"],
                    }
                },
                "datasets": {
                    "nested_model": {
                        "fields": {
                            "id": "int",
                            "strfield": "str",
                            "datetimefield": "datetime",
                            "subfield": {"model": "SubField", "is_array": True},
                        },
                        "reader_config": {
                            ".xml": {
                                "reader": "DuckDBXMLStreamReader",
                                "parameters": {"root_tag": "root", "record_tag": "NestedModel"},
                            }
                        },
                        "key_field": "id",
                    }
                },
            }
        }
    )

    _spark: SparkSession = SparkSession.builder.getOrCreate()
    data: List[Dict[str, Any]] = [
        dict(
            id=1,
            strfield="hi",
            datetimefield=str(datetime(2020, 9, 20, 12, 34, 56)),
            subfield=[
                dict(
                    id=1,
                    substrfield="bye",
                    subarrayfield=[str(date(2020, 9, 20)), str(date(2020, 9, 21))],
                )
            ],
        ),
        dict(
            id=2,
            strfield="hello",
            datetimefield=str(datetime(2020, 9, 21, 12, 34, 56)),
            subfield=[
                dict(
                    id=2,
                    substrfield="bye",
                    subarrayfield=[str(date(2020, 9, 20)), str(date(2020, 9, 21))],
                ),
                dict(
                    id=2,
                    substrfield="aurevoir",
                    subarrayfield=[str(date(2020, 9, 22)), str(date(2020, 9, 23))],
                ),
            ],
        ),
    ]

    output_location: URI = str(Path(temp_dir).joinpath("nested_parquet").as_posix()) + "/"

    _df: DataFrame = _spark.createDataFrame(
        data,
        schema=StructType(
            [
                StructField("id", StringType()),
                StructField("strfield", StringType()),
                StructField("datetimefield", StringType()),
                StructField(
                    "subfield",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("substrfield", StringType()),
                                StructField("subarrayfield", ArrayType(StringType())),
                            ]
                        )
                    ),
                ),
            ]
        ),
    )
    _df.coalesce(1).write.format("parquet").save(output_location)
    yield output_location, contract_meta, data


@pytest.fixture(scope="function")
def simple_typecast_parquet(temp_dir) -> Iterator[Tuple[URI, List[Dict[str, Any]]]]:
    _spark: SparkSession = SparkSession.builder.getOrCreate()
    data: List[Dict[str, Any]] = [
        dict(
            id=1,
            datefield=date(2020, 9, 20),
            strfield="hi",
            datetimefield=datetime(2020, 9, 20, 12, 34, 56),
        ),
        dict(
            id=2,
            datefield=date(2020, 9, 21),
            strfield="hi",
            datetimefield=datetime(2020, 9, 21, 12, 34, 56),
        ),
    ]

    output_location: URI = str(Path(temp_dir).joinpath("simple_typecast_parquet").as_posix()) + "/"

    _df: DataFrame = _spark.createDataFrame(
        data,
        schema=StructType(
            [
                StructField("id", LongType()),
                StructField("datefield", DateType()),
                StructField("strfield", StringType()),
                StructField("datetimefield", TimestampType()),
            ]
        ),
    )
    _df.coalesce(1).write.format("parquet").save(output_location)
    yield output_location, data


@pytest.fixture(scope="function")
def nested_typecast_parquet(temp_dir) -> Iterator[Tuple[URI, List[Dict[str, Any]]]]:
    _spark: SparkSession = SparkSession.builder.getOrCreate()
    data: List[Dict[str, Any]] = [
        dict(
            id=1,
            strfield="hi",
            datetimefield=datetime(2020, 9, 20, 12, 34, 56),
            subfield=[
                dict(id=1, substrfield="bye", subarrayfield=[date(2020, 9, 20), date(2020, 9, 21)])
            ],
        ),
        dict(
            id=2,
            strfield="hello",
            datetimefield=datetime(2020, 9, 21, 12, 34, 56),
            subfield=[
                dict(id=2, substrfield="bye", subarrayfield=[date(2020, 9, 20), date(2020, 9, 21)]),
                dict(
                    id=2,
                    substrfield="aurevoir",
                    subarrayfield=[date(2020, 9, 22), date(2020, 9, 23)],
                ),
            ],
        ),
    ]

    output_location: URI = (
        str(Path(temp_dir).joinpath("nested_typecast_parquet.parquet").as_posix()) + "/"
    )

    _df: DataFrame = _spark.createDataFrame(
        data,
        schema=StructType(
            [
                StructField("id", LongType()),
                StructField("strfield", StringType()),
                StructField("datetimefield", TimestampType()),
                StructField(
                    "subfield",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", LongType()),
                                StructField("substrfield", StringType()),
                                StructField("subarrayfield", ArrayType(DateType())),
                            ]
                        )
                    ),
                ),
            ]
        ),
    )
    _df.coalesce(1).write.format("parquet").save(output_location)
    yield output_location, data

@pytest.fixture(scope="function")
def nested_all_string_parquet_w_errors(temp_dir,
                                       nested_parquet_custom_dc_err_details) -> Iterator[Tuple[URI, str, List[Dict[str, Any]]]]:
    contract_meta = json.dumps(
        {
            "contract": {
                "error_details": f"{nested_parquet_custom_dc_err_details.as_posix()}",
                "schemas": {
                    "SubField": {
                        "fields": {
                            "id": "int",
                            "substrfield": "str",
                            "subarrayfield": {"type": "date", "is_array": True},
                        },
                        "mandatory_fields": ["id"],
                    }
                },
                "datasets": {
                    "nested_model": {
                        "fields": {
                            "id": "int",
                            "strfield": "str",
                            "datetimefield": "datetime",
                            "subfield": {"model": "SubField", "is_array": True},
                        },
                        "reader_config": {
                            ".xml": {
                                "reader": "DuckDBXMLStreamReader",
                                "parameters": {"root_tag": "root", "record_tag": "NestedModel"},
                            }
                        },
                        "key_field": "id",
                    }
                },
            }
        }
    )

    _spark: SparkSession = SparkSession.builder.getOrCreate()
    data: List[Dict[str, Any]] = [
        dict(
            id=1,
            strfield="hi",
            datetimefield=str(datetime(2020, 9, 20, 12, 34, 56)),
            subfield=[
                dict(
                    id=1,
                    substrfield="bye",
                    subarrayfield=[str(date(2020, 9, 20)), str(date(2020, 9, 21))],
                )
            ],
        ),
        dict(
            id="WRONG",
            strfield="hello",
            datetimefield=str(datetime(2020, 9, 21, 12, 34, 56)),
            subfield=[
                dict(
                    id=2,
                    substrfield="bye",
                    subarrayfield=[str(date(2020, 9, 20)), str(date(2020, 9, 21))],
                ),
                dict(
                    id="WRONG",
                    substrfield="aurevoir",
                    subarrayfield=[str(date(2020, 9, 22)), str(date(2020, 9, 23))],
                ),
            ],
        ),
    ]

    output_location: URI = str(Path(temp_dir).joinpath("nested_parquet").as_posix()) + "/"

    _df: DataFrame = _spark.createDataFrame(
        data,
        schema=StructType(
            [
                StructField("id", StringType()),
                StructField("strfield", StringType()),
                StructField("datetimefield", StringType()),
                StructField(
                    "subfield",
                    ArrayType(
                        StructType(
                            [
                                StructField("id", StringType()),
                                StructField("substrfield", StringType()),
                                StructField("subarrayfield", ArrayType(StringType())),
                            ]
                        )
                    ),
                ),
            ]
        ),
    )
    _df.coalesce(1).write.format("parquet").save(output_location)
    yield output_location, contract_meta, data


@pytest.fixture()
def nested_parquet_custom_dc_err_details(temp_dir):
    file_path = Path(temp_dir).joinpath("nested_parquet_data_contract_codes.json")
    err_details = {
        "id": {
            "Blank": {"error_code": "TESTIDBLANK",
                      "error_message": "id cannot be null"},
            "Bad value": {"error_code": "TESTIDBAD",
                          "error_message": "id is invalid: id - {{id}}"}
                },
        "datetimefield": {
            "Bad value": {"error_code": "TESTDTFIELDBAD",
                          "error_message": "datetimefield is invalid: id - {{id}}, datetimefield - {{datetimefield}}"}
        },
        "subfield.id": {
            "Blank": {"error_code": "SUBFIELDTESTIDBLANK",
                      "error_message": "subfield id cannot be null"},
            "Bad value": {"error_code": "SUBFIELDTESTIDBAD",
                          "error_message": "subfield id is invalid: subfield.id - {{__error_value}}"}
                },
            }
    with open(file_path, mode="w") as fle:
        json.dump(err_details, fle)
    
    yield file_path

    
