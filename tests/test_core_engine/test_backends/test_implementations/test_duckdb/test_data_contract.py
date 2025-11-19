import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
from duckdb import DuckDBPyRelation, default_connection
from duckdb.typing import DuckDBPyType

from dve.core_engine.backends.implementations.duckdb.contract import DuckDBDataContract
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.readers.csv import DuckDBCSVReader
from dve.core_engine.backends.implementations.duckdb.readers.xml import DuckDBXMLStreamReader
from dve.core_engine.backends.metadata.contract import DataContractMetadata, ReaderConfig
from dve.core_engine.backends.utilities import stringify_model
from dve.core_engine.type_hints import URI
from dve.core_engine.validation import RowValidator
from tests.test_core_engine.test_backends.fixtures import (
    nested_all_string_parquet,
    simple_all_string_parquet,
    nested_all_string_parquet_w_errors,
    nested_parquet_custom_dc_err_details,
    temp_csv_file,
    temp_duckdb_dir,
    temp_xml_file,
)

def test_duckdb_data_contract_csv(temp_csv_file):
    uri, _, _, mdl = temp_csv_file
    connection = default_connection

    contract_meta = json.dumps(
        {
            "contract": {
                "datasets": {
                    "test_ds": {
                        "fields": {
                            "ID": "NonNegativeInt",
                            "varchar_field": "str",
                            "bigint_field": "NonNegativeInt",
                            "date_field": "date",
                            "timestamp_field": "datetime",
                            "time_field": {
                                "description": "test",
                                "callable": "formattedtime",
                                "constraints": {
                                    "time_format": "%Y-%m-%d",
                                    "timezone_treatment": "forbid"
                                }
                            }
                        },
                        "reader_config": {
                            ".csv": {
                                "reader": "DuckDBCSVReader",
                                "parameters": {"header": True, "delim": ","},
                            }
                        },
                        "key_field": "ID",
                    }
                },
            }
        }
    )

    contract_dict = json.loads(contract_meta).get("contract")
    dc_meta = DataContractMetadata(
        reader_metadata={
            "test_ds": {
                ".csv": ReaderConfig(
                    **contract_dict.get("datasets", {})
                    .get("test_ds", {})
                    .get("reader_config", {})
                    .get(".csv")
                )
            }
        },
        validators={"test_ds": RowValidator(contract_dict, "test_ds")},
        reporting_fields={"test_ds": ["ID"]},
    )
    entities: Dict[str, DuckDBPyRelation] = {
        "test_ds": DuckDBCSVReader(
            header=True, delim=",", connection=connection
        ).read_to_entity_type(DuckDBPyRelation, str(uri), "test_ds", stringify_model(mdl))
    }

    data_contract: DuckDBDataContract = DuckDBDataContract(connection)
    entities, messages, stage_successful = data_contract.apply_data_contract(entities, dc_meta)
    rel: DuckDBPyRelation = entities.get("test_ds")
    assert dict(zip(rel.columns, rel.dtypes)) == {
        fld.name: str(get_duckdb_type_from_annotation(fld.annotation))
        for fld in mdl.__fields__.values()
    }
    assert len(messages) == 0
    assert stage_successful


def test_duckdb_data_contract_xml(temp_xml_file):
    uri, header_model, header_data, class_model, class_data = temp_xml_file
    connection = default_connection
    contract_meta = json.dumps(
        {
            "contract": {
                "schemas": {
                    "ClassInfo": {
                        "fields": {
                            "class_size": "int",
                            "teacher": "str",
                            "date_updated": "date",
                            "class_houses": {"type": "str", "is_array": True},
                        },
                        "mandatory_fields": [],
                    }
                },
                "datasets": {
                    "test_header": {
                        "fields": {"school_name": "str", "category": "str", "headteacher": "str"},
                        "reader_config": {
                            ".xml": {
                                "reader": "DuckDBXMLStreamReader",
                                "parameters": {"root_tag": "root", "record_tag": "Header"},
                            }
                        },
                        "key_field": "school_name",
                    },
                    "test_class_info": {
                        "fields": {
                            "year": "int",
                            "class_info": {"model": "ClassInfo", "is_array": False},
                        },
                        "reader_config": {
                            ".xml": {
                                "reader": "DuckDBXMLStreamReader",
                                "parameters": {"root_tag": "root", "record_tag": "ClassData"},
                            }
                        },
                        "key_field": "ID",
                    },
                },
            }
        }
    )

    contract_dict = json.loads(contract_meta).get("contract")
    entities: Dict[str, DuckDBPyRelation] = {
        "test_header": DuckDBXMLStreamReader(
            connection, root_tag="root", record_tag="Header"
        ).read_to_relation(str(uri), "header", header_model),
        "test_class_info": DuckDBXMLStreamReader(
            connection, root_tag="root", record_tag="ClassData"
        ).read_to_relation(str(uri), "class_info", class_model),
    }

    dc_meta = DataContractMetadata(
        reader_metadata={
            "test_header": {
                ".xml": ReaderConfig(
                    **contract_dict.get("datasets", {})
                    .get("test_header", {})
                    .get("reader_config", {})
                    .get(".xml")
                )
            },
            "test_class_info": {
                ".xml": ReaderConfig(
                    **contract_dict.get("datasets", {})
                    .get("test_class_info", {})
                    .get("reader_config", {})
                    .get(".xml")
                )
            },
        },
        validators={
            "test_header": RowValidator(contract_dict, "test_header"),
            "test_class_info": RowValidator(contract_dict, "test_class_info"),
        },
        reporting_fields={"test_header": ["school"], "test_class_info": ["year"]},
    )

    data_contract: DuckDBDataContract = DuckDBDataContract(connection)
    entities, messages, stage_successful = data_contract.apply_data_contract(entities, dc_meta)
    header_rel: DuckDBPyRelation = entities.get("test_header")
    header_expected_schema: Dict[str, DuckDBPyType] = {
        fld.name: get_duckdb_type_from_annotation(fld.type_)
        for fld in header_model.__fields__.values()
    }
    class_data_expected_schema: Dict[str, DuckDBPyType] = {
        fld.name: get_duckdb_type_from_annotation(fld.type_)
        for fld in class_model.__fields__.values()
    }
    class_data_rel: DuckDBPyRelation = entities.get("test_class_info")
    assert len(messages) == 0
    assert header_rel.count("*").fetchone()[0] == 1
    assert dict(zip(header_rel.columns, header_rel.dtypes)) == header_expected_schema
    assert class_data_rel.count("*").fetchone()[0] == 2
    assert dict(zip(class_data_rel.columns, class_data_rel.dtypes)) == class_data_expected_schema
    assert stage_successful


def test_ddb_data_contract_read_and_write_basic_parquet(
    simple_all_string_parquet: Tuple[URI, str, List[Dict[str, Any]]],
):
    # can we read in a stringified parquet and run the data contract on it?
    # basic file - simple data structures
    connection = default_connection
    parquet_uri, contract_meta, _ = simple_all_string_parquet
    data_contract = DuckDBDataContract(connection)
    # check can read
    entity = data_contract.read_parquet(path=parquet_uri)
    assert entity.count("*").fetchone()[0] == 2
    assert dict(zip(entity.columns, entity.dtypes)) == {
        "id": "VARCHAR",
        "datefield": "VARCHAR",
        "strfield": "VARCHAR",
        "datetimefield": "VARCHAR",
    }
    # check processes entity
    contract_dict = json.loads(contract_meta).get("contract")
    entities: Dict[str, DuckDBPyRelation] = {
        "simple_model": entity,
    }

    dc_meta = DataContractMetadata(
        reader_metadata={
            "simple_model": {
                ".csv": ReaderConfig(
                    **contract_dict.get("datasets", {})
                    .get("simple_model", {})
                    .get("reader_config", {})
                    .get(".csv")
                )
            }
        },
        validators={
            "simple_model": RowValidator(contract_dict, "simple_model"),
        },
        reporting_fields={"simple_model": ["id"]},
    )

    entities, messages, stage_successful = data_contract.apply_data_contract(entities, dc_meta)
    assert stage_successful
    assert len(messages) == 0
    assert entities["simple_model"].count("*").fetchone()[0] == 2
    # check writes entity to parquet
    output_path: Path = Path(parquet_uri).parent.joinpath("simple_model_output.parquet")
    data_contract.write_parquet(
        entity=entities["simple_model"], target_location=output_path.as_posix()
    )
    assert output_path.exists()
    # check when read back in what is expected
    check = data_contract.read_parquet(path=output_path.as_posix())
    assert check.count("*").fetchone()[0] == 2
    assert dict(zip(check.columns, check.dtypes)) == {
        "id": "BIGINT",
        "datefield": "DATE",
        "strfield": "VARCHAR",
        "datetimefield": "TIMESTAMP",
    }


def test_ddb_data_contract_read_nested_parquet(nested_all_string_parquet):
    # can we read in a stringified parquet and run the data contract on it?
    # more complex file - nested, arrays of structs
    parquet_uri, contract_meta, _ = nested_all_string_parquet
    connection = default_connection
    data_contract = DuckDBDataContract(connection)
    # check can read
    entity = data_contract.read_parquet(path=parquet_uri)
    assert entity.count("*").fetchone()[0] == 2
    assert dict(zip(entity.columns, entity.dtypes)) == {
        "id": "VARCHAR",
        "strfield": "VARCHAR",
        "datetimefield": "VARCHAR",
        "subfield": "STRUCT(id VARCHAR, substrfield VARCHAR, subarrayfield VARCHAR[])[]",
    }
    # check processes entity
    contract_dict = json.loads(contract_meta).get("contract")
    entities: Dict[str, DuckDBPyRelation] = {
        "nested_model": entity,
    }

    dc_meta = DataContractMetadata(
        reader_metadata={
            "nested_model": {
                ".xml": ReaderConfig(
                    **contract_dict.get("datasets", {})
                    .get("nested_model", {})
                    .get("reader_config", {})
                    .get(".xml")
                )
            }
        },
        validators={
            "nested_model": RowValidator(contract_dict, "nested_model"),
        },
        reporting_fields={"nested_model": ["id"]},
    )

    entities, messages, stage_successful = data_contract.apply_data_contract(entities, dc_meta)
    assert stage_successful
    assert len(messages) == 0
    assert entities["nested_model"].count("*").fetchone()[0] == 2
    # check writes entity to parquet
    output_path: Path = Path(parquet_uri).parent.joinpath("nested_model_output.parquet")
    data_contract.write_parquet(
        entity=entities["nested_model"], target_location=output_path.as_posix()
    )
    assert output_path.exists()
    # check when read back in what is expected
    check = data_contract.read_parquet(path=output_path.as_posix())
    assert check.count("*").fetchone()[0] == 2
    assert dict(zip(check.columns, check.dtypes)) == {
        "id": "BIGINT",
        "strfield": "VARCHAR",
        "datetimefield": "TIMESTAMP",
        "subfield": "STRUCT(id BIGINT, substrfield VARCHAR, subarrayfield DATE[])[]",
    }

def test_duckdb_data_contract_custom_error_details(nested_all_string_parquet_w_errors,
                                                  nested_parquet_custom_dc_err_details):
    parquet_uri, contract_meta, _ = nested_all_string_parquet_w_errors
    connection = default_connection
    data_contract = DuckDBDataContract(connection)

    entity = data_contract.read_parquet(path=parquet_uri)
    assert entity.count("*").fetchone()[0] == 2
   
    # check processes entity
    contract_dict = json.loads(contract_meta).get("contract")
    entities: Dict[str, DuckDBPyRelation] = {
        "nested_model": entity,
    }
    
    with open(nested_parquet_custom_dc_err_details) as err_dets:
        custom_error_details = json.load(err_dets)

    dc_meta = DataContractMetadata(
        reader_metadata={
            "nested_model": {
                ".xml": ReaderConfig(
                    **contract_dict.get("datasets", {})
                    .get("nested_model", {})
                    .get("reader_config", {})
                    .get(".xml")
                )
            }
        },
        validators={
            "nested_model": RowValidator(contract_dict,
                                         "nested_model",
                                         error_info=custom_error_details)
        },
        reporting_fields={"nested_model": ["id"]},
    )

    entities, messages, stage_successful = data_contract.apply_data_contract(entities, dc_meta)
    assert stage_successful
    assert len(messages) == 2
    messages = sorted(messages, key= lambda x: x.error_code)
    assert messages[0].error_code == "SUBFIELDTESTIDBAD"
    assert messages[0].error_message == "subfield id is invalid: subfield.id - WRONG"
    assert messages[1].error_code == "TESTIDBAD"
    assert messages[1].error_message == "id is invalid: id - WRONG"