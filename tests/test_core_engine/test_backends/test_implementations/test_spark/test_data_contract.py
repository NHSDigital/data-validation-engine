import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dve.core_engine.backends.implementations.spark.contract import SparkDataContract
from dve.core_engine.backends.metadata.contract import DataContractMetadata, ReaderConfig
from dve.core_engine.type_hints import URI
from dve.core_engine.validation import RowValidator
from tests.test_core_engine.test_backends.fixtures import (
    nested_all_string_parquet,
    nested_all_string_parquet_w_errors,
    simple_all_string_parquet,
    nested_parquet_custom_dc_err_details
)


def test_spark_data_contract_read_and_write_basic_parquet(
    simple_all_string_parquet: Tuple[URI, str, List[Dict[str, Any]]],
):
    # can we read in a stringified parquet and run the data contract on it?
    # basic file - simple data structures
    parquet_uri, contract_meta, _ = simple_all_string_parquet
    data_contract = SparkDataContract()
    # check can read
    entity = data_contract.read_parquet(path=parquet_uri)
    assert entity.count() == 2
    assert entity.schema == StructType(
        [
            StructField("id", StringType()),
            StructField("datefield", StringType()),
            StructField("strfield", StringType()),
            StructField("datetimefield", StringType()),
        ]
    )
    # check processes entity
    contract_dict = json.loads(contract_meta).get("contract")
    entities: Dict[str, DataFrame] = {
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
    assert entities["simple_model"].count() == 2
    # check writes entity to parquet
    output_path: Path = Path(parquet_uri).parent.joinpath("simple_model_output.parquet")
    data_contract.write_parquet(
        entity=entities["simple_model"], target_location=output_path.as_posix()
    )
    assert output_path.exists()
    # check when read back in what is expected
    check = data_contract.read_parquet(path=output_path.as_posix())
    assert check.count() == 2
    assert check.schema == StructType(
        [
            StructField("id", LongType()),
            StructField("datefield", DateType()),
            StructField("strfield", StringType()),
            StructField("datetimefield", TimestampType()),
        ]
    )


def test_spark_data_contract_read_nested_parquet(nested_all_string_parquet):
    # can we read in a stringified parquet and run the data contract on it?
    # more complex file - nested, arrays of structs
    parquet_uri, contract_meta, _ = nested_all_string_parquet
    data_contract = SparkDataContract()
    # check can read
    entity = data_contract.read_parquet(path=parquet_uri)
    assert entity.count() == 2
    assert entity.schema == StructType(
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
    )
    # check processes entity
    contract_dict = json.loads(contract_meta).get("contract")
    entities: Dict[str, DataFrame] = {
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
    assert entities["nested_model"].count() == 2
    # check writes entity to parquet
    output_path: Path = Path(parquet_uri).parent.joinpath("nested_model_output.parquet")
    data_contract.write_parquet(
        entity=entities["nested_model"], target_location=output_path.as_posix()
    )
    assert output_path.exists()
    # check when read back in what is expected
    check = data_contract.read_parquet(path=output_path.as_posix())
    assert check.count() == 2
    assert check.schema == StructType(
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
    )

def test_spark_data_contract_custom_error_details(nested_all_string_parquet_w_errors,
                                                  nested_parquet_custom_dc_err_details):
    parquet_uri, contract_meta, _ = nested_all_string_parquet_w_errors
    data_contract = SparkDataContract()

    entity = data_contract.read_parquet(path=parquet_uri)
    assert entity.count() == 2
    assert entity.schema == StructType(
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
    )
    # check processes entity
    contract_dict = json.loads(contract_meta).get("contract")
    entities: Dict[str, DataFrame] = {
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

   