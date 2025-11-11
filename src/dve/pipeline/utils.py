"""Utilities to be used with services to abstract away some of the config loading and threading"""
import json
from threading import Lock
from typing import Dict, List, Optional, Tuple, Union

from pydantic.main import ModelMetaclass
from pyspark.sql import SparkSession

import dve.core_engine.backends.implementations.duckdb  # pylint: disable=unused-import
import dve.core_engine.backends.implementations.spark  # pylint: disable=unused-import
import dve.parser.file_handling as fh
from dve.core_engine.backends.readers import _READER_REGISTRY
from dve.core_engine.configuration.v1 import SchemaName, V1EngineConfig, _ModelConfig
from dve.core_engine.type_hints import URI, Messages, SubmissionResult
from dve.metadata_parser.model_generator import JSONtoPyd
from dve.reporting.error_report import conditional_cast

Dataset = Dict[SchemaName, _ModelConfig]
_configs: Dict[str, Tuple[Dict[str, ModelMetaclass], V1EngineConfig, Dataset]] = {}
locks = Lock()


def load_config(
    dataset_id: str,
    file_uri: URI,
) -> Tuple[Dict[SchemaName, ModelMetaclass], V1EngineConfig, Dict[SchemaName, _ModelConfig]]:
    """Loads the configuration for a given dataset"""
    if dataset_id in _configs:
        return _configs[dataset_id]

    with fh.open_stream(file_uri) as f:
        generator = JSONtoPyd(json.load(f)["contract"])

    models = generator.generate_models()
    config = V1EngineConfig.load(file_uri)
    dataset = config.contract.datasets

    with locks:
        _configs[dataset_id] = models, config, dataset

    return models, config, dataset


def load_reader(dataset: Dataset, model_name: str, file_extension: str):
    """Loads the readers for the diven feed, model name and file extension"""
    reader_config = dataset[model_name].reader_config[f".{file_extension}"]
    reader = _READER_REGISTRY[reader_config.reader](**reader_config.kwargs_)
    return reader


def unpersist_all_rdds(spark: SparkSession):
    """Unpersist any checkpointed or cached rdds to avoid memory leaks"""
    for (
        _,
        rdd,
    ) in spark.sparkContext._jsc.getPersistentRDDs().items():  # type: ignore # pylint: disable=protected-access
        rdd.unpersist()


def deadletter_file(source_uri: URI) -> None:
    """Move files that can't be processed to a deadletter location"""
    try:
        source_parent: URI = source_uri.rsplit("/", 1)[0]
        deadletter_path: URI = fh.joinuri(source_parent.rsplit("/", 1)[0], "deadletter")
        target_uri = fh.joinuri(deadletter_path, fh.get_file_name(source_uri))
        return fh.move_resource(source_uri, target_uri)
    except TypeError:
        return None

def dump_errors(
    working_folder: URI,
    step_name: str,
    messages: Messages,
    key_fields: Optional[Dict[str, List[str]]] = None,
):
    if not working_folder:
        raise AttributeError("processed files path not passed")

    if not key_fields:
        key_fields = {}

    errors = fh.joinuri(
        working_folder, "errors", f"{step_name}_errors.json"
    )
    processed = []

    for message in messages:
        primary_keys: List[str] = key_fields.get(message.entity if message.entity else "", [])
        error = message.to_dict(
            key_field=primary_keys,
            value_separator=" -- ",
            max_number_of_values=10,
            record_converter=None,
        )
        error["Key"] = conditional_cast(error["Key"], primary_keys, value_separator=" -- ")
        processed.append(error)

    with fh.open_stream(errors, "a+") as f:
        json.dump(
            processed,
            f,
            default=str,
        )

class SubmissionStatus:
    """Submission status for a given submission."""

    # _logger = get_logger("submission_status")

    def __init__(self, failed: bool, number_of_records: Optional[int] = None):
        self._failed = failed
        self._number_of_records = number_of_records

    @property
    def failed(self):
        """Whether the submission was successfully processed."""
        return self._failed

    @property
    def number_of_records(self) -> Union[int, None]:
        """Number of records within a submission."""
        return self._number_of_records

    @property
    def submission_result(self) -> SubmissionResult:
        """The result of the submission, either succes, failed or failed_xml_generation"""
        if self.failed:
            return "failed"
        return "success"
