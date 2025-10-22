"""Utilities to be used with services to abstract away some of the config loading and threading"""

import json
from threading import Lock
from typing import Optional, Union

from pydantic.main import ModelMetaclass
from pyspark.sql import SparkSession

import dve.parser.file_handling as fh
from dve.core_engine.backends.base.reader import BaseFileReader
from dve.core_engine.backends.readers.csv import CSVFileReader
from dve.core_engine.backends.readers.xml import BasicXMLFileReader, XMLStreamReader
from dve.core_engine.configuration.v1 import SchemaName, V1EngineConfig, _ModelConfig
from dve.core_engine.type_hints import URI, SubmissionResult
from dve.metadata_parser.model_generator import JSONtoPyd

Dataset = dict[SchemaName, _ModelConfig]
_configs: dict[str, tuple[dict[str, ModelMetaclass], V1EngineConfig, Dataset]] = {}
locks = Lock()
reader_lock = Lock()
_readers: dict[tuple[str, str, str], BaseFileReader] = {}

reader_map = {
    "BasicXMLFileReader": BasicXMLFileReader,
    "CSVFileReader": CSVFileReader,
    "XMLStreamReader": XMLStreamReader,
}


def load_config(
    dataset_id: str,
    file_uri: URI,
) -> tuple[dict[SchemaName, ModelMetaclass], V1EngineConfig, dict[SchemaName, _ModelConfig]]:
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


def load_reader(dataset: Dataset, model_name: str, dataset_id: str, file_extension: str):
    """Loads the readers for the diven feed, model name and file extension"""
    key = (dataset_id, model_name, file_extension)
    if key in _readers:
        return _readers[key]
    reader_config = dataset[model_name].reader_config[f".{file_extension}"]
    reader = reader_map[reader_config.reader](**reader_config.kwargs_)
    with reader_lock:
        _readers[key] = reader
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
