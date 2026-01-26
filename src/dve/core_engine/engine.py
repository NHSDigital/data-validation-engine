"""The core engine for the data validation engine."""

import csv
import json
import logging
import shutil
from contextlib import ExitStack
from pathlib import Path
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import Any, Optional, Union

from pydantic import BaseModel, Field, PrivateAttr, validate_arguments, validator
from pydantic.types import FilePath
from pyspark.sql import SparkSession

from dve.core_engine.backends.base.backend import BaseBackend
from dve.core_engine.backends.implementations.spark.backend import SparkBackend
from dve.core_engine.backends.implementations.spark.types import SparkEntities
from dve.core_engine.configuration.base import BaseEngineConfig
from dve.core_engine.configuration.v1 import V1EngineConfig
from dve.core_engine.constants import ROWID_COLUMN_NAME
from dve.core_engine.loggers import get_child_logger, get_logger
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.models import EngineRunValidation, SubmissionInfo
from dve.core_engine.type_hints import EntityName, JSONstring, Messages
from dve.parser.file_handling import (
    TemporaryPrefix,
    get_resource_exists,
    joinuri,
    open_stream,
    resolve_location,
)
from dve.parser.type_hints import URI, Location


class CoreEngine(BaseModel):
    """The core engine implementation for the data validation engine."""

    class Config:  # pylint: disable=too-few-public-methods
        """`pydantic` configuration options."""

        arbitrary_types_allowed = True
        validate_assignment = True

    backend_config: BaseEngineConfig
    """The backend configuration for the given run."""
    dataset_config_uri: URI
    """The dischema location for the current run"""
    output_prefix_uri: URI = Field(default_factory=lambda: Path("outputs").resolve().as_posix())
    """The prefix for the parquet outputs."""
    main_log: logging.Logger = Field(default_factory=lambda: get_logger("CoreEngine"))
    """The `logging.Logger instance for the data ingest process."""
    cache_prefix_uri: Optional[URI] = None
    """
    An optional cache prefix URI. If not provided, a local temporary directory will
    be used instead (this will not play nicely in Databricks).

    """
    _cache_dir: Optional[TemporaryPrefix] = PrivateAttr(default=None)
    """
    The `TemporaryPrefix` indicating the cache dir.

    Data will be chunked to parquet in this directory after being read,
    and written here before filters are applied.

    """
    backend: BaseBackend = None  # type: ignore
    """The backend to use to process the files."""
    debug: bool = False
    """Indication of if this run is in debug mode."""

    @validator("cache_prefix_uri", "output_prefix_uri", allow_reuse=True, pre=True)
    # pylint: disable=E0213
    def _validate_prefix_uri(cls, location: Optional[Location]) -> Optional[URI]:
        """Ensure we support the cache prefix scheme."""
        if location is None:
            return None
        return resolve_location(location)

    def __init__(self, *args, **kwargs):
        # pylint: disable=W0235
        super().__init__(*args, **kwargs)

    @validator("backend", always=True)
    @classmethod
    def _ensure_backend(cls, value: Optional[BaseBackend], values: dict[str, Any]) -> BaseBackend:
        """Ensure a default backend is created if a backend is not specified."""
        if value is not None:
            return value

        main_logger = values.get("main_log")
        if main_logger is None:
            return SparkBackend(dataset_config_uri=values.get("dataset_config_uri"))
        return SparkBackend(
            dataset_config_uri=values.get("dataset_config_uri"),
            logger=get_child_logger(
                ".".join((SparkBackend.__module__, SparkBackend.__name__)), main_logger
            ),
        )

    @classmethod
    @validate_arguments(config={"arbitrary_types_allowed": True})
    def build(
        cls,
        dataset_config_path: Union[FilePath, URI],
        output_prefix: Location = Path("./outputs"),
        cache_prefix: Optional[Location] = None,
        parent_logger: Optional[logging.Logger] = None,
        debug: Optional[bool] = False,
        **kwargs,
    ):
        """Build an engine from serialised definitions.

        Args:
         - `dataset_config_path`: a URI or path indicating the location of the
           dataset configuration.
         - `output_prefix`: the prefix for parquet outputs (a URI or a local path).
         - `cache_prefix`: the prefix for caching (a URI or a local path).
         - `parent_logger`: an optional parent logger for the engine.
         - `debug`: whether to run in debug mode (default: False).

        """
        if parent_logger:
            main_log = get_child_logger(cls.__name__, parent_logger)
        else:
            main_log = get_logger(cls.__name__)
        main_log.info("Initialising...")
        main_log.info(f"Debug mode: {debug}")

        if isinstance(dataset_config_path, Path):
            dataset_config_uri = dataset_config_path.resolve().as_posix()
        else:
            dataset_config_uri = dataset_config_path
        if isinstance(output_prefix, Path):
            output_prefix_uri = output_prefix.resolve().as_posix()
        else:
            output_prefix_uri = output_prefix
        
        backend_config = V1EngineConfig.load(dataset_config_uri)

        self = cls(
            dataset_config_uri=dataset_config_uri,
            output_prefix_uri=output_prefix_uri,
            main_log=main_log,
            cache_prefix_uri=cache_prefix,
            backend_config=backend_config,
            debug=debug,
            **kwargs,
        )
        self.main_log.info(f"Output path: {self.output_prefix_uri!r}")
        return self

    @classmethod
    def build_from_model(cls, model_str: JSONstring):
        """Build an engine from a serialised JSON pydantic model of definitions.

        Args:
         - `dataset_config_path`: a URI or path indicating the location of the
           dataset configuration.
         - `output_prefix`: the prefix for parquet outputs (a URI or a local path).

        """
        main_log = get_logger("CoreEngine")
        main_log.info("Initalise from model...")
        return cls.build(**EngineRunValidation(**json.loads(model_str)).dict())

    def __enter__(self) -> "CoreEngine":
        self.main_log.info("Entering pipeline context.")
        if self._cache_dir is not None:
            raise ValueError("Pipeline already within context")

        self._cache_dir = TemporaryPrefix(self.cache_prefix_uri)
        self._cache_dir.__enter__()
        self.main_log.info(f"Pipeline will cache to {self.cache_prefix!r}")
        return self

    def __exit__(
        self,
        exc_type: Optional[type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ) -> None:
        self.main_log.info(f"Exiting pipeline context, clearing {self.cache_prefix!r}")
        cache_dir = self._cache_dir
        self._cache_dir = None

        if cache_dir is not None:
            cache_dir.__exit__(exc_type, exc_value, traceback)

        self.main_log.info("Cleared cache.")

    @property
    def cache_prefix(self) -> URI:
        """The cache directory for the pipeline run."""
        if self._cache_dir is None:
            raise ValueError(
                "`cache_prefix` is undefined when the pipeline is not being used as a "
                + "context manager"
            )
        return self._cache_dir.prefix

    def _write_entity_outputs(self, entities: SparkEntities) -> SparkEntities:
        """Write the final entities to the output prefix as Parquet.

        This will result in a directory of files for each entity, containing
        parquet files for each partition in the entity.

        """
        output_entities = {}

        self.main_log.info(f"Writing entities to the output location: {self.output_prefix_uri}")
        for entity_name, entity in entities.items():
            entity = entity.drop(ROWID_COLUMN_NAME)

            self.main_log.info(f"Entity: {entity_name} {type(entity)}")

            output_uri = joinuri(self.output_prefix_uri, entity_name)
            if get_resource_exists(output_uri):
                self.main_log.info(f"{output_uri} already exists - will be overwritten")

            self.main_log.info(f"+ Writing parquet output to {output_uri!r}")
            entity.write.mode("overwrite").parquet(output_uri)
            spark_session = SparkSession.builder.getOrCreate()
            output_entities[entity_name] = spark_session.read.format("parquet").load(
                output_uri, schema=entity.schema
            )

        return output_entities

    def _write_outputs(
        self, entities: SparkEntities
    ) -> SparkEntities:
        """Write the outputs from the pipeline, returning the written entities
        and messages.

        """
        entities = self._write_entity_outputs(entities)

        return entities

    def _show_available_entities(self, entities: SparkEntities, *, verbose: bool = False) -> None:
        """Print current entities."""
        self.main_log.info("Displaying available dataframes in this run:")

        for entity_name, entity in entities.items():
            # FIXME: Currently a print statement because log messages
            #  can arrive out of sequence with the df.show()
            if self.debug:
                print(f"+ Entity dataframe: {entity_name} has {entity.count()} rows")
            else:
                print(f"+ Entity dataframe: {entity_name}")

            if verbose:
                # Cap the number of rows displayed to reduce probs with max log size on dbr
                entity.show(n=10, truncate=False)

    def run_pipeline(
        self,
        entity_locations: dict[EntityName, URI],
        # pylint: disable=unused-argument
        submission_info: Optional[SubmissionInfo] = None,
    ) -> tuple[SparkEntities, URI]:
        """Run the pipeline, reading in the entities and applying validation
        and transformation rules, and then write the outputs.

        The returned entities will reference the output locations, so
        references should be valid after the pipeline context exits.

        """
        entities, errors_uri = self.backend.process_legacy(
            self.output_prefix_uri,
            entity_locations,
            self.backend_config.get_contract_metadata(),
            self.backend_config.get_rule_metadata(),
            submission_info,
        )
        return self._write_outputs(entities), errors_uri
