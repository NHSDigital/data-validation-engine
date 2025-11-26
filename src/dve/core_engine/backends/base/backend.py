"""A complete backend implementation."""

import logging
import warnings
from abc import ABC, abstractmethod
from collections.abc import Mapping, MutableMapping
from typing import Any, ClassVar, Generic, Optional

from pyspark.sql import DataFrame, SparkSession

from dve.core_engine.backends.base.contract import BaseDataContract
from dve.core_engine.backends.base.core import EntityManager, get_entity_type
from dve.core_engine.backends.base.reference_data import BaseRefDataLoader, ReferenceConfigUnion
from dve.core_engine.backends.base.rules import BaseStepImplementations
from dve.core_engine.backends.metadata.contract import DataContractMetadata
from dve.core_engine.backends.metadata.rules import RuleMetadata
from dve.core_engine.backends.types import Entities, EntityType, StageSuccessful
from dve.core_engine.loggers import get_logger
from dve.core_engine.models import SubmissionInfo
from dve.core_engine.type_hints import (
    URI,
    EntityLocations,
    EntityName,
    EntityParquetLocations,
    Messages,
)


class BaseBackend(Generic[EntityType], ABC):
    """A complete implementation of a backend."""

    __entity_type__: ClassVar[type[EntityType]]  # type: ignore
    """
    The entity type used within the backend.

    This will be populated from the generic annotation at class creation time.

    """

    def __init_subclass__(cls, *_, **__) -> None:
        # Set entity type from parent class subscript.
        if cls is not BaseBackend:
            cls.__entity_type__ = get_entity_type(cls, "BaseBackend")

    def __init__(  # pylint: disable=unused-argument
        self,
        contract: BaseDataContract[EntityType],
        steps: BaseStepImplementations[EntityType],
        reference_data_loader_type: Optional[type[BaseRefDataLoader[EntityType]]],
        logger: Optional[logging.Logger] = None,
        **kwargs: Any,
    ) -> None:
        for component_name, component in (
            ("Contract", contract),
            ("Step implementation", steps),
            ("Reference data loader", reference_data_loader_type),
        ):
            component_entity_type = getattr(component, "__entity_type__", None)
            if component_entity_type != self.__entity_type__:
                raise TypeError(
                    f"{component_name} entity type ({component_entity_type}) does not match "
                    + f"the type expected by this backend ({self.__entity_type__})"
                )

        self.contract = contract
        """The data contract implementation used by the backend."""
        self.step_implementations = steps
        """The step implementations used by the backend."""
        self.reference_data_loader_type = reference_data_loader_type
        """
        The loader type to use for the reference data. If `None`, do not
        load any reference data and error if it is provided.

        """
        self.logger = logger or get_logger(type(self).__name__)
        """The `logging.Logger instance for the backend."""

    def load_reference_data(
        self,
        reference_entity_config: dict[EntityName, ReferenceConfigUnion],
        submission_info: Optional[SubmissionInfo],
    ) -> Mapping[EntityName, EntityType]:
        """Load the reference data as specified in the reference entity config."""
        sub_info_entity: Optional[EntityType] = None
        if submission_info:
            sub_info_entity = self.convert_submission_info(submission_info)

        if self.reference_data_loader_type is None:
            if reference_entity_config:
                raise ValueError(
                    "Reference data has been specified but no reference data loader is "
                    + "configured for this backend"
                )

            reference_data_dict = {}
            if sub_info_entity is not None:
                reference_data_dict["dve_submission_info"] = sub_info_entity
            return reference_data_dict

        reference_data_loader = self.reference_data_loader_type(reference_entity_config)
        if sub_info_entity is not None:
            reference_data_loader.entity_cache["dve_submission_info"] = sub_info_entity

        return reference_data_loader

    @abstractmethod
    def convert_submission_info(self, submission_info: SubmissionInfo) -> EntityType:
        """Convert the submission info to an entity."""

    @abstractmethod
    def write_entities_to_parquet(
        self, entities: Entities, cache_prefix: URI
    ) -> EntityParquetLocations:
        """Write entities out to parquet, returning the locations."""
        raise NotImplementedError()

    def convert_entities_to_spark(
        self, entities: Entities, cache_prefix: URI, _emit_deprecation_warning: bool = True
    ) -> dict[EntityName, DataFrame]:
        """Convert entities to Spark DataFrames.

        Entities may be omitted if they are blank, because Spark cannot create an
        entity from an empty parquet file.

        """
        if _emit_deprecation_warning:
            self.logger.warning("DEPRECATED: Converting entities to Spark is deprecated")
            warnings.warn(
                "Converting entities to Spark is deprecated, and may be removed if the core engine "
                + "changes the internal representation",
                category=DeprecationWarning,
            )

        parquet_locations = self.write_entities_to_parquet(entities, cache_prefix)
        spark_session = SparkSession.builder.getOrCreate()

        spark_entities = {}
        for entity_name, parquet_location in parquet_locations.items():
            try:
                spark_entities[entity_name] = spark_session.read.parquet(parquet_location)
            except Exception as err:  # pylint: disable=broad-except
                self.logger.warning(
                    f"Failed to read entity {entity_name!r} back from parquet location "
                    + repr(parquet_location)
                )
                self.logger.exception(err)
        return spark_entities

    def apply(
        self,
        entity_locations: EntityLocations,
        contract_metadata: DataContractMetadata,
        rule_metadata: RuleMetadata,
        submission_info: Optional[SubmissionInfo] = None,
    ) -> tuple[Entities, Messages, StageSuccessful]:
        """Apply the data contract and the rules, returning the entities and all
        generated messages.

        """
        reference_data = self.load_reference_data(
            rule_metadata.reference_data_config, submission_info
        )
        entities, messages, successful = self.contract.apply(entity_locations, contract_metadata)
        if not successful:
            return entities, messages, successful

        for entity_name, entity in entities.items():
            entities[entity_name] = self.step_implementations.add_row_id(entity)

        # TODO: Handle entity manager creation errors.
        entity_manager = EntityManager(entities, reference_data)
        # TODO: Add stage success to 'apply_rules'
        rule_messages = self.step_implementations.apply_rules(entity_manager, rule_metadata)
        messages.extend(rule_messages)

        for entity_name, entity in entity_manager.entities.items():
            entity_manager.entities[entity_name] = self.step_implementations.drop_row_id(entity)

        return entity_manager.entities, messages, True

    def process(
        self,
        entity_locations: EntityLocations,
        contract_metadata: DataContractMetadata,
        rule_metadata: RuleMetadata,
        cache_prefix: URI,
        submission_info: Optional[SubmissionInfo] = None,
    ) -> tuple[MutableMapping[EntityName, URI], Messages]:
        """Apply the data contract and the rules, write the entities out to parquet
        and returning the entity locations and all generated messages.

        """
        entities, messages, successful = self.apply(
            entity_locations, contract_metadata, rule_metadata, submission_info
        )
        if successful:
            parquet_locations = self.write_entities_to_parquet(entities, cache_prefix)
        else:
            parquet_locations = {}
        return parquet_locations, messages

    def process_legacy(
        self,
        entity_locations: EntityLocations,
        contract_metadata: DataContractMetadata,
        rule_metadata: RuleMetadata,
        cache_prefix: URI,
        submission_info: Optional[SubmissionInfo] = None,
    ) -> tuple[MutableMapping[EntityName, DataFrame], Messages]:
        """Apply the data contract and the rules, create Spark `DataFrame`s from the
        entities and return the Spark entities and all generated messages.

        Entities may be omitted if they are blank, because Spark cannot create an
        entity from an empty parquet file.

        """
        self.logger.warning("DEPRECATED: Processing entities to Spark is deprecated")
        warnings.warn(
            "Converting entities to Spark is deprecated, and may be removed if the core engine "
            + "changes the internal representation",
            category=DeprecationWarning,
        )

        entities, messages, successful = self.apply(
            entity_locations, contract_metadata, rule_metadata, submission_info
        )

        if not successful:
            return {}, messages

        if self.__entity_type__ == DataFrame:
            return entities, messages  # type: ignore

        return (
            self.convert_entities_to_spark(entities, cache_prefix, _emit_deprecation_warning=False),
            messages,
        )
