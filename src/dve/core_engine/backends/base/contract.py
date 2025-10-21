"""Base implementation of the data contract."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from inspect import ismethod
from typing import Any, ClassVar, Generic, Optional, TypeVar

from pydantic import BaseModel
from typing_extensions import Protocol

from dve.core_engine.backends.base.core import get_entity_type
from dve.core_engine.backends.base.reader import BaseFileReader
from dve.core_engine.backends.exceptions import ReaderLacksEntityTypeSupport, render_error
from dve.core_engine.backends.metadata.contract import DataContractMetadata
from dve.core_engine.backends.readers import get_reader
from dve.core_engine.backends.types import Entities, EntityType, StageSuccessful
from dve.core_engine.backends.utilities import dedup_messages, stringify_model
from dve.core_engine.loggers import get_logger
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import (
    URI,
    ArbitraryFunction,
    EntityLocations,
    EntityName,
    JSONDict,
    Messages,
    WrapDecorator,
)
from dve.parser.file_handling import get_file_suffix, get_resource_exists
from dve.parser.type_hints import Extension

T = TypeVar("T")
ExtensionConfig = dict[Extension, "ReaderConfig"]
"""Configuration options for file extensions."""
_READER_OVERRIDE_ATTR_NAME = "_implements_reader_for"
"""The name of the reader override function's reader override attribute."""


class ReaderConfig(BaseModel):
    """Configuration options for a given reader."""

    reader: str
    """The name of the reader to be used."""
    parameters: JSONDict
    """The parameters the reader should use."""


class _UnboundReaderOverride(Protocol[T]):  # pylint: disable=too-few-public-methods
    """The protocol required to implement an override for a specific file reader."""

    @staticmethod
    def __call__(  # pylint: disable=bad-staticmethod-argument
        self: "BaseDataContract[T]",  # This is the protocol for an _unbound_ method.
        reader: BaseFileReader,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> T:
        ...


def reader_override(reader_type: type[BaseFileReader]) -> WrapDecorator:
    """A decorator function which wraps a `ReaderProtocol` method to add support
    for custom reader overrides.

    """

    def reader_impl_decorator(func: ArbitraryFunction) -> ArbitraryFunction:
        """Wrap a reader function to indicate the reader type it implements an override
        for.

        """
        setattr(func, _READER_OVERRIDE_ATTR_NAME, reader_type)
        return func

    return reader_impl_decorator


class BaseDataContract(Generic[EntityType], ABC):
    """The base implementation of a data contract."""

    __entity_type__: ClassVar[type[EntityType]]  # type: ignore
    """
    The entity type that should be requested from a reader without a
    specific implementation.

    This will be populated from the generic annotation at class creation time.

    """
    __reader_overrides__: ClassVar[dict[type[BaseFileReader], _UnboundReaderOverride[EntityType]]] = {}  # type: ignore # pylint: disable=line-too-long
    """
    A dictionary mapping implemented reader types to override functions which provide
    a 'local' implementation of the reader. These can provide a more optimised version
    of a specific reader for the implemented backend.

    This is set and populated in `__init_subclass__` by identifying methods
    decorated with the '@reader_override' decorator, and is used in `read_entity_type`.

    """

    def __init_subclass__(cls, *_, **__) -> None:
        """When this class is subclassed, create and populate the `__reader_overrides__`
        and `__entity_type__` class variables for this subclass.

        """
        # Set entity type from parent class subscript.
        if cls is not BaseDataContract:
            cls.__entity_type__ = get_entity_type(cls, "BaseDataContract")

        # Identify provided reader overrides.
        cls.__reader_overrides__ = {}

        for method_name in dir(cls):
            method = getattr(cls, method_name, None)
            if not (ismethod(method) or callable(method)):
                continue

            reader_type = getattr(method, _READER_OVERRIDE_ATTR_NAME, None)
            if reader_type is None:
                continue

            if not (isinstance(reader_type, type) and issubclass(reader_type, BaseFileReader)):
                continue

            cls.__reader_overrides__[reader_type] = method  # type: ignore

    def __init__(  # pylint: disable=unused-argument
        self,
        logger: Optional[logging.Logger] = None,
        **kwargs: Any,
    ):
        self.logger = logger or get_logger(type(self).__name__)
        """The `logging.Logger instance for the data contract config."""

    @abstractmethod
    def create_entity_from_py_iterator(
        self, entity_name: EntityName, records: Iterator[dict[str, Any]], schema: type[BaseModel]
    ) -> EntityType:
        """A fallback function to be used where no entity type specific
        reader implemenattions are available.

        """

    def read_entity_from_py_iterator(
        self,
        reader: BaseFileReader,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> EntityType:
        """A fallback function for readers that should read records with the
        'read_to_py_iterator' implementation and create an entity of the correct
        type.

        This will be used where there are not more specific implementations for a
        given reader type (either as a reader-specific override, or through direct
        support for the contract's entity type in the reader).

        """
        py_iterator = reader.read_to_py_iterator(resource, entity_name, schema)
        return self.create_entity_from_py_iterator(entity_name, py_iterator, schema)

    def read_entity(
        self,
        reader: BaseFileReader,
        resource: URI,
        entity_name: EntityName,
        schema: type[BaseModel],
    ) -> EntityType:
        """Read an entity using the provided reader class.

        NOTE: In the reader, simple types will either be returned as strings (if present)
        or `None`. Format validation, casting, and parsing should be done when the
        contract is applied.

        NOTE 2: The default implementation will stringify schemas before passing them
        to the reader and `create_entity_from_py_iterator`.

        """
        schema = stringify_model(schema)
        try:
            # Try fetching an overridden implementation for the given reader type.
            impl = self.__reader_overrides__[type(reader)]
        except KeyError:
            try:
                # If there is no override, try having the reader read directly to
                # the contract's entity type.
                self.logger.debug("Attempting to read directly to contract entity type...")
                entity = reader.read_to_entity_type(
                    self.__entity_type__, resource, entity_name, schema
                )
                return entity
            except ReaderLacksEntityTypeSupport:
                pass
        else:
            self.logger.debug(f"Using contract-specific override for {type(reader).__name__}...")
            return impl(self, reader, resource, entity_name, schema)

        # Finally, fall back to using the pure Python reader and creating an entity.
        self.logger.debug("Reading via Python iterator...")
        return self.read_entity_from_py_iterator(reader, resource, entity_name, schema)

    def _create_critical_error(
        self, entity_name: EntityName, error_message: str
    ) -> FeedbackMessage:
        """Create a critical data contract error."""
        return FeedbackMessage(
            record=None,
            entity=entity_name,
            failure_type="integrity",
            error_message=error_message,
            error_location="Whole file",
            category="Bad file",
        )

    def _ensure_all_entities_provided(
        self, entity_names: Iterable[str], contract_metadata: DataContractMetadata
    ) -> Messages:
        """Ensure all entities are provided, with no extras."""
        provided_entities = set(entity_names)
        expected_entities = set(contract_metadata.schemas.keys())

        missing_entities = sorted(provided_entities - expected_entities)
        extra_entities = sorted(expected_entities - provided_entities)

        messages: Messages = []

        for entity_name in missing_entities:
            self.logger.error(f"No location specified for {entity_name!r}")
            message = self._create_critical_error(entity_name, "Entity was not provided")
            messages.append(message)

        for entity_name in extra_entities:
            self.logger.error(f"Unrecognised entity provided ({entity_name!r})")
            message = self._create_critical_error(entity_name, "Unrecognised entity name provided")
            messages.append(message)

        return messages

    def _ensure_entity_locations_appropriate(
        self, entity_locations: EntityLocations, contract_metadata: DataContractMetadata
    ) -> Messages:
        """Ensure the provided entity locations really exist."""
        messages: Messages = []
        for entity_name in contract_metadata.schemas:
            try:
                entity_location = entity_locations[entity_name]
            except KeyError:
                continue

            try:
                if not get_resource_exists(entity_location):
                    self.logger.error(
                        f"Resource does not exist for {entity_name!r} (location: "
                        + f"{entity_location!r})"
                    )
                    message = self._create_critical_error(
                        entity_name, "The provided location does not exist"
                    )
                    messages.append(message)
            except Exception as err:  # pylint: disable=broad-except
                self.logger.error(
                    f"Error checking location exists for {entity_name!r} (location: "
                    + f"{entity_location!r})"
                )
                self.logger.exception(err)
                error_message = (
                    f"Unable to ensure entity location exists ({type(err).__name__}: {err})"
                )
                message = self._create_critical_error(entity_name, error_message)
                messages.append(message)

        return messages

    def _ensure_entity_locations_have_read_support(
        self, entity_locations: EntityLocations, contract_metadata: DataContractMetadata
    ) -> Messages:
        """Ensure that provided entity locations have supported readers."""
        messages: Messages = []

        for entity_name in contract_metadata.schemas:
            try:
                entity_location = entity_locations[entity_name]
            except KeyError:
                continue

            suffix = get_file_suffix(entity_location) or ""
            if not suffix:
                self.logger.error(
                    f"{entity_name!r} (location: {entity_location!r}) missing file extension"
                )
                message = self._create_critical_error(entity_name, "Missing file extension")
                messages.append(message)

            extension = f".{suffix}"
            if extension not in contract_metadata.reader_metadata[entity_name]:
                self.logger.error(
                    f"{entity_name!r} (location: {entity_location!r}) does not have configured "
                    + f"reader for {extension} files"
                )
                error_message = f"Does not implement support for {extension!r} types"
                message = self._create_critical_error(entity_name, error_message)

        return messages

    def read_raw_entities(
        self, entity_locations: EntityLocations, contract_metadata: DataContractMetadata
    ) -> tuple[Entities, Messages, StageSuccessful]:
        """Read the raw entities from the entity locations using the configured readers.

        These will not yet have had the data contracts applied.

        """
        messages: Messages = []
        messages.extend(self._ensure_all_entities_provided(entity_locations, contract_metadata))
        messages.extend(
            self._ensure_entity_locations_appropriate(entity_locations, contract_metadata)
        )
        messages.extend(
            self._ensure_entity_locations_have_read_support(entity_locations, contract_metadata)
        )
        if any(message.is_critical for message in messages):
            return {}, messages, False

        entities: Entities = {}
        successful = True
        for entity_name, resource in entity_locations.items():
            reader_metadata = contract_metadata.reader_metadata[entity_name]
            extension = "." + (
                get_file_suffix(resource) or ""
            )  # Already checked that extension supported.

            reader_config = reader_metadata[extension]
            reader_type = get_reader(reader_config.reader)
            reader = reader_type(**reader_config.parameters)

            self.logger.info(f"Reading entity {entity_name!r} using {reader_config.reader!r}")
            try:
                schema = contract_metadata.schemas[entity_name]
                entities[entity_name] = self.read_entity(
                    reader,
                    resource,
                    entity_name,
                    schema,  # type: ignore
                )
            except Exception as err:  # pylint: disable=broad-except
                successful = False
                location = f"data contract (reading entity {entity_name!r} from {resource!r})"
                new_messages = render_error(
                    err,
                    location,
                    self.logger,
                    entity_name=entity_name,
                    error_location="Whole file",
                    error_category="Bad file",
                )
                messages.extend(new_messages)

        return entities, dedup_messages(messages), successful

    @abstractmethod
    def apply_data_contract(
        self, entities: Entities, contract_metadata: DataContractMetadata
    ) -> tuple[Entities, Messages, StageSuccessful]:
        """Apply the data contract to the raw entities, returning the validated entities
        and any messages.

        Record-level identifiers should be added at this point.

        """
        raise NotImplementedError()

    def apply(
        self, entity_locations: EntityLocations, contract_metadata: DataContractMetadata
    ) -> tuple[Entities, Messages, StageSuccessful]:
        """Read the entities from the provided locations according to the data contract,
        and return the validated entities and any messages.

        """
        entities, messages, successful = self.read_raw_entities(entity_locations, contract_metadata)
        if not successful:
            return {}, messages, successful

        try:
            entities, contract_messages, successful = self.apply_data_contract(
                entities, contract_metadata
            )
            messages.extend(contract_messages)
        except Exception as err:  # pylint: disable=broad-except
            successful = False
            new_messages = render_error(
                err,
                "data contract",
                self.logger,
            )
            messages.extend(new_messages)

        if contract_metadata.cache_originals:
            for entity_name in list(entities):
                entities[f"Original{entity_name}"] = entities[entity_name]

        return entities, messages, successful

    def read_parquet(self, path: URI, **kwargs) -> EntityType:
        """Method to read parquet files from stringified parquet output
        from  file transformation phase.
        """
        raise NotImplementedError()

    def write_parquet(self, entity: EntityType, target_location: URI, **kwargs) -> URI:
        """Method to write parquet files from type cast entities
        following data contract application
        """
        raise NotImplementedError()
