"""The base implementation of the reference data loader.."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator, Mapping
from typing import ClassVar, Generic, Optional, Union, get_type_hints

from pydantic import BaseModel, Field
from typing_extensions import Annotated, Literal

import dve.parser.file_handling as fh
from dve.core_engine.backends.base.core import get_entity_type
from dve.core_engine.backends.exceptions import (
    MissingRefDataEntity,
    RefdataLacksFileExtensionSupport,
)
from dve.core_engine.backends.types import EntityType
from dve.core_engine.type_hints import URI, EntityName
from dve.parser.file_handling.implementations.file import LocalFilesystemImplementation
from dve.parser.file_handling.service import _get_implementation

_FILE_EXTENSION_NAME: str = "_REFDATA_FILE_EXTENSION"
"""Name of attribute added to methods where they relate
   to loading a particular reference file type."""


def mark_refdata_file_extension(file_extension):
    """Mark a method for loading a particular file extension"""

    def wrapper(func: Callable):
        setattr(func, _FILE_EXTENSION_NAME, file_extension)
        return func

    return wrapper


class ReferenceTable(BaseModel, frozen=True):
    """Configuration for a reference data object when table_name."""

    type: Literal["table"]
    """The object type."""
    table_name: str
    """Name of the table where the data persists."""
    database: Optional[str] = None
    """Name of the database where the reference data is located."""

    @property
    def fq_table_name(self):
        """The fully qualified table name"""
        if self.database:
            return f"{self.database}.{self.table_name}"
        return self.table_name


class ReferenceFile(BaseModel, frozen=True):
    """Configuration for a reference data object when a file."""

    type: Literal["filename"]
    """The object type."""
    filename: str
    """The path to the reference data relative to the contract."""

    @property
    def file_extension(self) -> str:
        """The file extension of the reference file"""
        return fh.get_file_suffix(self.filename)  # type: ignore


class ReferenceURI(BaseModel, frozen=True):
    """Configuration for a reference data object when a URI."""

    type: Literal["uri"]
    """The object type."""
    uri: str
    """The absolute URI of the reference data (as Parquet)."""

    @property
    def file_extension(self) -> str:
        """The file extension of the reference uri"""
        return fh.get_file_suffix(self.uri)  # type: ignore


ReferenceConfig = Union[ReferenceFile, ReferenceTable, ReferenceURI]
"""The config utilised to load the reference data"""

ReferenceConfigUnion = Annotated[ReferenceConfig, Field(discriminator="type")]
"""Discriminated union to determine refdata config from supplied type"""


class BaseRefDataLoader(Generic[EntityType], Mapping[EntityName, EntityType], ABC):
    """A reference data mapper which lazy-loads requested entities."""

    __entity_type__: ClassVar[type[EntityType]]  # type: ignore
    """
    The entity type used for the reference data.

    This will be populated from the generic annotation at class creation time.

    """
    __step_functions__: ClassVar[dict[type[ReferenceConfig], Callable]] = {}
    """
    A mapping between refdata config types and functions to call to load these configs
    into reference data entities
    """

    __reader_functions__: ClassVar[dict[str, Callable]] = {}
    """
    A mapping between file extensions and functions to load the file uris
    into reference data entities
    """
    prefix: str = "refdata_"

    def __init_subclass__(cls, *_, **__) -> None:
        """When this class is subclassed, create and populate the `__step_functions__`
        class variable for the subclass.

        """
        # Set entity type from parent class subscript.
        if cls is not BaseRefDataLoader:
            cls.__entity_type__ = get_entity_type(cls, "BaseRefDataLoader")

        # ensure that dicts are specific to each subclass - redefine rather
        # than keep the same reference
        cls.__reader_functions__ = {}
        cls.__step_functions__ = {}

        for method_name in dir(cls):
            if method_name.startswith("_"):
                continue

            method = getattr(cls, method_name, None)
            if method is None or not callable(method):
                continue

            if ext := getattr(method, _FILE_EXTENSION_NAME, None):
                cls.__reader_functions__[ext] = method
                continue

            type_hints = get_type_hints(method)
            if set(type_hints.keys()) != {"config", "return"}:
                continue
            config_type = type_hints["config"]
            if not issubclass(config_type, BaseModel):
                continue

            cls.__step_functions__[config_type] = method  # type: ignore

    # pylint: disable=unused-argument
    def __init__(
        self,
        reference_entity_config: dict[EntityName, ReferenceConfig],
        dataset_config_uri: Optional[URI] = None,
        **kwargs,
    ) -> None:
        self.reference_entity_config = reference_entity_config
        self.dataset_config_uri = dataset_config_uri
        """
        Configuration options for the reference data. This is likely to vary
        from backend to backend (e.g. might be locations and file types for
        some backends, and table names for others).

        """
        self.entity_cache: dict[EntityName, EntityType] = {}
        """A cache for already-loaded entities."""

    @abstractmethod
    def load_table(self, config: ReferenceTable) -> EntityType:
        """Load reference entity from a database table"""
        raise NotImplementedError()

    def load_file(self, config: ReferenceFile) -> EntityType:
        "Load reference entity from a relative file path"
        if not self.dataset_config_uri:
            raise AttributeError("dataset_config_uri must be specified if using relative paths")
        target_location = fh.build_relative_uri(self.dataset_config_uri, config.filename)
        if isinstance(_get_implementation(self.dataset_config_uri), LocalFilesystemImplementation):
            target_location = fh.file_uri_to_local_path(target_location).as_posix()
        try:
            impl = self.__reader_functions__[config.file_extension]
            return impl(self, target_location)
        except KeyError as exc:
            raise RefdataLacksFileExtensionSupport(file_extension=config.file_extension) from exc

    def load_uri(self, config: ReferenceURI) -> EntityType:
        "Load reference entity from an absolute URI"
        if isinstance(_get_implementation(config.uri), LocalFilesystemImplementation):
            target_location = fh.file_uri_to_local_path(config.uri).as_posix()
        else:
            target_location = config.uri
        try:
            impl = self.__reader_functions__[config.file_extension]
            return impl(self, target_location)
        except KeyError as exc:
            raise RefdataLacksFileExtensionSupport(file_extension=config.file_extension) from exc

    def load_entity(self, entity_name: EntityName, config: ReferenceConfig) -> EntityType:
        """Load a reference entity given the reference config"""
        config_type = type(config)
        func = self.__step_functions__[config_type]
        entity = func(self, config)
        self.entity_cache[entity_name] = entity
        return entity

    def __getitem__(self, key: EntityName) -> EntityType:
        try:
            return self.entity_cache[key]
        except KeyError:
            try:
                config = self.reference_entity_config[key]
                return self.load_entity(entity_name=key, config=config)
            except Exception as err:
                raise MissingRefDataEntity(entity_name=key) from err

    def __iter__(self) -> Iterator[str]:
        return iter(self.reference_entity_config.keys())

    def __len__(self) -> int:
        return len(self.reference_entity_config)
