"""The base engine configuration format."""

import json
from abc import ABC, abstractmethod
from typing import Any, TypeVar

from pydantic import BaseModel

from dve.core_engine.backends.metadata.contract import DataContractMetadata
from dve.core_engine.backends.metadata.rules import RuleMetadata
from dve.core_engine.type_hints import EntityName
from dve.parser.file_handling import open_stream
from dve.parser.type_hints import URI

CSelf = TypeVar("CSelf", bound="BaseEngineConfig")
"""The type of the config."""


class BaseEngineConfig(BaseModel, ABC):
    """The base engine configuration."""

    location: URI
    """
    The location of the root config file. All relative file loads required
    to build the metadata/contract should be relative to this.

    """

    @abstractmethod
    def get_rule_metadata(self) -> RuleMetadata:
        """Build the rule metadata from the configuration."""

    @abstractmethod
    def get_contract_metadata(self) -> DataContractMetadata:
        """Build the contract metadata from the configuration."""

    @abstractmethod
    def get_reference_data_config(self) -> dict[EntityName, dict[str, Any]]:
        """Get the configuration info for the reference data.

        This will likely be backend dependent, and should be a dict mapping reference
        dataset name (without e.g. the "refdata_" prefic) to the config options for
        that reference dataset (e.g. database and table name).

        """

    @classmethod
    def load(cls: type[CSelf], location: URI) -> CSelf:
        """Load an instance of the config from the URI."""
        with open_stream(location) as config_stream:
            json_config = json.load(config_stream)

        if not isinstance(json_config, dict):
            raise TypeError("JSON contract config must contain mapping in root")

        return cls(location=location, **json_config)
