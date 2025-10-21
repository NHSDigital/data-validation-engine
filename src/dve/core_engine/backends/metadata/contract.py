"""Metadata classes for the data contract."""

from typing import Any

from pydantic import BaseModel, PrivateAttr, root_validator

from dve.core_engine.type_hints import EntityName, ReportingFields
from dve.core_engine.validation import RowValidator
from dve.parser.type_hints import Extension


class ReaderConfig(BaseModel):
    """Configuration options for a given reader."""

    reader: str
    """The name of the reader to be used."""
    parameters: dict[str, Any]
    """The parameters the reader should use."""


class DataContractMetadata(BaseModel, frozen=True, arbitrary_types_allowed=True):
    """Metadata for the data contract."""

    reader_metadata: dict[EntityName, dict[Extension, ReaderConfig]]
    """
    The per-entity reader metadata.

    These are left as configurations until data contract application, because
    a given data contract might have a higher-performance implementation for
    the requested reader.

    """
    validators: dict[EntityName, RowValidator]
    """The per-entity record validators."""
    reporting_fields: dict[EntityName, ReportingFields]
    """The per-entity reporting fields."""
    cache_originals: bool = False
    """Whether to cache the original entities after loading."""
    _schemas: dict[EntityName, type[BaseModel]] = PrivateAttr(default_factory=dict)
    """The pydantic models of the schmas."""

    @property
    def schemas(self) -> dict[EntityName, type[BaseModel]]:
        """The per-entity schemas, as pydantic models."""
        if not self._schemas:
            for entity_name, validator in self.validators.items():
                self._schemas[entity_name] = validator.model  # type: ignore
        return self._schemas.copy()

    @root_validator(allow_reuse=True)
    @classmethod
    def _ensure_entities_complete(cls, values: dict[str, dict[EntityName, Any]]):
        """Ensure the entities in 'readers' and 'validators' are the same."""
        try:
            reader_entities = set(values["reader_metadata"].keys())
        except Exception:
            print(values.keys())
            raise
        validator_entities = set(values["validators"].keys())
        missing_reader = validator_entities - reader_entities
        missing_model = reader_entities - validator_entities

        messages = []
        for value_set, category in (
            (missing_reader, "reader specifications"),
            (missing_model, "validation models"),
        ):
            if value_set:
                messages.append(
                    "".join(
                        [
                            f"The following entities are missing {category}: ",
                            ", ".join(sorted(value_set)),
                        ]
                    )
                )
        if messages:
            raise ValueError(". ".join(messages))

        return values
