"""Metadata classes for the data contract."""

from typing import Any, Optional, Union

from pydantic import BaseModel, Field, PrivateAttr, model_validator

from dve.core_engine.type_hints import EntityName, ReportingFields
from dve.core_engine.validation import RowValidator
from dve.metadata_parser.exc import EntityNotFoundError
from dve.parser.type_hints import Extension

class HierarchyNode(BaseModel):
    entity_name: str
    children: Optional[list["HierarchyNode"]] = Field(default_factory=list)
    
    def get_descendents(self) -> list[str]:
        """Recursively list all descendents of the node"""
        descendents = []
        for node in self.children:
            descendents.append(node.entity_name)
            descendents.extend(node.get_descendents())
        return descendents
    
    def get_node(self, entity_name:str) -> Union["HierarchyNode", None]:
        """Recursively search for node and return if found"""
        node = None
        if self.entity_name == entity_name:
            return self
        else:
            for child in self.children:
                node = child.get_node(entity_name)
                if node:
                    break
        return node
    
    def add_child_node(self, parent_entity: str, child_info: "HierarchyNode") -> None:
        """Add a child node if the parent exists in the hierarchy"""
        try:
            self.get_node(parent_entity).children.append(child_info)
        except AttributeError:
            raise EntityNotFoundError(f"Can't find parent node {parent_entity} in {self.entity_name}")
    
    def as_dict(self):
        ret_dict = {}
        for node in self.children:
             ret_dict.update(node.as_dict())
        return {self.entity_name: {"children": ret_dict}}


class ChildHierarchyNode(HierarchyNode):
    join_fields: list[str]
    
    def as_dict(self):
        ret_value = {self.entity_name: {"join_fields": self.join_fields}}
        for node in self.children:
            ret_value[self.entity_name] |= {"children": node.as_dict()}
        return ret_value


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
    linkage_hierarchy: dict[EntityName, HierarchyNode] = Field(default_factor=dict)

    @property
    def schemas(self) -> dict[EntityName, type[BaseModel]]:
        """The per-entity schemas, as pydantic models."""
        if not self._schemas:
            for entity_name, validator in self.validators.items():
                self._schemas[entity_name] = validator.model  # type: ignore # pylint: disable=E1137
        return self._schemas.copy()  # pylint: disable=E1101

    @model_validator(mode="before")
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
