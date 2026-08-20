"""Classes to help determine and store entity hierarchy information."""

import json
from typing import Any, Iterable, Optional, Union

from pydantic import BaseModel, Field

from dve.core_engine.configuration.v1 import V1EngineConfig, _LinkageConfig
from dve.core_engine.type_hints import EntityName, ErrorCode, ErrorMessage
from dve.metadata_parser.exc import EntityNotFoundError
from dve.parser.file_handling.service import open_stream
from dve.parser.type_hints import URI


class HierarchyNode(BaseModel):
    """Stores entity hierarchy information"""

    entity_name: str
    children: list["HierarchyNode"] = Field(default_factory=list)

    def get_descendents(self) -> list[str]:
        """Recursively list all descendents of the node"""
        descendents = []
        for node in self.children:
            descendents.append(node.entity_name)
            descendents.extend(node.get_descendents())
        return descendents

    def get_node(self, entity_name: str) -> Union["HierarchyNode", None]:
        """Recursively search for node and return if found"""
        node = None
        if self.entity_name == entity_name:
            return self
        for child in self.children:
            node = child.get_node(entity_name)
            if node:
                break
        return node

    def add_child_node(self, parent_entity: str, child_info: "HierarchyNode") -> None:
        """Add a child node if the parent exists in the hierarchy"""
        try:
            self.get_node(parent_entity).children.append(child_info)  # type: ignore
        except AttributeError as exc:
            raise EntityNotFoundError(
                f"Can't find parent node {parent_entity} in {self.entity_name}"
            ) from exc

    def as_dict(self) -> dict[str, dict[str, Any]]:
        """Get dictionary representation of entity hierarchy"""
        child_dict = {}
        for node in self.children:
            child_dict.update(node.as_dict())

        ret_dict = self.model_dump(exclude={"entity_name", "children"})
        ret_dict.update({"children": child_dict})

        return {self.entity_name: ret_dict}


class ChildHierarchyNode(HierarchyNode):
    """Stores child entity hierarchy information"""

    join_fields: dict[str, str]
    mandatory: Optional[bool] = False
    no_valid_records_error_code: Optional[ErrorCode] = "NoValidRecords"
    no_valid_records_error_message: Optional[ErrorMessage] = (
        "parent record removed as no valid child records"
    )
    orphaned_records_error_code: Optional[ErrorCode] = "OrphanedRecords"
    orphaned_records_error_message: Optional[ErrorMessage] = "Orphaned records removed"


class EntityHierarchy:
    """Determines and stores entity hierarchy information from config"""

    def __init__(self, entity_trees: dict[EntityName, HierarchyNode]):
        self.entity_trees = entity_trees

    @staticmethod
    def determine_trees(
        all_datasets: Iterable[str], entity_relationships: dict[str, _LinkageConfig]
    ) -> dict[EntityName, HierarchyNode]:
        """Determine the entity hierarchy trees and store as HierarchyNodes"""
        top_level_parents: dict[EntityName, HierarchyNode] = {
            entity_name: HierarchyNode(entity_name=entity_name)
            for entity_name in all_datasets
            if not entity_name in entity_relationships
        }

        for name, linkage_detail in entity_relationships.items():
            for main_entity, parent_node in top_level_parents.items():
                if (
                    linkage_detail.parent_entity == main_entity
                    or linkage_detail.parent_entity in parent_node.get_descendents()
                ):
                    parent_node.add_child_node(
                        linkage_detail.parent_entity,
                        ChildHierarchyNode(
                            entity_name=name, **linkage_detail.model_dump(exclude={"parent_entity"})
                        ),
                    )
                    break
            else:
                raise EntityNotFoundError(
                    f"Can't find parent entity {linkage_detail.parent_entity} defined to "
                    + f"establish hierarchy for {name} - please ensure it is defined above "
                    + "any child entities in the dischema."
                )
        return top_level_parents

    @classmethod
    def from_dischema(cls, dischema_uri: URI):
        """Create entity hierarchy direct from dischema"""
        with open_stream(dischema_uri) as dischema:
            config_dict = json.load(dischema)
        all_datasets = config_dict.get("contract", {}).get("datasets", {}).keys()
        entity_relationships = {
            k: _LinkageConfig(**v) for k, v in config_dict.get("entity_relationships", {}).items()
        }
        return cls(entity_trees=cls.determine_trees(all_datasets, entity_relationships))

    @classmethod
    def from_engine_config(cls, engine_config: V1EngineConfig):
        """Create entity hierarchy direct from engine config"""
        return cls(
            entity_trees=cls.determine_trees(
                all_datasets=engine_config.contract.datasets.keys(),
                entity_relationships=engine_config.entity_relationships,
            )
        )
