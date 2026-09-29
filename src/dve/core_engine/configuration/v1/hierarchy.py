"""Classes to help determine and store entity hierarchy information."""

import json
from typing import Any, Iterable, Optional, Union

from pydantic import BaseModel, Field, model_validator

from dve.core_engine.configuration.v1 import V1EngineConfig, _LinkageConfig
from dve.core_engine.type_hints import EntityName, ErrorCode, ErrorMessage
from dve.metadata_parser.exc import EntityNotFoundError
from dve.parser.file_handling.service import open_stream
from dve.parser.type_hints import URI


class HierarchyNode(BaseModel):
    """Stores entity hierarchy information"""

    entity_name: str
    parent_entity: Optional[str] = None
    children: list["HierarchyNode"] = Field(default_factory=list)
    mandatory: bool = False
    join_fields: dict[str, str] = Field(default_factory=dict)
    no_valid_records_error_code: ErrorCode = "NoValidRecords"
    no_valid_records_error_message: ErrorMessage = "parent record removed as no valid child records"
    missing_parent_id_error_code: Optional[ErrorCode] = "MissingParentRecord"
    missing_parent_id_error_message: Optional[ErrorMessage] = (
        "Records removed due to no valid parent record"
    )
    empty_entity_error_code: ErrorCode = "EmptyEntity"
    empty_entity_error_message: ErrorMessage = "no valid records remaining"

    @model_validator(mode="after")
    def validate_empty_error_details(self):
        """
        Removes the default messaging for checking empty entities as not performed on
        non mandatory nodes/entities
        """
        if not self.mandatory:
            self.empty_entity_error_code = None
            self.empty_entity_error_message = None
        return self

    def get_descendents(self) -> list["HierarchyNode"]:
        """Recursively list all descendents of the node"""
        descendents = []
        for node in self.children:  # type: ignore
            descendents.append(node)
            descendents.extend(node.get_descendents())
        return descendents

    def get_descendent_names(self) -> list[str]:
        """Recursively list all names of descendents of the node"""
        return [node.entity_name for node in self.get_descendents()]

    def get_node(self, entity_name: str) -> Union["HierarchyNode", None]:
        """Recursively search for node and return if found"""
        node = None
        if self.entity_name == entity_name:
            return self
        for child in self.children:  # type: ignore
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
        child_dict: dict[str, dict[str, Any]] = {}
        for node in self.children:  # type: ignore
            child_dict.update(node.as_dict())

        ret_dict = self.model_dump(exclude={"entity_name", "children"})
        ret_dict.update({"children": child_dict})

        return {self.entity_name: ret_dict}

    def _get_full_tree(self):
        """Get all nodes in tree, including the root"""
        desc = self.get_descendents()
        desc.insert(0, self)
        return desc

    def iterate_root_down(self):
        """Iterate through nodes from root to lowest descendent"""
        yield from self._get_full_tree()

    def iterate_lowest_descendent_up(self):
        """Iterate through nodes from lowest descendent to root"""
        yield from self._get_full_tree()[::-1]


class EntityHierarchy:
    """Determines and stores entity hierarchy information from config"""

    def __init__(self, entity_trees: dict[EntityName, HierarchyNode]):
        self.entity_trees = entity_trees

    @staticmethod
    def determine_trees(
        all_datasets: Iterable[str], entity_relationships: dict[str, _LinkageConfig]
    ) -> dict[EntityName, HierarchyNode]:
        """Determine the entity hierarchy trees and store as HierarchyNodes"""
        root_entities: dict[str, _LinkageConfig] = dict(
            filter(lambda x: x[1].is_root_entity, entity_relationships.items())
        )
        top_level_parents: dict[EntityName, HierarchyNode] = {
            entity_name: HierarchyNode(
                entity_name=entity_name,
                parent_entity=None,
                **config.model_dump(
                    exclude={
                        "parent_entity",
                        "missing_parent_id_error_code",
                        "missing_parent_id_error_message",
                    }
                ),
                missing_parent_id_error_code=None,
                missing_parent_id_error_message=None,
            )
            for entity_name, config in root_entities.items()
        }

        if default_roots := [
            entity_name for entity_name in all_datasets if entity_name not in entity_relationships
        ]:
            for entity_name in default_roots:
                top_level_parents[entity_name] = HierarchyNode(
                    entity_name=entity_name,
                    parent_entity=None,
                    missing_parent_id_error_code=None,
                    missing_parent_id_error_message=None,
                )

        for name, linkage_detail in entity_relationships.items():
            for main_entity, parent_node in top_level_parents.items():
                if linkage_detail.is_root_entity:
                    break

                if (
                    linkage_detail.parent_entity == main_entity
                    or linkage_detail.parent_entity in parent_node.get_descendent_names()
                ):
                    parent_node.add_child_node(
                        linkage_detail.parent_entity,  # type: ignore
                        HierarchyNode(entity_name=name, **linkage_detail.model_dump()),
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

    def get_all_mandatory_nodes(
        self,
        node: Optional[HierarchyNode] = None,
        mandatory_nodes: Optional[list[HierarchyNode]] = None,
        nodes_visited: Optional[set[EntityName]] = None,
    ) -> list[HierarchyNode]:
        """Find and return all mandatory nodes"""
        if mandatory_nodes is None:
            mandatory_nodes = []

        if nodes_visited is None:
            nodes_visited = set()

        if node is None:
            for _node in self.entity_trees.values():
                self.get_all_mandatory_nodes(_node, mandatory_nodes, nodes_visited)

        if node:
            if node.mandatory and node.entity_name not in nodes_visited:
                nodes_visited.add(node.entity_name)
                mandatory_nodes.append(node)
            for child_node in node.children:
                self.get_all_mandatory_nodes(child_node, mandatory_nodes, nodes_visited)

        return mandatory_nodes
