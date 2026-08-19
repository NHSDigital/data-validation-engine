"""Classes to help determine and store entity hierarchy information."""

from typing import Any, Optional, Union
from pydantic import BaseModel, Field
from dve.metadata_parser.exc import EntityNotFoundError

class HierarchyNode(BaseModel):
    """Stores entity hierarchy information"""
    entity_name: str
    mandatory: Optional[bool] = False
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
    
    def as_dict(self) -> dict[str, dict[str, Any]]:
        """Get dictionary representation of entity hierarchy"""
        child_dict = {}
        for node in self.children:
             child_dict.update(node.as_dict())
        
        ret_dict = {"children": child_dict,
                    "mandatory": self.mandatory}
        if hasattr(self, "join_fields"):
            ret_dict |= {"join_fields": self.join_fields}
        
        return {self.entity_name: ret_dict}


class ChildHierarchyNode(HierarchyNode):
    """Stores child entity hierarchy information"""
    join_fields: list[str]
