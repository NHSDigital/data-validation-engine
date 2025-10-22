"""Very basic configuration options for steps.

These (mostly) only differ slightly from the metadata steps,
but there's some repetition here to make it possible to change
the metadata steps without altering the config classes.

N.B. These are quite coarsely copied from the JSON schema.

"""

# pylint: disable=missing-class-docstring
from abc import ABC, abstractmethod
from typing import Any, Optional, Union

from pydantic import BaseModel, Extra, Field, validator
from typing_extensions import Annotated, Literal

from dve.core_engine.backends.metadata.rules import (
    AbstractStep,
    Aggregation,
    AntiJoin,
    ColumnAddition,
    ColumnRemoval,
    ConfirmJoinHasMatch,
    CopyEntity,
    EntityRemoval,
    HeaderJoin,
    ImmediateFilter,
    InnerJoin,
    LeftJoin,
    OneToOneJoin,
    RenameEntity,
    SelectColumns,
    SemiJoin,
    TableUnion,
)
from dve.core_engine.type_hints import MultipleExpressions


class ConfigStep(BaseModel, ABC):
    """The parent for the config steps."""

    class Config:  # pylint: disable=too-few-public-methods
        """Config class for dynamically generated pydantic models"""

        extra = Extra.forbid

    name: Optional[str] = None
    """The 'name' of the rule. This is mapped to an ID in the entity."""
    operation: str
    """The operation implemented by the step."""

    @abstractmethod
    def to_step(self) -> AbstractStep:
        """Convert the config step definition to a 'real' metadata step."""


class AddConfig(ConfigStep):
    """Configuration step for adding a new column"""

    operation: Literal["add"]

    entity: str
    new_entity_name: Optional[str] = None
    column_name: str
    expression: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return ColumnAddition(
            id=self.name,
            entity_name=self.entity,
            new_entity_name=self.new_entity_name,
            column_name=self.column_name,
            expression=self.expression,
        )


class RemoveConfig(ConfigStep):
    """Configuration step for removing a column"""

    operation: Literal["remove"]

    entity: str
    new_entity_name: Optional[str] = None
    column_name: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return ColumnRemoval(
            id=self.name,
            entity_name=self.entity,
            new_entity_name=self.new_entity_name,
            column_name=self.column_name,
        )


class GroupByConfig(ConfigStep):
    """Configuration step for performing a GROUP BY operation"""

    operation: Literal["group_by"]

    entity: str
    new_entity_name: Optional[str] = None
    group_by: MultipleExpressions
    pivot_column: Optional[str] = None
    pivot_values: Optional[list[str]] = None
    agg_columns: MultipleExpressions

    @validator("pivot_values")
    @classmethod
    def _ensure_no_values_if_not_column(cls, value: Optional[str], values: dict[str, Any]):
        if value and not values["pivot_column"]:
            raise ValueError("Cannot provide 'pivot_values' if no 'pivot_column'")
        return value

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return Aggregation(
            id=self.name,
            entity_name=self.entity,
            new_entity_name=self.new_entity_name,
            group_by=self.group_by,
            pivot_column=self.pivot_column,
            pivot_values=self.pivot_values,
            agg_columns=self.agg_columns,
        )


class SelectConfig(ConfigStep):
    """Configuration step for performing a SELECT operation"""

    operation: Literal["select"]

    entity: str
    new_entity_name: Optional[str] = None
    columns: MultipleExpressions
    distinct: bool = False

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return SelectColumns(
            id=self.name,
            entity_name=self.entity,
            new_entity_name=self.new_entity_name,
            columns=self.columns,
            distinct=self.distinct,
        )


class RenameEntityConfig(ConfigStep):
    """Configuration step for renaming an entity"""

    operation: Literal["rename_entity"]

    entity: str
    new_entity_name: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return RenameEntity(
            id=self.name,
            entity_name=self.entity,
            new_entity_name=self.new_entity_name,
        )


class NonNotifyingFilterConfig(ConfigStep):
    """Configuration step for filtering out values without creating errors

    mainly used on derived entities
    """

    operation: Literal["filter_without_notifying"]

    entity: str
    new_entity_name: Optional[str] = None
    filter_rule: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return ImmediateFilter(
            id=self.name,
            entity_name=self.entity,
            new_entity_name=self.new_entity_name,
            expression=self.filter_rule,
        )


class HasMatchConfig(ConfigStep):
    """Configuration step for checking if a value has a match in another entity"""

    operation: Literal["has_match"]

    entity: str
    new_entity_name: Optional[str] = None
    target: str
    join_condition: str
    column_name: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return ConfirmJoinHasMatch(
            id=self.name,
            entity_name=self.entity,
            target_name=self.target,
            new_entity_name=self.new_entity_name,
            join_condition=self.join_condition,
            column_name=self.column_name,
        )


class SemiOrAntiJoinConfig(ConfigStep):
    """Configuration step for performing a SEMI or ANTI JOIN

    More performant than a left or right join for checking membership in another entity
    """

    operation: Literal["semi_join", "anti_join"]

    entity: str
    new_entity_name: Optional[str] = None
    target: str
    join_condition: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        type_ = AntiJoin if self.operation == "anti_join" else SemiJoin
        return type_(
            id=self.name,
            entity_name=self.entity,
            target_name=self.target,
            new_entity_name=self.new_entity_name,
            join_condition=self.join_condition,
        )


class LeftOrInnerJoinConfig(SemiOrAntiJoinConfig):
    """Configuration step for performing a LEFT or INNER JOIN"""

    operation: Literal["left_join", "inner_join"]  # type: ignore

    new_columns: MultipleExpressions

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        type_ = LeftJoin if self.operation == "left_join" else InnerJoin
        return type_(
            id=self.name,
            entity_name=self.entity,
            target_name=self.target,
            new_entity_name=self.new_entity_name,
            join_condition=self.join_condition,
            new_columns=self.new_columns,
        )


class OneToOneJoinConfig(LeftOrInnerJoinConfig):
    """Config for joining one entity to another"""

    operation: Literal["join", "one_to_one_join"]  # type: ignore

    perform_integrity_check: bool = True

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return OneToOneJoin(
            id=self.name,
            entity_name=self.entity,
            target_name=self.target,
            new_entity_name=self.new_entity_name,
            join_condition=self.join_condition,
            new_columns=self.new_columns,
            perform_integrity_check=self.perform_integrity_check,
        )


class JoinHeaderConfig(ConfigStep):
    """Config for joining a header onto another entity"""

    operation: Literal["join_header"]

    entity: str
    new_entity_name: Optional[str] = None
    target: str
    header_column_name: str = "_Header"
    perform_integrity_check: bool = True

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return HeaderJoin(
            id=self.name,
            entity_name=self.entity,
            target_name=self.target,
            new_entity_name=self.new_entity_name,
            header_column_name=self.header_column_name,
        )


class UnionConfig(ConfigStep):
    """Config to unioning two entities"""

    operation: Literal["union"]

    entity: str
    new_entity_name: Optional[str] = None
    target: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return TableUnion(
            id=self.name,
            entity_name=self.entity,
            target_name=self.target,
            new_entity_name=self.new_entity_name,
        )


class CopyEntityConfig(ConfigStep):
    """Config for copying entities"""

    operation: Literal["copy_entity"]

    entity: str
    new_entity_name: str

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return CopyEntity(
            id=self.name, entity_name=self.entity, new_entity_name=self.new_entity_name
        )


class RemoveEntityConfig(ConfigStep):
    """Config for removing entities"""

    operation: Literal["remove_entity", "remove_entities"]

    entity: Union[str, list[str]]

    def to_step(self) -> AbstractStep:
        """Takes a config object and returns a step object"""
        return EntityRemoval(id=self.name, entity_name=self.entity)


StepConfigUnion = Annotated[
    Union[
        AddConfig,
        CopyEntityConfig,
        GroupByConfig,
        HasMatchConfig,
        JoinHeaderConfig,
        LeftOrInnerJoinConfig,
        NonNotifyingFilterConfig,
        OneToOneJoinConfig,
        RemoveConfig,
        RemoveEntityConfig,
        RenameEntityConfig,
        SelectConfig,
        SemiOrAntiJoinConfig,
        UnionConfig,
    ],
    Field(discriminator="operation"),
]
"""Pydantic configuration classes for steps."""
