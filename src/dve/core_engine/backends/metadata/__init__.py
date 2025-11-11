# ruff: noqa: F401
"""Metadata classes for the engine.

These sit between the configuration layer and the backend implementations,
and should be flexible enough to allow for changes in config format without
necessitating big implementation changes.

"""

from pydantic import BaseModel

from dve.core_engine.backends.metadata.contract import DataContractMetadata, ReaderConfig
from dve.core_engine.backends.metadata.reporting import LegacyReportingConfig, ReportingConfig
from dve.core_engine.backends.metadata.rules import (
    AbstractStep,
    Aggregation,
    AntiJoin,
    BaseStep,
    ColumnAddition,
    ColumnRemoval,
    ConfirmJoinHasMatch,
    CopyEntity,
    DeferredFilter,
    EntityRemoval,
    HeaderJoin,
    ImmediateFilter,
    InnerJoin,
    LeftJoin,
    OneToOneJoin,
    OrphanIdentification,
    ParentMetadata,
    RenameEntity,
    Rule,
    RuleMetadata,
    SelectColumns,
    SemiJoin,
    TableUnion,
)


class RunMetadata(BaseModel):
    """Metadata for a whole engine run."""

    contract_metadata: DataContractMetadata
    """The metadata for the data contract."""
    rule_metadata: RuleMetadata
    """The metadata for the rules."""
