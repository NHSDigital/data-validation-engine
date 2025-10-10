"""The loader for the first JSON-based dataset configuration."""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from dve.core_engine.backends.metadata.reporting import UntemplatedReportingConfig
from dve.core_engine.backends.metadata.rules import AbstractStep, DeferredFilter
from dve.core_engine.type_hints import ErrorCategory


class ConcreteFilterConfig(BaseModel):
    """Configuration options for a reporting filter."""

    name: Optional[str] = None
    entity: str
    expression: str
    failure_type: str = "record"
    failure_message: Optional[str] = None
    is_informational: Union[str, bool] = False
    error_code: Optional[str] = None
    error_location: Optional[str] = None
    reporting_entity: Optional[str] = None
    reporting_field: Optional[Union[str, List[str]]] = None
    reporting_field_name: Optional[str] = None
    category: ErrorCategory = "Bad value"

    def to_step(self) -> AbstractStep:
        """Create a deferred filter from the concrete filter config."""
        reporting = UntemplatedReportingConfig(
            code=self.error_code,
            message=self.failure_message,
            category=self.category,
            reporting_entity_override=self.reporting_entity,
            reporting_field_override=self.reporting_field_name,
            legacy_error_type=self.failure_type,
            legacy_is_informational=self.is_informational,
            legacy_location=self.error_location,
            legacy_reporting_field=self.reporting_field,
        )

        return DeferredFilter(
            id=self.name,
            entity_name=self.entity,
            expression=self.expression,
            reporting=reporting,
        )


class BusinessFilterConfig(BaseModel):
    """A business filter."""

    rule_name: str
    """The name of the business rule."""
    parameters: Dict[str, Any] = Field(default_factory=dict)
    """Parameters for the business rule."""


FilterConfigUnion = Union[ConcreteFilterConfig, BusinessFilterConfig]
"""A union of the available filter configuration types."""
