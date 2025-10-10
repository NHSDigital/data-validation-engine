"""Models for components in the rule stores."""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated, Literal

from dve.core_engine.configuration.v1.filters import FilterConfigUnion
from dve.core_engine.configuration.v1.steps import StepConfigUnion


class BusinessSpecConfig(BaseModel):
    """A business rule or filter within the config."""

    type: str
    """The type of business rule."""

    description: Optional[str] = None
    """A description of what the rule/filter should do."""
    parameter_descriptions: Dict[str, str] = Field(default_factory=dict)
    """Descriptions of parameters used by the rule."""
    parameter_defaults: Dict[str, Any] = Field(default_factory=dict)
    """Default parameters to be used by the rule if no param is passed."""


class BusinessFilterSpecConfig(BusinessSpecConfig):
    """A business filter within the rule store."""

    type: Literal["filter"]

    rule_config: FilterConfigUnion
    """The configuration for the filter."""


class ComplexRuleConfig(BaseModel):
    """The rule config for a business rule."""

    rules: List[StepConfigUnion] = Field(default_factory=list)
    filters: List[FilterConfigUnion] = Field(default_factory=list)
    post_filter_rules: List[StepConfigUnion] = Field(default_factory=list)


class BusinessRuleSpecConfig(BusinessSpecConfig):
    """A business rule within the rule store."""

    type: Literal["complex_rule"]

    rule_config: ComplexRuleConfig
    """The configuration for the rule."""
    dependencies: List[str] = Field(default_factory=list)
    """The dependencies for the business rule."""


BusinessComponentSpecConfigUnion = Annotated[
    Union[BusinessFilterSpecConfig, BusinessRuleSpecConfig], Field(discriminator="type")
]
"""A union of the different business component types."""
