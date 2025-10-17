"""The loader for the first JSON-based dataset configuration."""

import json
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union

from pydantic import BaseModel, Field, PrivateAttr, validate_arguments
from typing_extensions import Annotated, Literal

from dve.core_engine.backends.base.reference_data import ReferenceConfig, ReferenceConfigUnion
from dve.core_engine.backends.metadata.contract import DataContractMetadata, ReaderConfig
from dve.core_engine.backends.metadata.rules import AbstractStep, Rule, RuleMetadata
from dve.core_engine.configuration.base import BaseEngineConfig
from dve.core_engine.configuration.v1.filters import (
    BusinessFilterConfig,
    ConcreteFilterConfig,
    FilterConfigUnion,
)
from dve.core_engine.configuration.v1.rule_stores.models import (
    BusinessComponentSpecConfigUnion,
    BusinessFilterSpecConfig,
    BusinessRuleSpecConfig,
)
from dve.core_engine.configuration.v1.steps import StepConfigUnion
from dve.core_engine.message import DataContractErrorDetail
from dve.core_engine.type_hints import EntityName, ErrorCategory, ErrorType, TemplateVariables
from dve.core_engine.validation import RowValidator
from dve.parser.file_handling import joinuri, open_stream
from dve.parser.type_hints import URI, Extension

TypeName = str
"""The name of a type."""
SchemaName = str
"""The name of a nested schema."""
RuleName = str
"""The name of a business rule."""
RuleDependencies = Set[RuleName]
"""A list of dependencies required by a rule."""

FieldName = str
"""The name of a field within a model/schema."""
TypeOrDef = Union[
    TypeName, "_CallableTypeDefinition", "_ModelTypeDefinition", "_TypeAliasDefinition"
]
"""The name or definition of a type."""

Operation = str
"""The operation """
RuleType = Type[AbstractStep]
"""The metadata step type implemented by the rule."""


class _BaseTypeDefintion(BaseModel):
    """The base definition of a type."""

    description: Optional[str] = None
    """An optional description for the type."""
    is_array: bool = False
    """Whether the type is an array."""


class _CallableTypeDefinition(_BaseTypeDefintion):
    """The definition of a type created from a callable."""

    callable: str
    """The callable to be called to create the type."""
    constraints: Dict[str, Any] = Field(default_factory=dict)
    """The keyword arguments passed to the callable as kwargs."""


class _ModelTypeDefinition(_BaseTypeDefintion):
    """The definition of a type created by specifying a model."""

    model: SchemaName
    """The name of the model to use for the field (e.g. a schema)."""


class _TypeAliasDefinition(_BaseTypeDefintion):
    """The definition of a type alias."""

    type: str
    """The name of the Python type."""


class _SchemaConfig(BaseModel):
    """Configuration for a component schema within a dataset."""

    fields: Dict[FieldName, TypeOrDef]
    """Field definitions within the schema."""
    mandatory_fields: List[FieldName] = Field(default_factory=list)
    """A list of the field names within the schema which _must_ be provided."""


class _ReaderConfig(BaseModel):  # type: ignore
    """Reader configuration options for a model."""

    reader: str
    """The name of the reader to use."""
    kwargs_: Dict[str, Any] = Field(alias="kwargs", default_factory=dict)
    """Keyword arguments for the reader."""
    field_names: Optional[List[str]] = None
    """The field names to request from the reader. These are deprecated and will not be used."""


class _ModelConfig(_SchemaConfig):
    """A concrete model within the dataset."""

    reporting_fields: List[FieldName] = Field(default_factory=list)
    """A list of the reporting fields within the model."""
    key_field: Optional[str] = None
    """A single key field to be used by the model."""
    reader_config: Dict[Extension, _ReaderConfig]
    """Reader configuration options for the model."""
    aliases: Dict[FieldName, FieldName] = Field(default_factory=dict)
    """An alias field name mapping."""


class _RuleStoreConfig(BaseModel):
    """Configuration options for a rule store."""

    store_type: Literal["json"]
    """The type of the rule store."""
    filename: str
    """The file name of the rule store."""


class _ComplexRuleConfig(BaseModel):
    """Configuration for a complex rule."""

    rule_name: str
    """The name of the complex rule."""
    parameters: Dict[str, Any] = Field(default_factory=dict)
    """The parameters for the rule."""


class V1DataContractConfig(BaseModel):
    """Configuration for the data contract component of the dataset."""

    cache_originals: bool = False
    """Whether to cache the original entities after loading."""
    contract_error_message_info: Optional[URI] = None
    """Optional URI containing custom data contract error codes and messages"""
    types: Dict[TypeName, TypeOrDef] = Field(default_factory=dict)
    """Dataset specific types defined within the config."""
    schemas: Dict[SchemaName, _SchemaConfig] = Field(default_factory=dict)
    """Component schemas within the config."""
    datasets: Dict[SchemaName, _ModelConfig]
    """Concrete entity definitions which will be loaded by the config."""


class V1TransformationConfig(BaseModel):
    """Configuration for the transformation component of the dataset."""

    rule_stores: List[_RuleStoreConfig] = Field(default_factory=list)
    """The external rule stores that rules can be referenced from."""
    reference_data: Dict[EntityName, ReferenceConfigUnion] = Field(default_factory=dict)
    """Configuration options for reference data."""
    parameters: Dict[str, Any] = Field(default_factory=dict)
    """Global parameters to be passed to rules for templating."""
    rules: List[StepConfigUnion] = Field(default_factory=list)
    """Pre-filter stage rules."""
    filters: List[FilterConfigUnion] = Field(default_factory=list)
    """Filter stage rules."""
    post_filter_rules: List[StepConfigUnion] = Field(default_factory=list)
    """Post-filter stage rules/"""
    complex_rules: List[_ComplexRuleConfig] = Field(default_factory=list)
    """Complex rules."""


class V1EngineConfig(BaseEngineConfig):
    """A loader for the first version of the configuration language."""

    contract: V1DataContractConfig
    """The data contract configuration."""
    transformations: V1TransformationConfig = Field(default_factory=V1TransformationConfig)
    """The transformation/rules configuration."""
    _rule_store_rules: Dict[RuleName, BusinessComponentSpecConfigUnion] = PrivateAttr(
        default_factory=dict
    )
    """Rule store rules from the loaded rule stores."""

    @validate_arguments
    def _update_rule_store(self, rule_store: Dict[RuleName, BusinessComponentSpecConfigUnion]):
        """Update the rule store rules to add/override the rules from the new store."""
        self._rule_store_rules.update(rule_store)

    def _load_rule_store(self, uri: URI):
        """Load a JSON rule store from the provided URI and update the stored
        rule store rules.

        """
        with open_stream(uri) as rule_store_stream:
            rule_store_json = json.load(rule_store_stream)

        if not isinstance(rule_store_json, dict):
            raise TypeError("Rule store must contain mapping in root")
        self._update_rule_store(rule_store_json)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        uri_prefix = self.location.rsplit("/", 1)[0]
        for rule_store_config in self.transformations.rule_stores:
            uri = joinuri(uri_prefix, rule_store_config.filename)
            self._load_rule_store(uri)

    def _resolve_business_filter(
        self, config: BusinessFilterConfig
    ) -> Tuple[ConcreteFilterConfig, TemplateVariables]:
        """Resolve a business filter and create a concrete filter."""
        local_params: TemplateVariables = config.parameters.copy()

        business_filter_spec = self._rule_store_rules[config.rule_name]
        if not isinstance(business_filter_spec, BusinessFilterSpecConfig):
            raise ValueError(f"Rule {config.rule_name!r} should be a filter, but is a complex rule")

        rule_config = business_filter_spec.rule_config
        # NOTE: This will no longer enable recursive templating - rule names must be constant.
        if isinstance(rule_config, BusinessFilterConfig):
            rule_config, other_local_params = self._resolve_business_filter(config)
            local_params.update(other_local_params)
        return rule_config, local_params

    def _create_rule(
        self,
        name: str,
        rules: List[StepConfigUnion],
        filters: List[FilterConfigUnion],
        post_filter_rules: List[StepConfigUnion],
    ) -> Tuple[Rule, TemplateVariables]:
        """Create a rule from the config types, returning the rule and any template vars
        from the filters.

        """
        local_params = {}
        pre_sync_steps = [step_config.to_step() for step_config in rules]

        sync_filter_steps = []
        for filter_def in filters:
            if isinstance(filter_def, BusinessFilterConfig):
                filter_def, temp_local_params = self._resolve_business_filter(filter_def)
                local_params.update(temp_local_params)
            sync_filter_steps.append(filter_def.to_step())

        post_sync_steps = [step_config.to_step() for step_config in post_filter_rules]

        rule = Rule(
            name=name,
            pre_sync_steps=pre_sync_steps,
            sync_filter_steps=sync_filter_steps,
            post_sync_steps=post_sync_steps,
        )
        return rule, local_params

    def _resolve_business_rule(
        self, config: _ComplexRuleConfig
    ) -> Tuple[Rule, TemplateVariables, RuleDependencies]:
        """Load a complex business rule spec to a rule."""
        rule_spec = self._rule_store_rules[config.rule_name]
        if not isinstance(rule_spec, BusinessRuleSpecConfig):
            raise ValueError(f"Rule {config.rule_name!r} should be a filter, but is a complex rule")
        local_params: TemplateVariables = rule_spec.parameter_defaults.copy()
        local_params.update(config.parameters.copy())

        rule, new_local_params = self._create_rule(
            name=config.rule_name,
            rules=rule_spec.rule_config.rules,
            filters=rule_spec.rule_config.filters,
            post_filter_rules=rule_spec.rule_config.post_filter_rules,
        )
        local_params.update(new_local_params)
        return rule, local_params, set(rule_spec.dependencies)

    def _load_rules_and_vars(self) -> Tuple[List[Rule], List[TemplateVariables]]:
        """Load the rules and local variables for the transformations."""
        rules, local_variable_list = [], []
        added_rules: Set[RuleName] = set()

        for index, complex_rule_config in enumerate(self.transformations.complex_rules):
            rule, local_params, deps = self._resolve_business_rule(complex_rule_config)
            missing_rules = deps - added_rules
            if missing_rules:
                raise ValueError(
                    f"Missing dependendencies ({missing_rules}) required by complex"
                    + f"rule {rule.name!r} (index: {index})"
                )
            rules.append(rule)
            local_variable_list.append(local_params)
            added_rules.add(rule.name)

        rule, local_params = self._create_rule(
            name="root",
            rules=self.transformations.rules,
            filters=self.transformations.filters,
            post_filter_rules=self.transformations.post_filter_rules,
        )
        rules.append(rule)
        local_variable_list.append(local_params)
        return rules, local_variable_list

    def get_contract_metadata(self) -> DataContractMetadata:
        """Get the data contract metadata from the config"""
        reader_metadata = {}
        validators = {}
        reporting_fields = {}

        contract_dict = self.contract.dict()
        error_info = {}
        if self.contract.contract_error_message_info:
            error_info = self.load_error_message_info(
                self.contract.contract_error_message_info
                )
        for entity_name, dataset_config in self.contract.datasets.items():
            reader_metadata[entity_name] = {
                ext: ReaderConfig(reader=config.reader, parameters=config.kwargs_)
                for ext, config in dataset_config.reader_config.items()
            }
            reporting_fields[entity_name] = dataset_config.reporting_fields
            validators[entity_name] = RowValidator(
                contract_dict, entity_name, error_info=error_info
            )

        return DataContractMetadata(
            reader_metadata=reader_metadata,
            validators=validators,
            reporting_fields=reporting_fields,
            cache_originals=self.contract.cache_originals,
        )

    def load_error_message_info(self, uri):
        """Load data contract error info from json file"""
        uri_prefix = self.location.rsplit("/", 1)[0]
        with open_stream(joinuri(uri_prefix, uri)) as stream:
            return json.load(stream)

    def get_reference_data_config(self) -> Dict[EntityName, ReferenceConfig]:  # type: ignore
        """Gets the reference data configuration from the transformations"""
        return self.transformations.reference_data

    def get_rule_metadata(self) -> RuleMetadata:
        """Gets the rule metadata from the Engine configuration"""
        rules, local_variables = self._load_rules_and_vars()
        return RuleMetadata(
            rules=rules,
            local_variables=local_variables,
            global_variables=self.transformations.parameters,
            reference_data_config=self.get_reference_data_config(),
        )
