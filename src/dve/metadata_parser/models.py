"""Data models used within the model generator."""

# pylint: disable=no-self-argument
import datetime as dt
import warnings
from collections import Counter
from collections.abc import Mapping, MutableMapping
from typing import Annotated, Any, Optional, Union

import pydantic as pyd
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, model_validator, field_validator
from typing_extensions import Literal, get_origin

from dve.metadata_parser import exc, function_library
from dve.metadata_parser.function_wrapper import create_validator
from dve.metadata_parser.utilities import FieldTypeOption, chain_get

TypeName = str
"""The name of a Python type."""
TypeAnnotation = Any
"""A Python type annotation."""
Default = Any
"""A specified default value."""
FieldName = str
"""The name of a field."""
FieldAlias = str
"""An alias for a field."""
EntityName = str
"""The name of an entity."""
PydanticType = Union[type, pyd.BaseModel]
"""A pydantic-appropriate type."""
ValidatorName = str
"""The name of a validator."""
Validators = dict[ValidatorName, classmethod]
"""The validators for a class."""


class UnusedConstraints(UserWarning):
    """A warning emitted when 'constraints' are unused."""


class UnusedAliases(UserWarning):
    """A warning emitted when extra aliases are specified."""


class UnsuitableDefault(UserWarning):
    """A warning emitted when the value of 'default' is not aligned
    with the expected type.
    """


class ValidationFunctionSpecification(BaseModel):  # type: ignore
    """Configuration options for a validation function."""

    name: str
    """The name of the validation function to be applied."""
    error_type: Literal["record_rejection", "file_rejection", "warning"] = "record_rejection"
    """The type of error/warning to emit if the function fails."""
    error_message: str = None  # type: ignore
    """The message to associate with the error."""
    fields: list[str] = Field(default_factory=list)
    """Fields to include in the validator."""
    kwargs_: dict[str, Any] = Field(default_factory=dict, alias="kwargs")
    """Keyword arguments for the validation function."""

    @field_validator("name")
    def validate_name(cls, value: str, info: ValidationInfo) -> str:
        """Ensure that the name exists in the function library."""
        if not hasattr(function_library, value):
            raise ValueError(f"Function {value!r} not available in function library")
        return value

    @field_validator("error_message")
    def validate_error_message(cls, value: str, info: ValidationInfo) -> str:
        """set a default error message if one is not available."""
        if value:
            return value
        name: str = info.data["name"]
        return f"{name} failed"

    def get_field_validator(self, field_name: str, **extra_kwargs: Any) -> classmethod:
        """Get a validator a given field."""
        func = getattr(function_library, self.name)
        _kwargs = self.kwargs_ | extra_kwargs
        return create_validator(
            func,
            field_name,
            exc.ERRORS[self.error_type.replace("_", "")],
            self.error_message,
            return_result=True,
            fields=self.fields,
            **_kwargs,
        )


class FieldSpecification(BaseModel):
    """Configuration options for a field."""

    type_: Optional[TypeName] = Field(None, alias="type")
    """
    The name of an allowed type. This should map to a Python type.
    This is mutually exclusive with 'model' and 'callable'.

    """
    model: Optional[EntityName] = None
    """
    The model that the field represents. This indicates the name of a schema
    (defined within the dataset) that will be present within the field.
    This is mutually exclusive with 'model' and 'callable'.

    """
    callable: Optional[str] = None
    """
    The name of a callable which should be called with some constraints (e.g.
    the callable `constr` and the constraints `{'min_length': 1}`) to produce
    a Python type. This is mututally exclusive with 'type' and 'model'.

    """
    constraints: dict[str, Any] = Field(default_factory=dict)
    """Keyword arguments to be used with 'callable'."""
    is_array: bool = False
    """
    A boolean indicating whether the field will be in an array. If this flag
    is `True`, defaults/working examples should be specified as arrays.
    """
    default: Any = None
    """A default value for the field, to be used if it is not provided."""
    functions: list[ValidationFunctionSpecification] = Field(default_factory=list)
    """Validation functions to be applied to the type."""

    model_config = {
        "validate_assignment": True
    }

    @model_validator(mode="after")
    @classmethod
    def ensure_one_type_spec_method(cls, field_spec) -> dict[str, Any]:
        """Ensure that exactly one of 'type', 'model' and 'callable' was specified."""
        has_type = bool(field_spec.type_)
        has_model = bool(field_spec.model)
        has_callable = bool(field_spec.callable)
        n_specified = sum((has_type, has_model, has_callable))

        failure_messages = [
            "Exactly one of 'type', 'model', 'callable' and 'reference'",
            "must be specified.",
        ]

        if n_specified == 0:
            failure_messages.append("None of these were supplied.")
            raise ValueError(" ".join(failure_messages))
        if n_specified > 1:
            supplied = []
            for name, present in (
                ("type", has_type),
                ("model", has_model),
                ("callable", has_callable),
            ):
                if present:
                    supplied.append(name)
                failure_messages.append(f"Got {supplied}")
            raise ValueError(" ".join(failure_messages))

        if not has_callable and field_spec.constraints:
            warnings.warn(
                "'constraints' only used when field specification uses 'callable'",
                category=UnusedConstraints,
            )

        return field_spec

    @field_validator("default")
    def validate_default(cls, value: Any, info: ValidationInfo) -> Any:
        """Validate that 'default' is aligned with 'is_array'."""
        if value is None:
            return value

        is_array = bool(info.data.get("is_array"))
        if is_array:
            if not isinstance(value, list):
                warnings.warn(
                    "Type should be an array but the default value is not",
                    category=UnsuitableDefault,
                )
        elif isinstance(value, list):
            warnings.warn(
                "Type should not be an array but the default value is",
                category=UnsuitableDefault,
            )
        return value

    @property
    def field_type(self) -> Literal["type", "model", "callable", "reference"]:
        """The type of field specification."""
        if self.type_:
            return "type"
        if self.model:
            return "model"
        if self.callable:
            return "callable"
        raise ValueError("No field type specified")  # pragma: no cover

    def _get_validators(
        self,
        field_name: str,
    ) -> Validators:
        """Get the validators for the field, given its name."""
        validators = {}
        for index, validation_function in enumerate(self.functions):
            validator_name = f"{field_name}_{index}_{validation_function.name}"
            validators[validator_name] = validation_function.get_field_validator(
                field_name, each_item=self.is_array
            )
        return validators

    def get_type_and_validators(
        self,
        field_name: str,
        *type_mappings: Mapping[TypeName, FieldTypeOption],
        schemas: Optional[dict[EntityName, pyd.BaseModel]] = None,
        is_mandatory: bool = False,
    ) -> tuple[PydanticType, Default, Validators]:
        """Get the type, default value, and validators for the specification."""
        default: Optional[Default] = self.default
        validators = self._get_validators(field_name)

        if self.type_:
            possible_python_type = chain_get(self.type_, *type_mappings, pyd, dt, __builtins__)
            if isinstance(possible_python_type, type):
                python_type = possible_python_type
            elif get_origin(possible_python_type) is Annotated:
                python_type = possible_python_type
            elif hasattr(possible_python_type, "get_type_and_validators"):
                possible_python_type: "FieldSpecification"  # type: ignore
                nested_vals = possible_python_type.get_type_and_validators(  # type: ignore
                    field_name, *type_mappings, schemas=schemas, is_mandatory=False
                )
                python_type, nested_default, nested_validators = nested_vals

                if nested_validators and self.is_array:
                    # Need to work out how to hook into the validators and update
                    # them to take list[T] instead of T. Probably create validators
                    # and wrap them later in `EntitySpecification`
                    raise ValueError(
                        f"{field_name!r}: Unable to create array of standard type with validators"
                    )

                default = default or nested_default
                for validator_name, nested_validator in nested_validators.items():
                    validators[f"{self.type_}_{validator_name}"] = nested_validator
            else:
                raise ValueError("Fetched type is not a type or field specification")

        elif self.model:
            if not schemas:
                raise ValueError("Type should be model, but `schemas` not passed")
            try:
                python_type = schemas[self.model]
            except KeyError as err:
                raise ValueError(
                    f"Type should be model {self.model!r} but this is not in `schemas`"
                ) from err
        elif self.callable:
            python_type_callable = chain_get(self.callable, *type_mappings, pyd, dt, __builtins__)
            if not callable(python_type_callable):
                raise ValueError("Fetched callable is not callable")
            python_type = python_type_callable(**self.constraints)  # pylint: disable=E1134
        else:
            raise ValueError("No field type set")

        default = default or (... if is_mandatory else None)

        if self.is_array:
            python_type = list[python_type]  # type: ignore

        if not is_mandatory:
            python_type = Optional[python_type]

        return python_type, default, validators


class EntitySpecification(BaseModel):
    """Configuration options for an entity."""

    fields: Annotated[dict[FieldName, FieldSpecification], Field(validate_default=True)]
    """
    A mapping of field names to their Python types. These will either be
    strings representing Python types (if there are no argumements to the type),
    and field specification objects otherwise.

    """
    aliases: dict[FieldName, FieldAlias] = Field(default_factory=dict)
    """A mapping of field name to allowed field alias."""
    mandatory_fields: list[FieldName] = Field(default_factory=list)
    """An array of field names which should be considered mandatory."""

    @field_validator("fields", mode="before")
    def validate_fields(
        cls, value: dict[FieldName, Union[TypeName, FieldSpecification]], info: ValidationInfo
    ) -> dict[FieldName, FieldSpecification]:
        """Convert bare string fields to field specifications."""
        for key in value:
            type_spec = value[key]
            if isinstance(type_spec, str):
                value[key] = FieldSpecification(type=type_spec)

        return value  # type: ignore

    @field_validator("aliases")
    def validate_aliases(
        cls, value: dict[FieldName, FieldAlias], info: ValidationInfo
    ) -> dict[FieldName, FieldAlias]:
        """Ensure that 'aliases' is aligned with 'fields'."""
        # Check that aliases are not given more than once
        if not value:
            return value

        alias_counts = Counter(value.values()).most_common()
        multiple_occurrences = []
        for alias, count in alias_counts:
            if count > 1:
                multiple_occurrences.append(alias)
            else:
                break
        if multiple_occurrences:
            raise ValueError(
                "Aliases must occur exactly once. The following aliases occur "
                + f"more than once: {multiple_occurrences}"
            )
        # And warn when unnecessary aliases were given.
        field_names: set[FieldName] = set(info.data["fields"].keys())
        missing_fields = set(value.keys()) - field_names
        if missing_fields:
            warnings.warn(
                "The following fields were given aliases but are not specified "
                + f"in 'fields': {missing_fields}",
                category=UnusedAliases,
            )

        return value

    @field_validator("mandatory_fields")
    def validate_mandatory_fields(
        cls, value: list[FieldName], info: ValidationInfo
    ) -> list[FieldName]:
        """Ensure that 'mandatory_fields' is aligned with 'fields'."""
        if not value:
            return value

        field_names: set[FieldName] = set(info.data["fields"].keys())
        missing_fields = set(value) - field_names
        if missing_fields:
            raise ValueError(
                "The following fields with specified as mandatory but are not "
                + f"specified in 'fields': {missing_fields}"
            )

        return value

    def as_model(
        self,
        model_name: str,
        *type_mappings: Mapping[TypeName, FieldTypeOption],
        schemas: Optional[dict[EntityName, pyd.BaseModel]] = None,
    ) -> pyd.BaseModel:
        """Get the pydantic model from an entity definition."""
        validators = {}
        pyd_fields = {}

        for field_name, field in self.fields.items():
            python_type, default, field_validators = field.get_type_and_validators(
                field_name,
                *type_mappings,
                schemas=schemas,
                is_mandatory=field_name in self.mandatory_fields,  # pylint: disable=E1135
            )
            pyd_fields[field_name] = (python_type, default)
            validators.update(field_validators)

        return pyd.create_model(  # type: ignore
            model_name,
            **pyd_fields,
            __config__=ConfigDict(
                str_strip_whitespace=True,
                populate_by_name=True,
                extra="ignore"
            ),  # type: ignore
            __validators__=validators,
        )


class DatasetSpecification(BaseModel):
    """Configuration options for a dataset."""

    cache_originals: bool = False
    types: dict[TypeName, FieldSpecification] = Field(default_factory=dict)
    """Predefined types to be used within schema/dataset definitions."""
    schemas: dict[EntityName, EntitySpecification] = Field(default_factory=dict)
    """Predefined models to be used within dataset definitions."""
    datasets: MutableMapping[EntityName, EntitySpecification]
    """Models which represent entities within the data."""

    def load_models(
        self,
        *type_mappings: Mapping[TypeName, FieldTypeOption],
    ) -> dict[EntityName, pyd.BaseModel]:
        """Load the models from the dataset definition."""
        loaded_schemas: dict[EntityName, pyd.BaseModel] = {}
        for model_name, specification in self.schemas.items():  # pylint: disable=E1101
            loaded_schemas[model_name] = specification.as_model(
                model_name, self.types, *type_mappings, schemas=loaded_schemas
            )

        entity_models = {}
        for entity_name, specification in self.datasets.items():
            entity_models[entity_name] = specification.as_model(
                entity_name, self.types, *type_mappings, schemas=loaded_schemas
            )
        return entity_models
