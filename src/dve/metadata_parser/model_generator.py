"""Tools for parsing metadata to pydantic Models"""

# pylint: disable=super-init-not-called
import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import Mapping
from copy import deepcopy

# This _needs_ to be `typing.Mapping`, or pydantic complains.
from typing import Any, Optional, Union

import pydantic as pyd
from typing_extensions import Literal

from dve.metadata_parser import domain_types
from dve.metadata_parser.models import DatasetSpecification
from dve.metadata_parser.utilities import FieldTypeOption

# Type doesn't like Ellipsis. the type hint this is used in doesn't like type(Ellipsis)
EllipsisType = Any
"""3.9 compatible ellipsis type"""

TypeAnnotation = Union[type, Any]  # pylint: disable=invalid-name
"""An arbitrary type annotation"""
TableType = Literal["schema", "table"]
"""
The type of 'table' specified in the schema.

Values:

 - 'table': the table is a 'real' table, and should be expected
   to be created by the engine.
 - 'schema': the 'table' is a schema definition for a complex field
   within a table. These should be defined before the tables they
   are used in.

"""


@pyd.validate_arguments
def constr(
    *,
    strip_whitespace: bool = False,
    to_lower: bool = False,
    strict: bool = False,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    curtail_length: Optional[int] = None,
    regex: Optional[str] = None,
):
    """Wrapper around constr to enable argument validation"""
    return pyd.constr(
        strip_whitespace=strip_whitespace,
        to_lower=to_lower,
        strict=strict,
        min_length=min_length,  # type: ignore
        max_length=max_length,  # type: ignore
        curtail_length=curtail_length,  # type: ignore
        regex=regex,  # type: ignore
    )


STR_TO_PY_MAPPING: Mapping[str, FieldTypeOption] = {
    "constr": constr,
    "conint": pyd.validate_arguments(pyd.conint),
    "condate": pyd.validate_arguments(pyd.condate),
    "condecimal": pyd.validate_arguments(pyd.condecimal),
    "postcode": domain_types.Postcode,
    "nhsnumber": domain_types.NHSNumber,
    "permissivenhsno": domain_types.permissive_nhs_number(),
    "alphanumeric": domain_types.alphanumeric,
    "identifier": domain_types.identifier,
    "orgid": domain_types.OrgID,
    "formatteddatetime": domain_types.formatteddatetime,
    "conformatteddate": domain_types.conformatteddate,
    "reportingperiodstart": domain_types.reportingperiod(reporting_period_type="start"),
    "reportingperiodend": domain_types.reportingperiod(reporting_period_type="end"),
}


class ModelLoader(metaclass=ABCMeta):  # pylint: disable=too-few-public-methods
    """An abstract model loader."""

    @abstractmethod
    def __init__(self, contract_contents: dict[str, Any], type_map: Optional[dict] = None):
        raise NotImplementedError()

    @abstractmethod
    def generate_models(
        self, additional_validators: Optional[dict] = None
    ) -> dict[str, pyd.main.ModelMetaclass]:
        """Generates models from the instance schema.

        Args:
            additional_validators (Optional[dict], optional): Any validation rules aside from
            those described in the schema. Defaults to None [DEPRECATED]

        Returns:
            dict[str, model]: dict of table names to pydantic models

        """
        raise NotImplementedError()


class JSONtoPyd(ModelLoader):  # pylint: disable=too-few-public-methods
    """Generate pydantic model from a JSON schema."""

    def __init__(self, contract_contents: dict[str, Any], type_map: Optional[dict] = None):
        self.contract_contents = contract_contents
        self.type_map = deepcopy(type_map or STR_TO_PY_MAPPING)

    def generate_models(
        self, additional_validators: Optional[dict] = None
    ) -> dict[str, pyd.main.ModelMetaclass]:
        """Generates pydantic models from a loaded json file"""
        if additional_validators:
            warnings.warn("Ignoring additional validator functions")
        return DatasetSpecification(**self.contract_contents).load_models(self.type_map)
