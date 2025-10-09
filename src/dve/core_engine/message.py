"""Functionality to represent messages."""

import datetime as dt
import json
from decimal import Decimal
from typing import Any, Callable, ClassVar, Dict, List, Optional, Set, Type, Union

from pydantic import ValidationError, validator
from pydantic.dataclasses import dataclass

from dve.core_engine.constants import ROWID_COLUMN_NAME
from dve.core_engine.templating import ENVIRONMENT
from dve.core_engine.type_hints import (
    EntityName,
    ErrorCategory,
    ErrorCode,
    FailureType,
    Field,
    Messages,
    MessageTuple,
    Record,
)

INTEGRITY_ERROR_CODES: Set[str] = {"blockingsubmission"}
"""
Error types which should raise integrity errors if encountered.

"""
SUBMISSION_ERROR_CODES: Set[str] = {"submission"}
"""
Error types which should raise submission errors if encountered.

"""


class Config:  # pylint: disable=too-few-public-methods
    """`pydantic` configuration options."""

    arbitrary_types_allowed = True


@dataclass(config=Config, eq=True)
class FeedbackMessage:  # pylint: disable=too-many-instance-attributes
    """Information which affects processing and needs to be feeded back."""

    entity: Optional[EntityName]
    """The entity that the message pertains to (if applicable)."""
    record: Optional[Record]
    """The record the message pertains to, if applicable."""
    failure_type: FailureType = "record"
    """
    The nature of the failure.

    Failures are processed as follows:
     - 'record': the record has violated a constraint.
       If not `is_informational`, the record will be excluded from the output.
     - 'integrity': the integrity of the data has been affected.
       If not `is_informational`, the submission has failed and further
       processing is not possible as the integrity of the data has been
       judged to be too damaged
     - 'submission': the submission has violated a business-rule constraint.
       If not `is_informational`, the submission has failed but processing can
       still be completed (i.e. filters and joins can still be applied).

    """
    is_informational: bool = False
    """Whether the message is simply for information or has affected the outputs."""
    error_type: Optional[str] = None
    """The name of the type of the error."""
    error_location: Optional[str] = None
    """The location of the error within the record."""
    error_message: Optional[str] = None
    """The error message."""
    error_code: Optional[str] = None
    """ETOS Error code for the error."""
    reporting_field: Union[str, List[str], None] = None
    """The field that the error pertains to."""
    reporting_field_name: Optional[str] = None
    """
    DEPRECATED: An optional override for the reporting field, which is used in
    the output for the name.

    """
    value: Optional[Any] = None
    """The value that caused the error."""
    category: Optional[ErrorCategory] = None
    """The category of the error."""

    HEADER: ClassVar[List[str]] = [
        "Entity",
        "Key",
        "FailureType",
        "Status",
        "ErrorType",
        "ErrorLocation",
        "ErrorMessage",
        "ErrorCode",
        "ReportingField",
        "Value",
        "Category",
    ]
    """The header that should be written to CSV."""

    @validator("reporting_field")
    # pylint: disable=no-self-argument
    def _split_reporting_field(cls, value) -> Union[List[str], str, None]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                lst = json.loads(value.replace("'", '"'))
                if len(lst) == 1:
                    return lst[0]
                return lst
            except json.JSONDecodeError:
                return value
        return None

    @validator("error_location", pre=True)
    # pylint: disable=no-self-argument
    def _validate_error_location(cls, value: Any) -> Optional[str]:
        """Format error location to a string."""
        if value is None:
            return None  # pragma: no cover

        if isinstance(value, str):
            return value

        # This would be triggered for pydantic errors raised by per-item list/dict
        # validations.
        if isinstance(value, (tuple, list)) and len(value) == 1:  # pragma: no cover
            return str(value[0])

        return str(value)

    @validator("record")
    def _strip_rowid(  # pylint: disable=no-self-argument
        cls, value: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Strip the row ID column from the record, if present."""
        if isinstance(value, dict):
            value.pop(ROWID_COLUMN_NAME, None)
        return value

    @property
    def is_critical(self) -> bool:
        """Whether the error is unrecoverable."""
        return self.failure_type == "integrity" and not self.is_informational

    @classmethod
    def from_pydantic_error(
        cls: Type["FeedbackMessage"],
        entity: str,
        record: Record,
        error: ValidationError,
        error_codes: Dict[Field, ErrorCode],
    ) -> Messages:
        """Create messages from a `pydantic` validation error."""
        messages: Messages = []
        for error_dict in error.errors():
            error_type = error_dict["type"]
            msg = "is invalid"
            if "none.not_allowed" in error_type or "value_error.missing" in error_type:
                category = "Blank"
                msg = "cannot be blank"
            else:
                category = "Bad value"
            error_code = error_type
            if "." in error_code:
                error_code = error_code.split(".", 1)[-1]

            if error_code in INTEGRITY_ERROR_CODES:
                failure_type: FailureType = "integrity"
            elif error_code in SUBMISSION_ERROR_CODES:
                failure_type = "submission"
            else:
                failure_type = "record"

            is_informational = False
            if error_code.endswith("warning"):
                is_informational = True
            messages.append(
                cls(
                    entity=entity,
                    record=record,
                    failure_type=failure_type,
                    is_informational=is_informational,
                    error_type=error_type,
                    error_location=error_dict["loc"],  # type: ignore
                    error_message=msg,
                    reporting_field=error_dict["loc"][-1],  # type: ignore
                    category=category,  # type: ignore
                    error_code=error_codes.get(error_dict["loc"][-1]),  # type: ignore
                )
            )

        return messages

    def to_row(
        self,
        key_field: Union[str, list, None] = None,
        max_number_of_values: Optional[int] = None,
        value_separator=", ",
        record_converter: Optional[Callable] = repr,
    ) -> MessageTuple:  # pragma: no cover
        """Create a reporting row from the message."""
        if not self.record:
            key = None
        elif not key_field:
            key = record_converter(self.record) if record_converter else self.record
        else:
            try:
                key = self._extract_key(key_field)
            except KeyError:
                key = record_converter(self.record) if record_converter else self.record
        reporting_field = self.reporting_field
        if not self.reporting_field and self.error_location:
            reporting_field = self.error_location

        value = self._extract_value(reporting_field, max_number_of_values, value_separator)

        if isinstance(reporting_field, list):
            reporting_field = ", ".join(reporting_field)

        if self.record and self.error_message:
            error_message: Optional[str] = ENVIRONMENT.from_string(self.error_message).render(
                **self.__dict__
            )
        else:
            error_message = self.error_message

        return (
            self.entity,
            key,
            self.failure_type,
            "informational" if self.is_informational else "error",
            self.error_type,
            self.error_location,
            error_message,
            self.error_code,
            self.reporting_field_name or reporting_field,
            value,
            self.category,
        )

    def _extract_key(self, key_field):
        if isinstance(key_field, list):
            return [self._extract_key(field) for field in key_field]
        return self.record[key_field]

    def _extract_value(
        self, reporting_field, max_number_of_values: Optional[int], value_separator: str
    ):
        value = None
        if self.record is None:
            return value

        loc = self.error_location
        if loc:
            # this is because, for some reason, even if error_location is set to be
            # a List[str] or Tuple[str] and set smart_unions to be True, it still
            #  always comes in as a string
            loc_items: List[Union[str, int]] = [
                field if not field.isnumeric() else int(field.strip())
                for field in loc.strip("()").replace("'", "").replace(" ", "").split(",")
            ]
        else:
            loc_items = []

        if len(loc_items) == 1 and isinstance(reporting_field, str):
            value = self.record.get(
                reporting_field,
                self.record.get(loc_items[0]),  # type: ignore
            )

        if isinstance(value, list):
            end_text = ""
            value = sorted(value, key=_sort_values)
            if max_number_of_values and len(value) > max_number_of_values:
                end_text = f"{value_separator}only first {max_number_of_values} shown"
            value = self._string_values(
                reporting_field, value[:max_number_of_values], value_separator
            )
            value = f"{value}{end_text}"

        elif isinstance(value, dict):
            value = value.get(reporting_field)

        elif isinstance(reporting_field, list):
            value = self._multi_reporting_fields(
                reporting_field, max_number_of_values, value_separator, loc, loc_items
            )

        elif len(loc_items) > 1:
            value = self.record.get(loc_items[0], None)  # type: ignore
            for item in loc_items[1:]:
                try:
                    value = value[item]  # type: ignore
                except (KeyError, TypeError, IndexError):
                    value = None
                    break
        return self._cond_str(value)

    def _multi_reporting_fields(
        self,
        reporting_field: List[str],
        max_number_of_values: Optional[int],
        value_separator: str,
        loc: Optional[str],
        loc_items: List[Union[str, int]],
    ) -> Any:
        value: Any

        if len(loc_items) == 1 and (loc and not loc.startswith("Filter")):
            record = self.record.get(loc_items[0])  # type: ignore
        else:
            record = self.record
        if isinstance(record, list):
            end_text = ""
            values = []
            for records in record:
                values.append(
                    ", ".join(
                        [
                            f"{field}={self._cond_str(records.get(field, None))}"
                            for field in reporting_field
                        ]
                    )
                )
            values = sorted(values, key=_sort_values)
            if len(values) > 10:
                end_text = f"{value_separator}only first {max_number_of_values} shown"
            value = self._string_values(
                reporting_field,
                values[:max_number_of_values],
                value_separator,  # type: ignore
            )
            value = f"{value}{end_text}"
        elif isinstance(record, dict):
            value = ", ".join(
                sorted(
                    [f"{field}={self._cond_str(record.get(field, None))}" for field in reporting_field]  # type: ignore # pylint:disable=line-too-long
                )
            )
        else:
            value = record
        return value

    @staticmethod
    def _cond_str(value: Any, str_none: bool = False) -> Optional[str]:
        """Ensure that datetimes are in an isoformat, and that Nones are returned as None
        unless explicitly stated otherwise
        """
        if isinstance(value, str):
            return value

        if value is None:
            return "None" if str_none else None

        if isinstance(value, dt.datetime):
            return value.isoformat()

        if isinstance(value, Decimal):
            return f"{value.normalize():f}"

        return str(value)

    def _string_values(
        self, reporting_field: Union[List[str], str], values: List[Any], value_separator: str
    ) -> str:
        if all(isinstance(item, dict) for item in values) and isinstance(reporting_field, str):
            values = [
                self._cond_str(
                    item.get(reporting_field),
                    str_none=True,
                )  # type: ignore
                for item in values
            ]
        else:
            values = [
                self._cond_str(
                    item,
                    str_none=True,
                )  # type: ignore
                for item in values
            ]
        value: str = value_separator.join(filter(None, values))
        return value

    def to_dict(
        self,
        key_field: Union[str, list, None] = None,
        max_number_of_values: Optional[int] = None,
        value_separator: str = ", ",
        record_converter: Optional[Callable] = repr,
    ) -> Dict[str, Any]:
        """Create a reporting dict from the message."""
        return dict(
            zip(
                self.HEADER,
                self.to_row(key_field, max_number_of_values, value_separator, record_converter),
            )
        )

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)


def _sort_values(value):
    """Sorting function for ensuring messages always come out in the same order"""
    if isinstance(value, dict):
        return list(value.values())[0]
    if isinstance(value, list):
        return _sort_values(value[0])
    return value
