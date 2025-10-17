"""XML schema/contract configuration."""

import warnings
from typing import Dict, List, Optional, Tuple

from pydantic import ValidationError
from pydantic.main import ModelMetaclass

from dve.core_engine.message import DEFAULT_ERROR_DETAIL, DataContractErrorDetail, FeedbackMessage
from dve.core_engine.type_hints import ContractContents, EntityName, ErrorCategory, Messages, Record
from dve.metadata_parser.exc import EntityNotFoundError
from dve.metadata_parser.model_generator import JSONtoPyd
from dve.parser.type_hints import FieldName


class RowValidator:
    """Picklable class to pass into process pools for row data contract
    validation.

    This is a callable class which enables validation of records
    in a multiprocessing-compliant way by re-generating the pydantic
    models (which cannot be serialised) within each process.

    """

    def __init__(
        self,
        model_definition: ContractContents,
        entity_name: EntityName,
        validators: Optional[dict] = None,
        error_info: Optional[dict] = None,
    ):
        self._model_definition = model_definition
        self._validators = validators
        self.entity_name = entity_name
        self._model: Optional[ModelMetaclass] = None
        self._error_info = error_info or {}
        self._error_details: Optional[Dict[FieldName,
                                           Dict[ErrorCategory,
                                                DataContractErrorDetail]]] = None

    def __reduce__(self):  # Don't attempt to pickle Pydantic models.
        self._model = None
        self._error_details = None
        return super().__reduce__()

    @property
    def model(self) -> ModelMetaclass:
        """The loaded pydantic model for the entity."""
        if not self._model:
            models = JSONtoPyd(self._model_definition).generate_models(
                additional_validators=self._validators
            )
            model = models.get(self.entity_name)

            if not model:
                raise EntityNotFoundError(
                    f"Given entity ({self.entity_name!r}) does not exist in the schema"
                )
            self._model = model
        return self._model
    
    @property
    def error_details(self) -> Dict[FieldName,
                                  Dict[ErrorCategory, DataContractErrorDetail]]:
        """Custom error code and message mapping for contract phase"""
        if not self._error_details:
            _error_details = {field: {err_type: DataContractErrorDetail(**detail) 
                           for err_type, detail in err_details.items()} 
                           for field, err_details in self._error_info.items()}
            self._error_details = _error_details
        return self._error_details
        
    def __call__(self, record: Record) -> Tuple[Optional[Record], Messages]:
        """Take a record, returning a validated record (is successful) and a list of messages."""
        with warnings.catch_warnings(record=True) as caught_warnings:
            messages: Messages = []
            try:
                # pylint: disable=not-callable
                validated: Record = self.model(**record).dict()
            except ValidationError as err:
                # we still want to report warnings
                # when a record is invalid
                if caught_warnings:
                    messages.extend(self.handle_warnings(record, caught_warnings))
                messages.extend(
                    FeedbackMessage.from_pydantic_error(
                        self.entity_name, record, err, self.error_details
                    )
                )
                return None, messages

            if not caught_warnings:
                return validated, messages

            messages.extend(self.handle_warnings(record, caught_warnings))

            return validated, messages

    def handle_warnings(self, record, caught_warnings) -> List[FeedbackMessage]:
        """Handle warnings from the pydantic validation."""
        messages: List[FeedbackMessage] = []
        for warning_message in caught_warnings:
            warning = warning_message.message

            if isinstance(warning, str):
                error_message = warning
                error_location = None
            else:
                for attr in ("message", "msg"):
                    message_attr_value = getattr(warning, attr, None)
                    if message_attr_value is not None:
                        error_message = str(message_attr_value)
                        break
                else:
                    error_message = str(warning)

                for attr in ("location", "loc"):
                    location_attr_value = getattr(warning, attr, None)
                    if location_attr_value is not None:
                        error_location = location_attr_value
                        break
                else:
                    error_location = None
                error_code = self.error_details.get(
                    error_location, DEFAULT_ERROR_DETAIL
                    ).get("Wrong Format").error_code

            messages.append(
                FeedbackMessage(
                    entity=self.entity_name,
                    record=record,
                    failure_type="record",
                    is_informational=True,
                    error_type=warning_message.category.__name__,
                    error_location=error_location,
                    error_message=error_message,
                    category="Wrong format",
                    error_code=error_code,
                )
            )
        return messages
