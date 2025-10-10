"""XML schema/contract configuration."""

import warnings
from typing import List, Optional, Tuple

from pydantic import ValidationError
from pydantic.main import ModelMetaclass

from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import ContractContents, EntityName, Messages, Record
from dve.metadata_parser.exc import EntityNotFoundError
from dve.metadata_parser.model_generator import JSONtoPyd


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
        error_codes: Optional[dict] = None,
    ):
        self._model_definition = model_definition
        self._validators = validators
        self.entity_name = entity_name
        self._model: Optional[ModelMetaclass] = None
        self.error_codes = error_codes or {}

    def __reduce__(self):  # Don't attempt to pickle the Pydantic model.
        self._model = None
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
                        self.entity_name, record, err, self.error_codes
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
                    error_code=self.error_codes.get(error_location, ""),
                )
            )
        return messages
