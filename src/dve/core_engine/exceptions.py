"""Exceptions emitted by the pipeline."""

from collections.abc import Iterator
from typing import Optional

from dve.core_engine.backends.types import Entities
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import Messages


class CriticalProcessingError(ValueError):
    """An exception emitted if critical errors are received."""

    def __init__(
        self,
        error_message: str,
        *args: object,
        messages: Optional[Messages],
        entities: Optional[Entities] = None
    ) -> None:
        super().__init__(error_message, *args)
        self.error_message = error_message
        """The error message explaining the critical processing error."""
        self.messages = messages
        """The messages gathered at the time the error was emitted."""
        self.entities = entities
        """The entities as they exist at the time the error was emitted."""

    @property
    def critical_messages(self) -> Iterator[FeedbackMessage]:
        """Critical messages which caused the processing error."""
        yield from filter(lambda message: message.is_critical, self.messages)  # type: ignore

    @classmethod
    def from_exception(cls, exc: Exception):
        """Create from broader exception, for recording in processing errors"""
        return cls(error_message=repr(exc), entities=None, messages=[])


class EntityTypeMismatch(TypeError):
    """An exception emitted if entity type outputs from two collaborative objects are different."""
