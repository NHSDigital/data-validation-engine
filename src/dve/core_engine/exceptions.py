"""Exceptions emitted by the pipeline."""

import traceback
from typing import Optional


class CriticalProcessingError(ValueError):
    """An exception emitted if critical errors are received."""

    def __init__(
        self,
        error_message: str,
        *args: object,
        messages: Optional[list[str]] = None,
    ) -> None:
        super().__init__(error_message, *args)
        self.error_message = error_message
        """The error message explaining the critical processing error."""
        self.messages = messages
        """The stacktrace for the messages."""

    @classmethod
    def from_exception(cls, exc: Exception):
        """Create from broader exception, for recording in processing errors"""
        return cls(error_message=repr(exc), messages=traceback.format_exception(exc))


class EntityTypeMismatch(TypeError):
    """An exception emitted if entity type outputs from two collaborative objects are different."""
