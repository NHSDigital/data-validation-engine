"""Exceptions emitted by the pipeline."""

from typing import Iterator

from dve.core_engine.backends.implementations.spark.types import SparkEntities
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.type_hints import Messages


class CriticalProcessingError(ValueError):
    """An exception emitted if critical errors are received."""

    def __init__(
        self, error_message: str, *args: object, messages: Messages, entities: SparkEntities
    ) -> None:
        super().__init__(error_message, *args)
        self.error_messsage = error_message
        """The error message explaining the critical processing error."""
        self.messages = messages
        """The messages gathered at the time the error was emitted."""
        self.entities = entities
        """The entities as they exist at the time the error was emitted."""

    @property
    def critical_messages(self) -> Iterator[FeedbackMessage]:
        """Critical messages which caused the processing error."""
        yield from filter(lambda message: message.is_critical, self.messages)


class EntityTypeMismatch(TypeError):
    """An exception emitted if entity type outputs from two collaborative objects are different."""
