"""Utilities to support reporting"""

import datetime as dt
import json
import logging
from collections.abc import Iterable
from itertools import chain
from multiprocessing import Queue
from threading import Thread
from typing import Optional, Union

import dve.parser.file_handling as fh
from dve.core_engine.exceptions import CriticalProcessingError
from dve.core_engine.loggers import get_logger
from dve.core_engine.message import UserMessage
from dve.core_engine.type_hints import URI, DVEStage, Messages


def get_feedback_errors_uri(working_folder: URI, step_name: DVEStage) -> URI:
    """Determine the location of json lines file containing all errors generated in a step"""
    return fh.joinuri(working_folder, "errors", f"{step_name}_errors.jsonl")


def get_processing_errors_uri(working_folder: URI) -> URI:
    """Determine the location of json lines file containing all processing
    errors generated from DVE run"""
    return fh.joinuri(working_folder, "errors", "processing_errors.jsonl")


def dump_feedback_errors(
    working_folder: URI,
    step_name: DVEStage,
    messages: Messages,
    key_fields: Optional[dict[str, list[str]]] = None,
) -> URI:
    """Write out captured feedback error messages."""
    if not working_folder:
        raise AttributeError("processed files path not passed")

    if not key_fields:
        key_fields = {}

    error_file = get_feedback_errors_uri(working_folder, step_name)
    processed = []

    for message in messages:
        if message.original_entity is not None:
            primary_keys = key_fields.get(message.original_entity, [])
        elif message.entity is not None:
            primary_keys = key_fields.get(message.entity, [])
        else:
            primary_keys = []

        error = message.to_dict(
            key_field=primary_keys,
            value_separator=" -- ",
            max_number_of_values=10,
            record_converter=None,
        )
        error["Key"] = conditional_cast(error["Key"], primary_keys, value_separator=" -- ")
        processed.append(error)

    with fh.open_stream(error_file, "a") as f:
        f.write("\n".join([json.dumps(rec, default=str) for rec in processed]) + "\n")
    return error_file


def dump_processing_errors(
    working_folder: URI, step_name: DVEStage, errors: list[CriticalProcessingError]
) -> URI:
    """Write out critical processing errors"""
    if not working_folder:
        raise AttributeError("processed files path not passed")
    if not step_name:
        raise AttributeError("step name not passed")
    if not errors:
        raise AttributeError("errors list not passed")

    error_file: URI = get_processing_errors_uri(working_folder)
    processed = []

    for error in errors:
        processed.append(
            {
                "step_name": step_name,
                "error_location": "processing",
                "error_level": "integrity",
                "error_message": error.error_message,
            }
        )

    with fh.open_stream(error_file, "a") as f:
        f.write("\n".join([json.dumps(rec, default=str) for rec in processed]) + "\n")

    return error_file


def load_feedback_messages(feedback_messages_uri: URI) -> Iterable[UserMessage]:
    """Load user messages from jsonl file"""
    if not fh.get_resource_exists(feedback_messages_uri):
        return
    with fh.open_stream(feedback_messages_uri) as errs:
        yield from (UserMessage(**json.loads(err)) for err in errs.readlines())


def load_all_error_messages(error_directory_uri: URI) -> Iterable[UserMessage]:
    "Load user messages from all jsonl files"
    return chain.from_iterable(
        [
            load_feedback_messages(err_file)
            for err_file, _ in fh.iter_prefix(error_directory_uri)
            if err_file.endswith(".jsonl")
        ]
    )


class BackgroundMessageWriter:
    """Controls batch writes to error jsonl files"""

    def __init__(
        self,
        working_directory: URI,
        dve_stage: DVEStage,
        key_fields: Optional[dict[str, list[str]]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._working_directory = working_directory
        self._dve_stage = dve_stage
        self._feedback_message_uri = get_feedback_errors_uri(
            self._working_directory, self._dve_stage
        )
        self._key_fields = key_fields
        self.logger = logger or get_logger(type(self).__name__)
        self._write_thread = None
        self._queue = Queue()

    @property
    def write_queue(self) -> Queue:  # type: ignore
        """Queue for storing batches of messages to be written"""
        return self._queue

    @property
    def write_thread(self) -> Thread:  # type: ignore
        """Thread to write batches of messages to jsonl file"""
        if not self._write_thread:
            self._write_thread = Thread(target=self._write_process_wrapper)
        return self._write_thread

    def _write_process_wrapper(self):
        """Wrapper for dump feedback errors to run in background process"""
        while True:
            if msgs := self.write_queue.get():
                dump_feedback_errors(
                    self._working_directory, self._dve_stage, msgs, self._key_fields
                )
            else:
                break

    def __enter__(self) -> "BackgroundMessageWriter":
        self.write_thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.logger.exception(
                "Issue occured during background write process:",
                exc_info=(exc_type, exc_value, traceback),
            )
        self.write_queue.put(None)
        self.write_thread.join()


def conditional_cast(value, primary_keys: list[str], value_separator: str) -> Union[list[str], str]:
    """Determines what to do with a value coming back from the error list"""
    if isinstance(value, list):
        casts = [
            conditional_cast(val, primary_keys, value_separator) for val in value
        ]  # type: ignore
        return value_separator.join(
            [f"{pk}: {id}" if pk else "" for pk, id in zip(primary_keys, casts)]
        )
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dict):
        return ""
    return str(value)
