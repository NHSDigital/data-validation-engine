import json
from typing import Optional
from dve.core_engine.exceptions import CriticalProcessingError
from dve.core_engine.type_hints import URI, Messages
import dve.parser.file_handling as fh
from dve.reporting.error_report import conditional_cast

def dump_feedback_errors(
    working_folder: URI,
    step_name: str,
    messages: Messages,
    key_fields: Optional[dict[str, list[str]]] = None,
):
    """Write out captured feedback error messages."""
    if not working_folder:
        raise AttributeError("processed files path not passed")

    if not key_fields:
        key_fields = {}

    errors = fh.joinuri(working_folder, "errors", f"{step_name}_errors.json")
    processed = []

    for message in messages:
        primary_keys: list[str] = key_fields.get(message.entity if message.entity else "", [])
        error = message.to_dict(
            key_field=primary_keys,
            value_separator=" -- ",
            max_number_of_values=10,
            record_converter=None,
        )
        error["Key"] = conditional_cast(error["Key"], primary_keys, value_separator=" -- ")
        processed.append(error)

    with fh.open_stream(errors, "a") as f:
        json.dump(
            processed,
            f,
            default=str,
        )

def dump_processing_errors(
    working_folder: URI,
    step_name: str,
    errors: list[CriticalProcessingError]
):
    """Write out critical processing errors"""
    if not working_folder:
        raise AttributeError("processed files path not passed")
    if not step_name:
        raise AttributeError("step name not passed")
    if not errors:
        raise AttributeError("errors list not passed")

    error_file: URI = fh.joinuri(working_folder, "errors", f"processing_errors.json")
    processed = []

    for error in errors:
        processed.append({"step_name": step_name,
                          "error_location": "processing",
                          "error_level": "integrity",
                          "error_message": error.error_message})

    with fh.open_stream(error_file, "a") as f:
        json.dump(
            processed,
            f,
            default=str,
        )