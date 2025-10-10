"""Tools to get common values from the context."""

from collections.abc import MutableMapping
from pathlib import Path
from typing import Dict, List, Set, Type, TypeVar, Union
from uuid import uuid4

from behave.runner import Context  # type: ignore
from typing_extensions import Literal

from dve.core_engine.backends.implementations.spark.types import SparkEntities
from dve.core_engine.engine import CoreEngine
from dve.core_engine.models import SubmissionInfo
from dve.core_engine.type_hints import EntityName, Messages
from dve.parser.type_hints import URI
from dve.pipeline.pipeline import BaseDVEPipeline
from dve.pipeline.utils import SubmissionStatus

__all__ = [
    "get_temp_dir",
    "get_submission_info",
    "set_submission_info",
    "get_processing_location",
    "set_processing_location",
    "get_pipeline",
    "set_pipeline",
    "get_submission_status",
    "set_submission_status",
    "get_failed_file_transformation",
    "set_failed_file_transformation",
]


T = TypeVar("T")

Stage = Literal[
    "FileTransformation",
    "DataContract",
    "BusinessRules",
    "ErrorReports",
]
"""Literal strings indicating stages within the pipeline."""
Stages = Set[Stage]
"""A set of stages indicating steps that have already completed."""


def _get_var(context: Context, attribute: str, expected_type: Type[T]) -> T:
    """Get a variable from the behave context."""
    var = getattr(context, attribute, None)
    if var is None:
        raise AttributeError(f"{attribute!r} not set up on the context.")
    if not isinstance(var, expected_type):
        raise TypeError(
            f"Context variable {attribute!r} is not a `{expected_type.__name__}` "
            + f"instance: got {var}"
        )
    return var


def _set_var(context: Context, attribute: str, expected_type: Type[T], var: T):
    """Set a variable on the behave context."""
    if not isinstance(var, expected_type):
        raise TypeError(f"Provided value is not a `{expected_type.__name__}` instance, got {var}")

    setattr(context, attribute, var)


def get_temp_dir(context: Context) -> Path:
    """Get the path to the temp dir for the scenario."""
    return _get_var(context, "temp_dir", Path)


def get_submission_info(context: Context) -> SubmissionInfo:
    """Get the submission info for the context."""
    return _get_var(context, "submission_info", SubmissionInfo)


def set_submission_info(context: Context, sub_info: SubmissionInfo):
    """Set the submission info for the context."""
    # Set this so we can know what it ends up being in the audit table.
    _set_var(context, "submission_info", SubmissionInfo, sub_info)


def get_processing_location(context: Context):
    """Get the processing location for the context."""
    return _get_var(context, "processing_location", Path)


def set_processing_location(context: Context, processing_location: Path):
    """Set the processing location for the context."""
    _set_var(context, "processing_location", Path, processing_location)


def get_pipeline(context: Context) -> BaseDVEPipeline:
    """Get the pipeline for the context."""
    return _get_var(context, "pipeline", BaseDVEPipeline)


def set_pipeline(context: Context, pipeline: BaseDVEPipeline):
    """Set the pipeline to run the DVE for the context"""
    _set_var(context, "pipeline", BaseDVEPipeline, pipeline)


def get_submission_status(context: Context) -> SubmissionStatus:
    """Get the pipeline for the context."""
    return _get_var(context, "submission_status", SubmissionStatus)


def set_submission_status(context: Context, submission_status: SubmissionStatus):
    """Set the pipeline to run the DVE for the context"""
    _set_var(context, "submission_status", SubmissionStatus, submission_status)


def get_failed_file_transformation(context: Context) -> SubmissionInfo:
    """Get the failed transformations on failed file transformation."""
    return _get_var(context, "failed_transformation", SubmissionInfo)


def set_failed_file_transformation(context: Context, sub_info: SubmissionInfo):
    """Capture the failed submissions on failed file transformation."""
    _set_var(context, "failed_transformation", SubmissionInfo, sub_info)


def register_temp_table(context: Context, table_name: str):
    """Register a temp table to be cleared after the scenario."""
    temp_table_list: List[str] = getattr(context, "temp_tables")
    temp_table_list.append(table_name)
