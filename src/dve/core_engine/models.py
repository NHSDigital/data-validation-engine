"""Models used by core_engine

Models for parametersied pipeline execution - also used by API service
"""

import datetime as dt
import json
import os
import uuid
from collections.abc import MutableMapping
from pathlib import Path, PurePath
from typing import Any, Optional

from pydantic import UUID4, BaseModel, Field, FilePath, root_validator, validator

from dve.core_engine.backends.metadata.contract import ReaderConfig
from dve.core_engine.type_hints import EntityName, ProcessingStatus, SubmissionResult
from dve.metadata_parser.models import DatasetSpecification, EntitySpecification
from dve.parser.file_handling import open_stream, resolve_location
from dve.parser.file_handling.service import get_file_stem
from dve.parser.type_hints import Extension, Location


class AuditRecord(BaseModel):
    """Record to add to audit table"""

    submission_id: str
    """Unique id of the submission"""
    date_updated: Optional[dt.date] = None
    """The date the record was added to the table"""
    time_updated: Optional[dt.datetime] = Field(default_factory=dt.datetime.now)
    """The timestamp the record was added to the table"""

    @root_validator(allow_reuse=True)
    def populate_date_updated(cls, values):  # pylint: disable=no-self-argument
        """Add date_updated from time_updated value"""
        values["date_updated"] = values["time_updated"].date()
        return values


class SubmissionInfoMismatchWarning(UserWarning):
    """Emitted when the submission info does not match the filename."""


class SubmissionInfo(AuditRecord):
    """Submission metadata"""

    dataset_id: str
    """The dataset that the submission relates to."""
    file_name: str
    """The name of the submitted file."""
    file_extension: str
    """The extension of the file received."""
    submission_method: str = None  # type: ignore
    """The method that the file was submitted"""
    submitting_org: str = None  # type: ignore
    """The organisation who submitted the file."""
    reporting_period: str = None  # type: ignore
    """The reporting period the submission relates to."""
    file_size: int = None  # type: ignore
    """The size (in bytes) of the file received."""
    datetime_received: dt.datetime = None  # type: ignore
    """The datetime the SEFT transfer finished."""

    @validator("file_name")
    def _ensure_metadata_extension_removed(cls, filename):  # pylint: disable=no-self-argument
        path = PurePath(filename)
        return path.stem

    @validator("file_extension")
    def _ensure_just_file_stem(cls, extension: str):  # pylint: disable=no-self-argument
        if "." in extension:
            return extension.split(".")[-1]
        return extension

    @property
    def file_name_with_ext(self):
        """Return file name with extension."""
        return f"{self.file_name}.{self.file_extension}"

    @classmethod
    def from_metadata_file(cls, submission_id: str, metadata_uri: Location):
        """Create a submission metadata instance from DVE metadata file."""
        metadata_uri = resolve_location(metadata_uri)
        with open_stream(metadata_uri, "r", "utf-8") as stream:
            try:
                metadata_dict: dict[str, Any] = json.load(stream)
            except json.JSONDecodeError as exc:
                raise ValueError(f"File found at {metadata_uri!r} is not valid JSON") from exc

        if isinstance(metadata_dict, list):
            raise ValueError(f"File found at {metadata_uri!r} is not a JSON mapping")

        if not metadata_dict.get("file_name"):
            metadata_dict["file_name"] = get_file_stem(metadata_uri)

        return cls(submission_id=submission_id, **metadata_dict)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SubmissionInfo):
            raise NotImplementedError("Unable to determine equality if not a SubmissionInfo object")
        _exclude = ["date_updated", "time_updated"]
        return {k: v for k, v in self.dict().items() if k not in _exclude} == {
            k: v for k, v in other.dict().items() if k not in _exclude
        }


class SubmissionStatisticsRecord(AuditRecord):
    """Record detailing key metrics from dve processing"""

    record_count: Optional[int]
    """Count of records in the submitted file"""
    number_record_rejections: Optional[int]
    """Number of record rejections raised following validation"""
    number_warnings: Optional[int]
    """Number of warnings raised following validation"""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SubmissionStatisticsRecord):
            raise NotImplementedError(
                "Unable to determine equality if not a SubmissionStatisticsRecord object"
            )  # pylint: disable=line-too-long
        _exclude = ["date_updated", "time_updated"]
        return {k: v for k, v in self.dict().items() if k not in _exclude} == {
            k: v for k, v in other.dict().items() if k not in _exclude
        }


class TransferRecord(AuditRecord):
    """A record detailing extracts sent following dve processing"""

    report_name: str
    """The type of extract sent"""
    transfer_id: str
    """The DPS transfer id for the extract sent"""
    transfer_method: Optional[str] = None
    """What transfer mechanism was used to send the extract"""
    recipient: Optional[str] = None
    """The recipient of the extract"""


class ProcessingStatusRecord(AuditRecord):
    """A record detailing what phase of processing a submission is"""

    processing_status: ProcessingStatus
    """The processing status of the submission"""
    job_run_id: Optional[int]
    """The run id of the databricks job used to process the submission"""
    submission_result: Optional[SubmissionResult]
    """Whether the file validation was a success or failure"""


class EngineRun(BaseModel):
    """The parameters needed to execute a core engine pipeline run
    Basic level of validation
    """

    submission_id: UUID4 = Field(default_factory=uuid.uuid4)
    dataset_config_path: Path
    output_prefix: Path = Path("./output")

    # TODO: What if we want to set an alt/override output prefix
    #  and not have the submission_id appended to it?
    @validator("output_prefix")
    def _set_output_path(cls, prefix, values: dict):  # pylint: disable=E0213
        v_id = values.get("submission_id")
        if v_id:
            return os.path.join(prefix, str(v_id))
        return prefix


class EngineRunValidation(EngineRun):
    """The parameters needed to execute a core engine pipeline run
    Additional validation for paths which point to valid files or directories
    """

    dataset_config_path: FilePath


class ConcreteEntity(EntitySpecification, arbitrary_types_allowed=True):
    """An entity which has a configured reader and (possibly) a key field."""

    reader_config: dict[Extension, ReaderConfig]
    """A reader configuration for the entity."""
    key_field: Optional[str] = None
    """An optional key field to use for the entity."""
    reporting_fields: Optional[list[str]] = None

    @validator("reporting_fields", pre=True)
    def _ensure_list(cls, value: Optional[str]) -> Optional[list[str]]:  # pylint: disable=E0213
        """Ensure the reporting fields are a list."""
        if value is None:
            return None
        return value if isinstance(value, list) else [value]


class ConcreteDatasetSpecification(DatasetSpecification):
    """A dataset with concrete entities."""

    datasets: MutableMapping[EntityName, ConcreteEntity]  # type: ignore
    """Datasets which can be read from the input files."""
