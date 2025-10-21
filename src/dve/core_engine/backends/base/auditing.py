# pylint: disable=C0209
"""Base auditing objects and managers for use of DVE services"""
import multiprocessing
import operator
import threading
import time
from abc import abstractmethod
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from multiprocessing import Queue as ProcessQueue
from queue import Queue as ThreadQueue
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from pydantic import ValidationError, validate_arguments
from typing_extensions import Literal, get_origin

from dve.core_engine.models import (
    AuditRecord,
    ProcessingStatusRecord,
    SubmissionInfo,
    SubmissionStatisticsRecord,
    TransferRecord,
)
from dve.core_engine.type_hints import (
    BinaryComparator,
    ExecutorType,
    ProcessingStatus,
    QueueType,
    SubmissionResult,
)

AuditReturnType = TypeVar("AuditReturnType")  # pylint: disable=invalid-name


@dataclass
class FilterCriteria:
    """Stores information about a filter criteria to be applied to audit records"""

    field: str
    comparison_value: Any
    operator_: Callable = operator.eq
    operator_mapping: ClassVar[dict[BinaryComparator, str]] = {
        operator.eq: "=",
        operator.ne: "!=",
        operator.lt: "<",
        operator.le: "<=",
        operator.gt: ">",
        operator.ge: ">=",
        operator.contains: "in",
    }

    @staticmethod
    def _quote_comp_val(val: Any) -> str:
        if isinstance(val, (list, tuple, set)):
            return str(tuple(val))
        return f"'{val}'"

    def to_sql(self) -> str:
        """Create sql expression from filter criteria"""
        return "{} {} {}".format(
            self.field,
            self.operator_mapping[self.operator_],
            self._quote_comp_val(self.comparison_value),
        )

    def __str__(self) -> str:
        return self.to_sql()


@dataclass
class OrderCriteria:
    """Stores information about ordering criteria to be applied to audit records"""

    field_name: str
    descending: bool = False

    def to_sql(self) -> str:
        """Create sql expression from order criteria"""
        return self.field_name + (" DESC" if self.descending else "")

    def __str__(self) -> str:
        return self.to_sql()


class BaseAuditor(Generic[AuditReturnType]):
    """Base auditor object - defines structure for implementations to use
    in conjunction with AuditingManager"""

    def __init__(self, name: str, record_type: type[AuditRecord]):
        self._name = name
        self._record_type = record_type

    @property
    def schema(self) -> dict[str, type]:
        """Determine python schema of auditor"""
        return {
            fld: str if get_origin(mdl.type_) == Literal else mdl.type_
            for fld, mdl in self._record_type.__fields__.items()
        }

    @staticmethod
    def normalise_filter(filter_condition: FilterCriteria):
        """Ensure filter criteria is converted to implementation specific format"""
        raise NotImplementedError()

    @staticmethod
    def normalise_order(order_condition: OrderCriteria):
        """Ensure order criteria is converted to implementation specific format"""
        raise NotImplementedError()

    @staticmethod
    def normalise_field(field: str):
        """Ensure field is converted to implementation specific format"""
        raise NotImplementedError()

    @abstractmethod
    def conv_to_records(self, recs: AuditReturnType) -> Iterable[AuditRecord]:
        """Convert the AuditReturnType for the implementation to an iterable of pydantic models"""
        raise NotImplementedError()

    @abstractmethod
    def conv_to_entity(self, recs: list[AuditRecord]) -> AuditReturnType:
        """Convert the list of pydantic models to an entity for use in pipelines"""
        raise NotImplementedError()

    @abstractmethod
    def add_records(self, records: Iterable[dict[str, Any]]):
        """Add audit records to the Auditor"""
        raise NotImplementedError()

    @abstractmethod
    def retrieve_records(
        self, filter_criteria: list[FilterCriteria], data: Optional[AuditReturnType] = None
    ) -> AuditReturnType:
        """Retrieve audit records from the Auditor"""
        raise NotImplementedError()

    def get_most_recent_records(
        self,
        order_criteria: list[OrderCriteria],
        partition_fields: Optional[list[str]] = None,
        pre_filter_criteria: Optional[list[FilterCriteria]] = None,
    ) -> AuditReturnType:
        """Retrieve the most recent records, defined by the ordering criteria
        for each partition combination"""
        raise NotImplementedError()


AuditorType = TypeVar("AuditorType", bound=BaseAuditor)  # pylint: disable=C0103
SubmissionMetadata = TypeVar("SubmissionMetadata", bound=SubmissionInfo)


class BaseAuditingManager(
    Generic[AuditorType, AuditReturnType]
):  # pylint: disable=too-many-public-methods
    """Manager of auditors - controls adding records to and querying from the
    audit objects supplied"""

    def __init__(
        self,
        processing_status: AuditorType,
        submission_info: AuditorType,
        submission_statistics: AuditorType,
        transfers: AuditorType,
        pool: Optional[ExecutorType] = None,
    ):
        """Audit manager to handle writing of audit information to auditors."""
        self._processing_status = processing_status
        self._submission_info = submission_info
        self._submission_statistics = submission_statistics
        self._transfers = transfers
        self.pool = pool
        if self.pool is not None:
            thread = isinstance(self.pool, ThreadPoolExecutor)
            self.queue: QueueType = ThreadQueue() if thread else ProcessQueue()
            self.pool_result = self.pool.submit(self._process_queue)
            self.clear_down = False
            self.processing_lock = threading.Lock() if thread else multiprocessing.Lock()

    @abstractmethod
    def combine_auditor_information(
        self,
        left: Union[AuditorType, AuditReturnType],
        right: Union[AuditorType, AuditReturnType],
    ) -> AuditReturnType:
        """Method to combine audit information of two auditors based on submission_id"""
        raise NotImplementedError()

    @staticmethod
    def conv_to_iterable(recs: Union[AuditorType, AuditReturnType]) -> Iterable[dict[str, Any]]:
        """Convert AuditReturnType to iterable of dictionaries"""
        raise NotImplementedError()

    @validate_arguments
    def add_processing_records(self, processing_records: list[ProcessingStatusRecord]):
        """Add an entry to the processing_status auditor."""
        if self.pool:
            return self._submit(
                audit_object=self._processing_status,
                records=[dict(rec) for rec in processing_records],
            )
        return self._processing_status.add_records(
            records=[dict(rec) for rec in processing_records]
        )

    @validate_arguments
    def add_submission_statistics_records(self, sub_stats: list[SubmissionStatisticsRecord]):
        """Add an entry to the submission statistics auditor."""
        if self.pool:
            return self._submit(
                audit_object=self._submission_statistics,
                records=[dict(rec) for rec in sub_stats],
            )
        return self._submission_statistics.add_records(records=[dict(rec) for rec in sub_stats])

    @validate_arguments
    def add_transfer_records(self, transfer_records: list[TransferRecord]):
        """Add an entry to the transfers auditor"""
        if self.pool:
            return self._submit(
                audit_object=self._transfers, records=[dict(rec) for rec in transfer_records]
            )
        return self._transfers.add_records(records=[dict(rec) for rec in transfer_records])

    @validate_arguments
    def add_new_submissions(
        self,
        submissions: list[SubmissionMetadata],
        job_run_id: Optional[int] = None,
    ):
        """Add an entry to the submission_info auditor."""
        # get timestamp of update and date of update to align
        # submission_info and processing_status tables
        time_now: datetime = datetime.now()
        ts_info = {"time_updated": time_now, "date_updated": time_now.date()}

        processing_status_recs: list[dict[str, Any]] = []
        submission_info_recs: list[dict[str, Any]] = []

        for sub_info in submissions:
            # add processing_record - add time info
            processing_rec = {
                **ProcessingStatusRecord(
                    submission_id=sub_info.submission_id,
                    processing_status="received",
                    job_run_id=job_run_id,
                    **ts_info,
                ).dict(),
            }
            processing_status_recs.append(processing_rec)
            if sub_info:
                sub_info_rec = {**dict(sub_info), **ts_info}
                submission_info_recs.append(sub_info_rec)

        if self.pool:
            self._submit(audit_object=self._processing_status, records=processing_status_recs)
            self._submit(audit_object=self._submission_info, records=submission_info_recs)
        else:
            self._processing_status.add_records(records=processing_status_recs)
            self._submission_info.add_records(records=submission_info_recs)
        return submissions

    def _submit(self, **kwargs):
        self.queue.put(kwargs)

    def _process_queue(self):
        while True:
            if self.queue.empty():
                time.sleep(2)
                continue
            item = self.queue.get()
            if item is None:
                break
            try:
                with self.processing_lock:
                    item.get("audit_object").add_records(item.get("records"))
            except Exception as exc:  # pylint: disable=broad-except
                print(exc)  # TODO - log this - rather than print

    def is_writing(self) -> bool:
        """Check if the audit manager is currently writing data to auditors"""
        if self.pool is None:
            return False
        if isinstance(self.processing_lock, type(threading.Lock())):
            locked = self.processing_lock.locked()
        else:
            # process locks don't have a locked method, if we try to aquire with a low timeout
            # if it succeeds then the lock is free. if it fails then it's not
            try:
                self.processing_lock.acquire(timeout=0.001)  # type: ignore
                locked = False
                self.processing_lock.release()  # type: ignore
            except TimeoutError:
                locked = True

        return not self.queue.empty() or locked

    def mark_transform(self, submission_ids: list[str], **kwargs):
        """Update submission processing_status to file_transformation."""

        recs = [
            ProcessingStatusRecord(
                submission_id=submission_id, processing_status="file_transformation", **kwargs
            )
            for submission_id in submission_ids
        ]

        return self.add_processing_records(recs)

    def mark_data_contract(self, submission_ids: list[str], **kwargs):
        """Update submission processing_status to data_contract."""

        recs = [
            ProcessingStatusRecord(
                submission_id=submission_id, processing_status="data_contract", **kwargs
            )
            for submission_id in submission_ids
        ]

        return self.add_processing_records(recs)

    def mark_business_rules(self, submissions: list[tuple[str, bool]], **kwargs):
        """Update submission processing_status to business_rules."""

        recs = [
            ProcessingStatusRecord(
                submission_id=submission_id,
                processing_status="business_rules",
                submission_result="failed" if failed else None,
                **kwargs,
            )
            for submission_id, failed in submissions
        ]

        return self.add_processing_records(recs)

    def mark_error_report(
        self,
        submissions: list[tuple[str, SubmissionResult]],
        job_run_id: Optional[int] = None,
    ):
        """Mark the given submission as being ready for error report"""
        processing_recs: list[ProcessingStatusRecord] = []

        sub_id: str
        sub_result: str

        for sub_id, sub_result in submissions:
            processing_recs.append(
                ProcessingStatusRecord(
                    submission_id=sub_id,
                    processing_status="error_report",
                    submission_result=sub_result,
                    job_run_id=job_run_id,
                )
            )

        return self.add_processing_records(processing_recs)

    def mark_finished(self, submissions: list[tuple[str, SubmissionResult]], **kwargs):
        """Update submission processing_status to finished."""

        recs = [
            ProcessingStatusRecord(
                submission_id=sub_id,
                processing_status="success",
                submission_result=sub_res,
                **kwargs,
            )
            for sub_id, sub_res in submissions
        ]

        return self.add_processing_records(recs)

    def mark_failed(self, submissions: list[str], **kwargs):
        """Update submission processing_status to failed."""
        recs = [
            ProcessingStatusRecord(
                submission_id=submission_id, processing_status="failed", **kwargs
            )
            for submission_id in submissions
        ]

        return self.add_processing_records(recs)

    def mark_archived(self, submissions: list[str], **kwargs):
        """Update submission processing_status to archived."""
        recs = [
            ProcessingStatusRecord(
                submission_id=submission_id, processing_status="archived", **kwargs
            )
            for submission_id in submissions
        ]

        return self.add_processing_records(recs)

    def add_feedback_transfer_ids(self, submissions: list[tuple[str, str]], **kwargs):
        """Adds transfer_id for error report to submission"""
        recs = [
            TransferRecord(
                submission_id=submission_id,
                report_name="error_report",
                transfer_id=transfer_id,
                **kwargs,
            )
            for submission_id, transfer_id in submissions
        ]

        return self.add_transfer_records(recs)

    def get_latest_processing_records(
        self, filter_criteria: Optional[list[FilterCriteria]] = None
    ) -> AuditReturnType:
        """Get the most recent processing record for each submission_id stored in
        the processing_status auditor"""
        return self._processing_status.get_most_recent_records(
            order_criteria=[OrderCriteria("time_updated", True)],
            partition_fields=["submission_id"],
            pre_filter_criteria=filter_criteria,
        )

    def downstream_pending(
        self,
        status: ProcessingStatus,
        max_concurrency: int = 1,
        run_number: int = 0,
        max_days_old: int = 3,
        statuses_to_include: Optional[list[ProcessingStatus]] = None,
    ) -> bool:
        """Checks if there are any downstream submissions currently pending"""
        steps: list[ProcessingStatus] = [
            "received",
            "file_transformation",
            "data_contract",
            "business_rules",
            "error_report",
        ]

        downstream: set[ProcessingStatus]
        if statuses_to_include:
            downstream = {status, *statuses_to_include}
        else:
            downstream = {*steps[: steps.index(status) + 1]}

        pending = self._processing_status.conv_to_records(
            self._processing_status.retrieve_records(
                filter_criteria=[
                    FilterCriteria(
                        "date_updated",
                        str(date.today() - timedelta(days=max_days_old)),
                        operator.gt,
                    ),
                    FilterCriteria("processing_status", downstream, operator.contains),
                ],
                data=self.get_latest_processing_records(),
            )
        )
        pending_for_job = filter(
            lambda sub_id: int(sub_id, 16) % max_concurrency == run_number,  # type: ignore
            [rw.submission_id for rw in pending],
        )
        try:
            next(pending_for_job)
            return True
        except StopIteration:
            return False

    def get_submission_info(self, submission_id: str) -> Optional[SubmissionInfo]:
        """Get all stored info for a submission"""
        try:
            return next(  # type: ignore
                self._submission_info.conv_to_records(
                    self._submission_info.retrieve_records(
                        filter_criteria=[FilterCriteria("submission_id", submission_id)]
                    )
                )
            )
        except StopIteration:
            return None

    def get_submission_statistics(self, submission_id: str) -> Optional[SubmissionStatisticsRecord]:
        """Get submission statistics record for submission if one exists"""
        try:
            return next(  # type: ignore
                self._submission_statistics.conv_to_records(
                    self._submission_statistics.retrieve_records(
                        filter_criteria=[FilterCriteria("submission_id", submission_id)]
                    )
                )
            )
        except StopIteration:
            return None

    def __enter__(self):
        """Use audit table as context manager"""
        if self.pool and self.pool_result.done():
            thread = isinstance(self.pool, ThreadPoolExecutor)
            self.queue: QueueType = ThreadQueue() if thread else ProcessQueue()
            self.pool_result = self.pool.submit(self._process_queue)
            self.clear_down = False
            self.processing_lock = threading.Lock() if thread else multiprocessing.Lock()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[Exception]],
        exc_value: Optional[Exception],
        traceback: Optional[TracebackType],
    ) -> None:
        """Use audit table as context manager"""
        if self.pool:
            self.queue.put(None)
            print(self.pool_result.result())
            while not self.queue.empty():
                time.sleep(1)

    def _get_status(
        self,
        status: Union[ProcessingStatus, set[ProcessingStatus], list[ProcessingStatus]],
        max_days_old: int,
    ) -> AuditReturnType:
        _filter = [
            FilterCriteria(
                "date_updated", str(date.today() - timedelta(days=max_days_old)), operator.gt
            )
        ]
        if isinstance(status, (set, list)):
            _filter.append(
                FilterCriteria(
                    "processing_status",
                    status,
                    operator.contains,
                )
            )
        else:
            _filter.append(FilterCriteria("processing_status", status))
        return self._processing_status.retrieve_records(filter_criteria=_filter)

    def get_all_file_transformation_submissions(self, max_days_old: int = 3) -> AuditReturnType:
        """Gets all of the submissions that are ready to be parsed"""
        return self._get_status("file_transformation", max_days_old)

    def get_all_data_contract_submissions(self, max_days_old: int = 3) -> AuditReturnType:
        """Gets all of the submissions that are ready for data contract to be applied"""
        return self._get_status("data_contract", max_days_old)

    def get_all_business_rule_submissions(self, max_days_old: int = 3) -> AuditReturnType:
        """Gets all of the submissions that are ready for business rules to be applied"""
        return self._get_status("business_rules", max_days_old)

    def get_all_error_report_submissions(self, max_days_old: int = 3):
        """Gets all the submissions that are ready for error reports to be generated"""
        subs = self._get_status("error_report", max_days_old)

        sub_infos = self.conv_to_iterable(
            self.combine_auditor_information(subs, self._submission_info)
        )

        processed: list[SubmissionInfo] = []
        dodgy_info: list[tuple[dict, str]] = []

        for sub_info in sub_infos:
            try:
                processed.append(SubmissionInfo(**sub_info))
            except ValidationError:
                dodgy_info.append((sub_info, sub_info["submission_id"]))

        return processed, dodgy_info

    def get_current_processing_info(self, submission_id: str) -> Optional[ProcessingStatusRecord]:
        """Gets the current status of the record with the given submission_id"""
        try:
            return next(  # type: ignore
                iter(
                    self._processing_status.conv_to_records(
                        self._processing_status.get_most_recent_records(
                            pre_filter_criteria=[FilterCriteria("submission_id", submission_id)],
                            order_criteria=[OrderCriteria("time_updated", True)],
                        )
                    )
                )
            )
        except StopIteration:
            return None
