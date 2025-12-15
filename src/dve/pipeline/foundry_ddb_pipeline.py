"""A duckdb pipeline for running on Foundry platform"""

from typing import Optional
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import duckdb_get_entity_count, duckdb_write_parquet
from dve.core_engine.exceptions import CriticalProcessingError
from dve.core_engine.models import SubmissionInfo
from dve.core_engine.type_hints import URI, Failed
from dve.parser.file_handling.implementations.file import LocalFilesystemImplementation
from dve.parser.file_handling.service import _get_implementation
from dve.pipeline.duckdb_pipeline import DDBDVEPipeline
from dve.pipeline.utils import SubmissionStatus
from dve.parser import file_handling as fh
from dve.reporting.utils import dump_processing_errors

@duckdb_get_entity_count
@duckdb_write_parquet
class FoundryDDBPipeline(DDBDVEPipeline):
    """DuckDB pipeline for running on Foundry Platform"""

    def persist_audit_records(self, submission_info: SubmissionInfo) -> URI:
        """Write out key audit relations to parquet for persisting to datasets"""
        write_to = fh.joinuri(self.processed_files_path, submission_info.submission_id, "audit/")
        if isinstance(_get_implementation(write_to), LocalFilesystemImplementation):
            write_to = fh.file_uri_to_local_path(write_to)
            write_to.parent.mkdir(parents=True, exist_ok=True)
            write_to = write_to.as_posix()
        self.write_parquet(
            self._audit_tables._processing_status.get_relation(),
            fh.joinuri(write_to, "processing_status.parquet"),
        )
        self.write_parquet(
            self._audit_tables._submission_statistics.get_relation(),
            fh.joinuri(write_to, "submission_statistics.parquet"),
        )
        return write_to

    def file_transformation(
        self, submission_info: SubmissionInfo
    ) -> tuple[SubmissionInfo, SubmissionStatus]:
        try:
            return super().file_transformation(submission_info)
        except Exception as exc: # pylint: disable=W0718
            self._logger.error(f"File transformation raised exception: {exc}")
            self._logger.exception(exc)
            dump_processing_errors(
                    fh.joinuri(self.processed_files_path, submission_info.submission_id),
                    "file_transformation",
                    [CriticalProcessingError.from_exception(exc)]
                )
            self._audit_tables.mark_failed(submissions=[submission_info.submission_id])
            return submission_info, SubmissionStatus(processing_failed=True)

    def apply_data_contract(self, submission_info: SubmissionInfo, submission_status: SubmissionStatus) -> tuple[SubmissionInfo | SubmissionStatus]:
        try:
            return super().apply_data_contract(submission_info, submission_status)
        except Exception as exc: # pylint: disable=W0718
            self._logger.error(f"Apply data contract raised exception: {exc}")
            self._logger.exception(exc)
            dump_processing_errors(
                    fh.joinuri(self.processed_files_path, submission_info.submission_id),
                    "contract",
                    [CriticalProcessingError.from_exception(exc)]
                )
            self._audit_tables.mark_failed(submissions=[submission_info.submission_id])
            return submission_info, SubmissionStatus(processing_failed=True)

    def apply_business_rules(self, submission_info: SubmissionInfo, submission_status: SubmissionStatus):
        try:
            return super().apply_business_rules(submission_info, submission_status)
        except Exception as exc: # pylint: disable=W0718
            self._logger.error(f"Apply business rules raised exception: {exc}")
            self._logger.exception(exc)
            dump_processing_errors(
                    fh.joinuri(self.processed_files_path, submission_info.submission_id),
                    "business_rules",
                    [CriticalProcessingError.from_exception(exc)]
                )
            self._audit_tables.mark_failed(submissions=[submission_info.submission_id])
            return submission_info, SubmissionStatus(processing_failed=True)
    
    def error_report(self, submission_info: SubmissionInfo, submission_status: SubmissionStatus):
        try:
            return super().error_report(submission_info, submission_status)
        except Exception as exc: # pylint: disable=W0718
            self._logger.error(f"Error reports raised exception: {exc}")
            self._logger.exception(exc)
            sub_stats = None
            report_uri = None
            dump_processing_errors(
                    fh.joinuri(self.processed_files_path, submission_info.submission_id),
                    "error_report",
                    [CriticalProcessingError.from_exception(exc)]
                )
            self._audit_tables.mark_failed(submissions=[submission_info.submission_id])
            return submission_info, submission_status, sub_stats, report_uri

    def run_pipeline(self, submission_info: SubmissionInfo) -> tuple[Optional[URI], Optional[URI], URI]:
        """Sequential single submission pipeline runner"""
        try:
            sub_id: str = submission_info.submission_id
            report_uri = None
            self._audit_tables.add_new_submissions(submissions=[submission_info])
            self._audit_tables.mark_transform(submission_ids=[sub_id])
            sub_info, sub_status = self.file_transformation(submission_info=submission_info)
            if not (sub_status.validation_failed or sub_status.processing_failed):
                self._audit_tables.mark_data_contract(submission_ids=[sub_id])
                sub_info, sub_status = self.apply_data_contract(submission_info=sub_info, submission_status=sub_status)
                self._audit_tables.mark_business_rules(submissions=[(sub_id, sub_status.validation_failed)])
                sub_info, sub_status = self.apply_business_rules(
                    submission_info=submission_info, submission_status=sub_status
                )

            if not sub_status.processing_failed:
                self._audit_tables.mark_error_report(
                    submissions=[(sub_id, sub_status.submission_result)]
                )
                sub_info, sub_status, sub_stats, report_uri = self.error_report(
                    submission_info=submission_info, submission_status=sub_status
                )
                self._audit_tables.add_submission_statistics_records(sub_stats=[sub_stats])
        except Exception as err: # pylint: disable=W0718
            self._logger.error(
                f"During processing of submission_id: {sub_id}, the following exception was raised: {err}"
            )
            dump_processing_errors(
                    fh.joinuri(self.processed_files_path, submission_info.submission_id),
                    "run_pipeline",
                    [CriticalProcessingError.from_exception(err)]
                )
            self._audit_tables.mark_failed(submissions=[sub_id])
        finally:
            audit_files_uri = self.persist_audit_records(submission_info=submission_info)
        return (
            (
                None
                if (sub_status.validation_failed or sub_status.processing_failed)
                else fh.joinuri(self.processed_files_path, sub_id, "business_rules")
            ),
            report_uri if report_uri else None,
            audit_files_uri,
        )
