"""A duckdb pipeline for running on Foundry platform"""
from typing import List, Optional, Tuple
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import duckdb_write_parquet
from dve.core_engine.backends.utilities import dump_errors
from dve.core_engine.models import SubmissionInfo
from dve.core_engine.type_hints import URI, Failed
from dve.pipeline.duckdb_pipeline import DDBDVEPipeline
from dve.pipeline.utils import SubmissionStatus
from dve.parser import file_handling as fh

@duckdb_write_parquet
class FoundryDDBPipeline(DDBDVEPipeline):
    """DuckDB pipeline for running on Foundry Platform"""
    def persist_audit_records(self, submission_info: SubmissionInfo) -> URI:
        """Write out key audit relations to parquet for persisting to datasets"""
        write_to = fh.joinuri(self.processed_files_path, submission_info.submission_id, "audit/")
        self.write_parquet(
            self._audit_tables._processing_status.get_relation(),
            write_to + "processing_status.parquet")
        self.write_parquet(
            self._audit_tables._submission_statistics.get_relation(),
            write_to + "submission_statistics.parquet")
        return write_to
    
    def file_transformation(self, submission_info: SubmissionInfo) -> SubmissionInfo | dict[str, str]:
        try:
            return super().file_transformation(submission_info)
        except Exception as exc:
            self._logger.error(f"File transformation raised exception: {exc}")
            self._logger.exception(exc)
            # TODO: write errors to file here (maybe processing errors - not to be seen by end user)
            return submission_info.dict()
    
    def apply_data_contract(self, submission_info: SubmissionInfo) -> Tuple[SubmissionInfo | bool]:
        try:
            return super().apply_data_contract(submission_info)
        except Exception as exc:
            self._logger.error(f"Apply data contract raised exception: {exc}")
            self._logger.exception(exc)
            # TODO: write errors to file here (maybe processing errors - not to be seen by end user)
            return submission_info, True
    
    def apply_business_rules(self, submission_info: SubmissionInfo, failed: Failed):
        try:
            return super().apply_business_rules(submission_info, failed)
        except Exception as exc:
            self._logger.error(f"Apply business rules raised exception: {exc}")
            self._logger.exception(exc)
            # TODO: write errors to file here (maybe processing errors - not to be seen by end user)
            return submission_info, SubmissionStatus(failed=True)
            
       
    def run_pipeline(self, submission_info: SubmissionInfo) -> Tuple[Optional[URI], URI, URI]:
        """Sequential single submission pipeline runner"""
        try:
            sub_id: str = submission_info.submission_id
            self._audit_tables.add_new_submissions(submissions=[submission_info])
            self._audit_tables.mark_transform(submission_ids=[sub_id])
            sub_info = self.file_transformation(submission_info=submission_info)
            if isinstance(sub_info, SubmissionInfo):
                self._audit_tables.mark_data_contract(submission_ids=[sub_id])
                sub_info, failed = self.apply_data_contract(submission_info=submission_info)
                self._audit_tables.mark_business_rules(submissions=[(sub_id, failed)])
                sub_info, sub_status = self.apply_business_rules(submission_info=submission_info, failed= failed)
            else:
                sub_status = SubmissionStatus(failed=True)    
            self._audit_tables.mark_error_report(submissions=[(sub_id, sub_status.submission_result)])
            sub_info, sub_status, sub_stats, report_uri = self.error_report(submission_info=submission_info, status=sub_status)
            self._audit_tables.add_submission_statistics_records(sub_stats=[sub_stats])
        except Exception as err:
            self._logger.error(f"During processing of submission_id: {sub_id}, the following exception was raised: {err}")
            self._audit_tables.mark_failed(submissions=[sub_id])
        finally:
            audit_files_uri = self.persist_audit_records(submission_info=submission_info)
            return  (
                None if sub_status.failed else fh.joinuri(
                    self.processed_files_path,
                    sub_id,
                    "business_rules"),
                report_uri,
                audit_files_uri
            )
            