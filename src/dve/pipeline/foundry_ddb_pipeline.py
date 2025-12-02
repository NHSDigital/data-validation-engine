"""A duckdb pipeline for running on Foundry platform"""
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import duckdb_write_parquet
from dve.core_engine.models import SubmissionInfo
from dve.pipeline.duckdb_pipeline import DDBDVEPipeline
from dve.pipeline.utils import SubmissionStatus
from dve.parser import file_handling as fh

@duckdb_write_parquet
class FoundryDDBPipeline(DDBDVEPipeline):
    """DuckDB pipeline for running on Foundry Platform"""
    def persist_audit_records(self, submission_info: SubmissionInfo):
        """Write out key audit relations to parquet for persisting to datasets"""
        write_to = fh.joinuri(self.processed_files_path, submission_info.submission_id, "audit/")
        self.write_parquet(
            self._audit_tables._processing_status.get_relation(),
            write_to + "processing_status.parquet")
        self.write_parquet(
            self._audit_tables._submission_statistics.get_relation(),
            write_to + "submission_statistics.parquet")
    
    def run_pipeline(self, submission_info: SubmissionInfo):
        """Sequential single submission pipeline runner"""
        try:
            sub_id: str = submission_info.submission_id
            self._audit_tables.add_new_submissions(submissions=[submission_info])
            self._audit_tables.mark_transform(submission_ids=[sub_id])
            sub_info = self.file_transformation(submission_info=submission_info)
            if isinstance(sub_info, SubmissionInfo):
                self._audit_tables.mark_data_contract(submission_ids=[sub_id])
                sub_info, failed = self.apply_data_contract(submission_info=submission_info)
                self._audit_tables.mark_business_rules(submissions=[(sub_info, failed)])
                sub_info, sub_status = self.apply_business_rules(submission_info=submission_info, failed= failed)
            else:
                sub_status = SubmissionStatus(failed=True)    
            self._audit_tables.mark_error_report(submissions=[(sub_id, sub_status.submission_result)])
            sub_info, sub_status, sub_stats = self.error_report(submission_info=submission_info)
            self._audit_tables.add_submission_statistics_records(subs_stats=[sub_stats])
        except Exception as err:
            self._logger.error(f"During processing of submission_id: {sub_id}, the following exception was raised: {err}")
            self._audit_tables.mark_failed(submissions=[sub_id])
        finally:
            self.persist_audit_records(submission_info=submission_info)
            