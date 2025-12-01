# pylint: disable=protected-access,too-many-instance-attributes,too-many-arguments,line-too-long
"""Generic Pipeline object to define how DVE should be interacted with."""
import json
import re
from collections import defaultdict
from collections.abc import Generator, Iterable, Iterator
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from functools import lru_cache
from threading import Lock
from typing import Optional, Union
from uuid import uuid4

import polars as pl
from pydantic import validate_arguments

import dve.reporting.excel_report as er
from dve.core_engine.backends.base.auditing import BaseAuditingManager
from dve.core_engine.backends.base.contract import BaseDataContract
from dve.core_engine.backends.base.core import EntityManager
from dve.core_engine.backends.base.reference_data import BaseRefDataLoader
from dve.core_engine.backends.base.rules import BaseStepImplementations
from dve.core_engine.backends.exceptions import MessageBearingError
from dve.core_engine.backends.readers import BaseFileReader
from dve.core_engine.backends.types import EntityType
from dve.core_engine.backends.utilities import dump_errors, stringify_model
from dve.core_engine.loggers import get_logger
from dve.core_engine.models import SubmissionInfo, SubmissionStatisticsRecord
from dve.core_engine.type_hints import URI, Failed, FileURI, InfoURI
from dve.parser import file_handling as fh
from dve.pipeline.utils import SubmissionStatus, deadletter_file, load_config, load_reader
from dve.reporting.error_report import ERROR_SCHEMA, calculate_aggregates

PERMISSIBLE_EXCEPTIONS: tuple[type[Exception]] = (
    FileNotFoundError,  # type: ignore
    FileNotFoundError,
)


class BaseDVEPipeline:
    """
    Base class for running a DVE Pipeline either by a given step or a full e2e process.
    """

    def __init__(
        self,
        audit_tables: BaseAuditingManager,
        job_run_id: int,
        data_contract: BaseDataContract,
        step_implementations: Optional[BaseStepImplementations[EntityType]],
        rules_path: Optional[URI],
        processed_files_path: Optional[URI],
        submitted_files_path: Optional[URI],
        reference_data_loader: Optional[type[BaseRefDataLoader]] = None,
    ):
        self._submitted_files_path = submitted_files_path
        self._processed_files_path = processed_files_path
        self._rules_path = rules_path
        self._reference_data_loader = reference_data_loader
        self._job_run_id = job_run_id
        self._audit_tables = audit_tables
        self._data_contract = data_contract
        self._step_implementations = step_implementations
        self._logger = get_logger(__name__)
        self._summary_lock = Lock()
        self._rec_tracking_lock = Lock()
        self._aggregates_lock = Lock()

    @property
    def job_run_id(self) -> int:
        """Unique Identifier for the job/process that is running this Pipeline."""
        return self._job_run_id

    @property
    def processed_files_path(self) -> Optional[URI]:
        """URI Location for where the files are being processed."""
        return self._processed_files_path

    @property
    def rules_path(self) -> Optional[URI]:
        """URI Location for rules of the dataset (i.e. the dischema.json)."""
        return self._rules_path

    @property
    def data_contract(self) -> BaseDataContract:
        """Data contract object to load and apply the rules to a given dataset."""
        return self._data_contract

    @property
    def step_implementations(self) -> Optional[BaseStepImplementations[EntityType]]:
        """The step implementations to apply the business rules to a given dataset"""
        return self._step_implementations

    @staticmethod
    def get_entity_count(entity: EntityType) -> int:
        """Get a row count of an entity stored as parquet"""
        raise NotImplementedError()

    @validate_arguments
    def _move_submission_to_working_location(
        self,
        submission_id: str,
        submitted_file_uri: URI,
        submission_info_uri: URI,
    ) -> tuple[URI, URI]:
        if not self.processed_files_path:
            raise AttributeError("Path for processed files not supplied.")

        paths: list[URI] = []
        for path in (submitted_file_uri, submission_info_uri):
            source = fh.resolve_location(path)
            dest = fh.joinuri(self.processed_files_path, submission_id, fh.get_file_name(path))
            fh.move_resource(source, dest)
            paths.append(dest)

        return tuple(paths)  # type: ignore

    def _get_submission_files_for_run(self) -> Generator[tuple[FileURI, InfoURI], None, None]:
        """Yields submission files from the submitted_files path"""
        # TODO - I think the metadata generation needs to be redesigned or at least generated
        # TODO - if we continue with this approach. This comments is based on the fact that
        # TODO - we still rely on a metadata.json atm.
        if not self._submitted_files_path:
            raise AttributeError("Path for submitted files not supplied.")

        files = defaultdict(list)

        for _file, _ in fh.iter_prefix(self._submitted_files_path):
            stem = fh.get_file_stem(_file)
            stem = re.sub(".metadata$", "", stem)
            files[stem].append(_file)

        for res in files.values():
            if len(res) < 2:
                # not a pair of meta and submission to process (yet!)
                continue
            if len(res) > 2:
                # somehow got multiple files attached to same meta (csv and xml sub together)
                for fle in res:
                    deadletter_file(fle)
                continue

            if "metadata" in res[0]:
                metadata_uri = res[0]
                sub_uri = res[1]
            else:
                metadata_uri = res[1]
                sub_uri = res[0]

            yield sub_uri, metadata_uri

    def write_file_to_parquet(
        self,
        submission_file_uri: URI,
        submission_info: SubmissionInfo,
        output: URI,
        entity_type: Optional[EntityType] = None,
    ):
        """Takes a submission file and a valid submission_info and converts the file to parquet"""

        if not self.rules_path:
            raise AttributeError("rules path not provided")

        ext = submission_info.file_extension
        models, _config, dataset = load_config(submission_info.dataset_id, self.rules_path)
        out = fh.joinuri(output, submission_info.submission_id, "transform/")
        fh.create_directory(out)  # simply for local file systems
        errors = []

        for model_name, model in models.items():
            reader: BaseFileReader = load_reader(dataset, model_name, ext)
            try:
                if not entity_type:
                    reader.write_parquet(
                        reader.read_to_py_iterator(
                            submission_file_uri, model_name, stringify_model(model)  # type: ignore
                        ),
                        f"{out}{model_name}",
                    )
                else:
                    reader.write_parquet(
                        reader.read_to_entity_type(
                            entity_type,  # type: ignore
                            submission_file_uri,
                            model_name,
                            stringify_model(model),  # type: ignore
                        ),
                        f"{out}{model_name}",
                    )
            except MessageBearingError as exc:
                errors.extend(exc.messages)

        return list(dict.fromkeys(errors))  # remove any duplicate errors

    def audit_received_file(
        self, submission_id: str, submission_file: FileURI, metadata_file: InfoURI
    ):
        """Sets a file as received the moves it to working location"""
        # move file
        _, meta_uri = self._move_submission_to_working_location(  # pylint: disable=W0632
            submission_id, submission_file, metadata_file
        )
        sub_info = SubmissionInfo.from_metadata_file(submission_id, meta_uri)

        return sub_info

    def audit_received_file_step(
        self, pool: ThreadPoolExecutor, submitted_files: Iterable[tuple[FileURI, InfoURI]]
    ) -> tuple[list[SubmissionInfo], list[SubmissionInfo]]:
        """Set files as being received and mark them for file transformation"""
        audit_received_futures: list[tuple[str, FileURI, Future]] = []
        for submission_file in submitted_files:
            data_uri, metadata_uri = submission_file
            submission_id = uuid4().hex
            future = pool.submit(self.audit_received_file, submission_id, data_uri, metadata_uri)
            audit_received_futures.append((submission_id, data_uri, future))

        success: list[SubmissionInfo] = []
        failed: list[SubmissionInfo] = []
        for submission_id, submission_file_uri, future in audit_received_futures:
            try:
                submission_info = future.result()
            except AssertionError as exc:
                self._logger.error(f"audit_received_file raised exception: {exc}")
                raise exc
            except PERMISSIBLE_EXCEPTIONS as exc:
                self._logger.warning(
                    f"audit_received_file raised exception: {exc}. Will be retried later."
                )
                continue
            except Exception as exc:  # pylint: disable=W0703
                self._logger.error(f"audit_received_file raised exception: {exc}")
                self._logger.exception(exc)
                # sub_info should at least
                # be populated with file_name and file_extension
                failed.append(
                    SubmissionInfo(
                        submission_id=submission_id,
                        dataset_id=None,
                        file_name=fh.get_file_stem(submission_file_uri),
                        file_extension=fh.get_file_suffix(submission_file_uri),
                    )
                )
                continue
            if isinstance(submission_info, SubmissionInfo):
                success.append(submission_info)
            else:
                failed.append(submission_info)
        if len(success + failed) > 0:
            self._audit_tables.add_new_submissions(success + failed, job_run_id=self.job_run_id)
        if len(success) > 0:
            self._audit_tables.mark_transform(
                list(map(lambda x: x.submission_id, success)), job_run_id=self.job_run_id
            )
        if len(failed) > 0:
            self._audit_tables.mark_failed(
                list(map(lambda x: x.submission_id, failed)), job_run_id=self.job_run_id
            )

        return success, failed

    def file_transformation(
        self, submission_info: SubmissionInfo
    ) -> Union[SubmissionInfo, dict[str, str]]:
        """Transform a file from its original format into a 'stringified' parquet file"""
        if not self.processed_files_path:
            raise AttributeError("processed files path not provided")

        submission_file_uri: URI = fh.joinuri(
            self.processed_files_path,
            submission_info.submission_id,
            submission_info.file_name_with_ext,
        )
        try:
            errors = self.write_file_to_parquet(
                submission_file_uri, submission_info, self.processed_files_path
            )
            if errors:
                dump_errors(
                    fh.joinuri(self.processed_files_path, submission_info.submission_id),
                    "file_transformation",
                    errors,
                )
                return submission_info.dict()
            return submission_info
        except ValueError as exc:
            self._logger.error(f"File transformation write_file_to_parquet raised error: {exc}")
            return submission_info.dict()
        except Exception as exc:  # pylint: disable=broad-except
            self._logger.error(f"Unexpected file transformation error: {exc}")
            self._logger.exception(exc)
            return submission_info.dict()

    def file_transformation_step(
        self, pool: Executor, submissions_to_process: list[SubmissionInfo]
    ) -> tuple[list[SubmissionInfo], list[SubmissionInfo]]:
        """Step to transform files from their original format into parquet files"""
        file_transform_futures: list[tuple[SubmissionInfo, Future]] = []

        for submission_info in submissions_to_process:
            # add audit entry
            future = pool.submit(self.file_transformation, submission_info)
            file_transform_futures.append((submission_info, future))

        success: list[SubmissionInfo] = []
        failed: list[SubmissionInfo] = []
        failed_processing: list[SubmissionInfo] = []

        for sub_info, future in file_transform_futures:
            try:
                # sub_info passed here either return SubInfo or dict. If SubInfo, not actually
                # modified in anyway during this step.
                result = future.result()
            except AttributeError as exc:
                self._logger.error(f"File transformation raised exception: {exc}")
                raise exc
            except PERMISSIBLE_EXCEPTIONS as exc:
                self._logger.warning(
                    f"File transformation raised exception: {exc}. Will be retried later."
                )
                continue
            except Exception as exc:  # pylint: disable=W0703
                self._logger.error(f"File transformation raised exception: {exc}")
                self._logger.exception(exc)
                failed_processing.append(sub_info)
                continue

            if isinstance(result, SubmissionInfo):
                success.append(sub_info)
            else:
                failed.append(sub_info)

        if len(success) > 0:
            self._audit_tables.mark_data_contract(
                list(map(lambda x: x.submission_id, success)), job_run_id=self.job_run_id
            )

        if len(failed) > 0:
            self._audit_tables.mark_error_report(
                list(
                    map(
                        lambda x: (
                            x.submission_id,
                            "failed",
                        ),
                        failed,
                    )
                ),
                job_run_id=self.job_run_id,
            )

        if len(failed_processing) > 0:
            self._audit_tables.mark_failed(
                [si.submission_id for si in failed_processing], job_run_id=self.job_run_id
            )

        return success, failed

    def apply_data_contract(self, submission_info: SubmissionInfo) -> tuple[SubmissionInfo, Failed]:
        """Method for applying the data contract given a submission_info"""

        if not self.processed_files_path:
            raise AttributeError("processed files path not provided")

        if not self.rules_path:
            raise AttributeError("rules path not provided")

        read_from = fh.joinuri(
            self.processed_files_path, submission_info.submission_id, "transform/"
        )
        write_to = fh.joinuri(self.processed_files_path, submission_info.submission_id, "contract/")

        _, config, model_config = load_config(submission_info.dataset_id, self.rules_path)
        entities = {}

        for path, _ in fh.iter_prefix(read_from):
            entities[fh.get_file_name(path)] = self.data_contract.read_parquet(path)

        entities, messages, _success = self.data_contract.apply_data_contract(  # type: ignore
            entities, config.get_contract_metadata()
        )

        entitity: self.data_contract.__entity_type__  # type: ignore
        for entity_name, entitity in entities.items():
            self.data_contract.write_parquet(entitity, fh.joinuri(write_to, entity_name))

        key_fields = {model: conf.reporting_fields for model, conf in model_config.items()}
        if messages:
            dump_errors(
                fh.joinuri(self.processed_files_path, submission_info.submission_id),
                "contract",
                messages,
                key_fields=key_fields,
            )

        failed = any(not rule_message.is_informational for rule_message in messages)

        return submission_info, failed

    def data_contract_step(
        self, pool: Executor, file_transform_results: list[SubmissionInfo]
    ) -> tuple[list[tuple[SubmissionInfo, Failed]], list[SubmissionInfo]]:
        """Step to validate the types of an untyped (stringly typed) parquet file"""
        processed_files: list[tuple[SubmissionInfo, Failed]] = []
        failed_processing: list[SubmissionInfo] = []
        dc_futures: list[tuple[SubmissionInfo, Future]] = []

        for info in file_transform_results:
            dc_futures.append((info, pool.submit(self.apply_data_contract, info)))

        for sub_info, future in dc_futures:
            try:
                submission_info: SubmissionInfo
                failed: Failed
                submission_info, failed = future.result()
            except AttributeError as exc:
                self._logger.error(f"Data Contract raised exception: {exc}")
                raise exc
            except PERMISSIBLE_EXCEPTIONS as exc:
                self._logger.warning(
                    f"Data Contract raised exception: {exc}. Will be retried later."
                )
                continue
            except Exception as exc:  # pylint: disable=W0703
                self._logger.error(f"Data Contract raised exception: {exc}")
                self._logger.exception(exc)
                failed_processing.append(sub_info)
                continue

            processed_files.append((submission_info, failed))

        if len(processed_files) > 0:
            self._audit_tables.mark_business_rules(
                [
                    (sub_info.submission_id, failed)  # type: ignore
                    for sub_info, failed in processed_files
                ],
                job_run_id=self.job_run_id,
            )

        if len(failed_processing) > 0:
            self._audit_tables.mark_failed(
                [sub_info.submission_id for sub_info in failed_processing],
                job_run_id=self.job_run_id,
            )

        return processed_files, failed_processing

    def apply_business_rules(self, submission_info: SubmissionInfo, failed: bool):
        """Apply the business rules to a given submission, the submission may have failed at the
        data_contract step so this should be passed in as a bool
        """
        if not self.rules_path:
            raise AttributeError("business rules path not provided.")

        if not self._reference_data_loader:
            raise AttributeError("reference data loader not provided.")

        if not self.processed_files_path:
            raise AttributeError("processed files path has not been provided.")

        if not self._step_implementations:
            raise AttributeError("step implementations has not been provided.")

        _, config, model_config = load_config(submission_info.dataset_id, self.rules_path)
        ref_data = config.get_reference_data_config()
        rules = config.get_rule_metadata()
        reference_data = self._reference_data_loader(ref_data)  # type: ignore
        entities = {}
        contract = fh.joinuri(self.processed_files_path, submission_info.submission_id, "contract")

        for parquet_uri, _ in fh.iter_prefix(contract):
            file_name = fh.get_file_name(parquet_uri)
            entities[file_name] = self.step_implementations.read_parquet(parquet_uri)  # type: ignore
            entities[file_name] = self.step_implementations.add_row_id(entities[file_name])  # type: ignore
            entities[f"Original{file_name}"] = self.step_implementations.read_parquet(parquet_uri)  # type: ignore

        sub_info_entity = (
            self._audit_tables._submission_info.conv_to_entity(  # pylint: disable=protected-access
                [submission_info]
            )
        )
        reference_data.entity_cache["dve_submission_info"] = sub_info_entity

        entity_manager = EntityManager(entities=entities, reference_data=reference_data)

        rule_messages = self.step_implementations.apply_rules(entity_manager, rules)  # type: ignore
        key_fields = {model: conf.reporting_fields for model, conf in model_config.items()}

        if rule_messages:
            dump_errors(
                fh.joinuri(self.processed_files_path, submission_info.submission_id),
                "business_rules",
                rule_messages,
                key_fields,
            )

        failed = any(not rule_message.is_informational for rule_message in rule_messages) or failed

        for entity_name, entity in entity_manager.entities.items():
            projected = self._step_implementations.write_parquet(  # type: ignore
                entity,
                fh.joinuri(
                    self.processed_files_path,
                    submission_info.submission_id,
                    "business_rules",
                    entity_name,
                ),
            )
            entity_manager.entities[entity_name] = self.step_implementations.read_parquet(  # type: ignore
                projected
            )

        status = SubmissionStatus(
            failed=failed,
            number_of_records=self.get_entity_count(
                entity=entity_manager.entities[
                    f"""Original{rules.global_variables.get(
                                              'entity',
                                              submission_info.dataset_id)}"""
                ]
            ),
        )

        return submission_info, status

    def business_rule_step(
        self,
        pool: Executor,
        files: list[tuple[SubmissionInfo, Failed]],
    ) -> tuple[
        list[tuple[SubmissionInfo, SubmissionStatus]],
        list[tuple[SubmissionInfo, SubmissionStatus]],
        list[SubmissionInfo],
    ]:
        """Step to apply business rules (Step impl) to a typed parquet file"""
        future_files: list[tuple[SubmissionInfo, Future]] = []

        for submission_info, submission_failed in files:
            future_files.append(
                (
                    submission_info,
                    pool.submit(self.apply_business_rules, submission_info, submission_failed),
                )
            )

        failed_processing: list[SubmissionInfo] = []
        unsucessful_files: list[tuple[SubmissionInfo, SubmissionStatus]] = []
        successful_files: list[tuple[SubmissionInfo, SubmissionStatus]] = []

        for sub_info, future in future_files:
            status: SubmissionStatus
            try:
                submission_info, status = future.result()
            except AttributeError as exc:
                self._logger.error(f"Business Rules raised exception: {exc}")
                raise exc
            except PERMISSIBLE_EXCEPTIONS as exc:
                self._logger.warning(
                    f"Business Rules raised exception: {exc}. Will be retried later."
                )
                continue
            except Exception as exc:  # pylint: disable=W0703
                self._logger.error(f"Business Rules raised exception: {exc}")
                self._logger.exception(exc)
                failed_processing.append(sub_info)
                continue

            if status.failed:
                unsucessful_files.append((submission_info, status))
            else:
                successful_files.append((submission_info, status))

        if len(unsucessful_files + successful_files) > 0:
            self._audit_tables.mark_error_report(
                [
                    (sub_info.submission_id, status.submission_result)
                    for sub_info, status in successful_files + unsucessful_files
                ],
                job_run_id=self.job_run_id,
            )

        if len(failed_processing) > 0:
            self._audit_tables.mark_failed(
                [si.submission_id for si in failed_processing], job_run_id=self.job_run_id
            )

        return successful_files, unsucessful_files, failed_processing

    @lru_cache()  # noqa: B019
    def _get_error_dataframes(self, submission_id: str):
        if not self.processed_files_path:
            raise AttributeError("processed files path not provided")

        path = fh.joinuri(self.processed_files_path, submission_id, "errors")
        errors_dfs = [pl.DataFrame([], schema=ERROR_SCHEMA)]  # type: ignore

        for file, _ in fh.iter_prefix(path):
            if fh.get_file_suffix(file) != "json":
                continue
            with fh.open_stream(file) as f:
                errors = None
                try:
                    errors = json.load(f)
                except UnicodeDecodeError as exc:
                    self._logger.error(f"Error reading file: {file}")
                    self._logger.exception(exc)
                    continue
                if not errors:
                    continue

                df = pl.DataFrame(errors, schema={key: pl.Utf8() for key in errors[0]})  # type: ignore
                df = df.with_columns(
                    pl.when(pl.col("Status") == pl.lit("error"))  # type: ignore
                    .then(pl.lit("Submission Failure"))  # type: ignore
                    .otherwise(pl.lit("Warning"))  # type: ignore
                    .alias("error_type")
                )
                df = df.select(
                    pl.col("Entity").alias("Table"),  # type: ignore
                    pl.col("error_type").alias("Type"),  # type: ignore
                    pl.col("ErrorCode").alias("Error_Code"),  # type: ignore
                    pl.col("ReportingField").alias("Data_Item"),  # type: ignore
                    pl.col("ErrorMessage").alias("Error"),  # type: ignore
                    pl.col("Value"),  # type: ignore
                    pl.col("Key").alias("ID"),  # type: ignore
                    pl.col("Category"),  # type: ignore
                )
                df = df.select(
                    pl.col(column).cast(ERROR_SCHEMA[column])  # type: ignore
                    for column in df.columns
                )
                df = df.sort("Type", descending=False)  # type: ignore
                errors_dfs.append(df)

        errors_df = pl.concat(errors_dfs, how="align")  # type: ignore
        aggregates = calculate_aggregates(errors_df)

        return errors_df, aggregates

    def error_report(self, submission_info: SubmissionInfo, status: SubmissionStatus):
        """Creates the error reports given a submission info and submission status"""
        if not self.processed_files_path:
            raise AttributeError("processed files path not provided")

        errors_df, aggregates = self._get_error_dataframes(submission_info.submission_id)

        if not status.number_of_records:
            sub_stats = None
        else:
            err_types = {
                rw.get("Type"): rw.get("Count")
                for rw in aggregates.group_by(pl.col("Type"))  # type: ignore
                .agg(pl.col("Count").sum())  # type: ignore
                .iter_rows(named=True)
            }
            sub_stats = SubmissionStatisticsRecord(
                submission_id=submission_info.submission_id,
                record_count=status.number_of_records,
                number_record_rejections=err_types.get("Submission Failure", 0),
                number_warnings=err_types.get("Warning", 0),
            )

        summary_dict = {
            key.replace("_", " ").title(): value
            for key, value in submission_info.dict().items()
            if value is not None and not key.endswith("_updated")
        }
        summary_items = er.SummaryItems(
            summary_dict=summary_dict,
            row_headings=["Submission Failure", "Warning"],
        )

        workbook = er.ExcelFormat(
            error_details=errors_df, error_aggregates=aggregates
        ).excel_format(summary_items=summary_items)

        report_uri = fh.joinuri(
            self.processed_files_path,
            submission_info.submission_id,
            "error_reports",
            f"{submission_info.file_name}_{submission_info.file_extension.strip('.')}.xlsx",
        )
        with fh.open_stream(report_uri, "wb") as stream:
            stream.write(er.ExcelFormat.convert_to_bytes(workbook))

        return submission_info, status, sub_stats, report_uri

    def error_report_step(
        self,
        pool: Executor,
        processed: Iterable[tuple[SubmissionInfo, SubmissionStatus]] = tuple(),
        failed_file_transformation: Iterable[SubmissionInfo] = tuple(),
    ) -> list[
        tuple[SubmissionInfo, SubmissionStatus, Union[None, SubmissionStatisticsRecord], URI]
    ]:
        """Step to produce error reports
        takes processed files and files that failed file transformation
        """
        futures: list[tuple[SubmissionInfo, Future]] = []
        reports: list[
            tuple[SubmissionInfo, SubmissionStatus, Union[None, SubmissionStatisticsRecord], URI]
        ] = []
        failed_processing: list[SubmissionInfo] = []

        for info, status in processed:
            futures.append((info, pool.submit(self.error_report, info, status)))

        for info_dict in failed_file_transformation:
            status = SubmissionStatus(True, 0)
            futures.append((info_dict, pool.submit(self.error_report, info_dict, status)))

        for sub_info, future in futures:
            try:
                submission_info, status, stats, feedback_uri = future.result()
            except AttributeError as exc:
                self._logger.error(f"Error reports raised exception: {exc}")
                raise exc
            except PERMISSIBLE_EXCEPTIONS as exc:
                self._logger.warning(
                    f"Error reports raised exception: {exc}. Will be retried later."
                )
                continue
            except Exception as exc:  # pylint: disable=W0703
                self._logger.error(f"Error reports raised exception: {exc}")
                self._logger.exception(exc)
                failed_processing.append(sub_info)
                continue
            reports.append((submission_info, status, stats, feedback_uri))

        if reports:
            self._audit_tables.mark_finished(
                [
                    (submission_info.submission_id, status.submission_result)  # type: ignore
                    for submission_info, status, _stats, _feedback_uri in reports
                ],
                job_run_id=self.job_run_id,
            )
            self._audit_tables.add_submission_statistics_records(
                [stats for _submission_info, _status, stats, _feedback_uri in reports if stats]
            )

        if failed_processing:
            self._audit_tables.mark_failed(
                [submission_info.submission_id for submission_info in failed_processing],
                job_run_id=self.job_run_id,
            )

        return reports

    def cluster_pipeline_run(
        self, max_workers: int = 7
    ) -> Iterator[list[tuple[SubmissionInfo, SubmissionStatus, URI]]]:
        """Method for running the full DVE pipeline from start to finish."""
        submission_files = self._get_submission_files_for_run()

        # parse files to parquet order doesn't matter
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            with self._audit_tables:
                audited, _ = self.audit_received_file_step(pool, submission_files)
                # what should we do with files that fail auditing - likely to be an internal matter -
                # no error report required?
                transformed, failed_transformation = self.file_transformation_step(pool, audited)
                passed_contract, _failed_contract = self.data_contract_step(pool, transformed)
                passed_br, failed_br, _failed_br_other_reason = self.business_rule_step(
                    pool, passed_contract
                )

                report_results = self.error_report_step(
                    pool,
                    [
                        *passed_br,
                        *failed_br,
                    ],
                    failed_transformation,
                )

        yield from report_results  # type: ignore
