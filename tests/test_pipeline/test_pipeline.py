"""
Test Pipeline object methods
"""
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

import tempfile
from pathlib import Path
from pyspark.sql import DataFrame
from uuid import uuid4

from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager
from dve.core_engine.models import SubmissionInfo
from dve.pipeline.pipeline import BaseDVEPipeline

from .pipeline_helpers import PLANETS_RULES_PATH, planet_test_files  # pylint: disable=unused-import


def test_get_submission_files_for_run(planet_test_files):  # pylint: disable=redefined-outer-name
    dve_pipeline = BaseDVEPipeline(
        audit_tables=None,
        job_run_id=1,
        data_contract=None,
        step_implementations=None,
        rules_path=None,
        processed_files_path=planet_test_files,
        submitted_files_path=planet_test_files,
        reference_data_loader=None,
    )

    result = list(dve_pipeline._get_submission_files_for_run())

    assert len(result) == 2


def test_write_file_to_parquet(planet_test_files):  # pylint: disable=redefined-outer-name
    dve_pipeline = BaseDVEPipeline(
        audit_tables=None,
        job_run_id=1,
        data_contract=None,
        step_implementations=None,
        rules_path=PLANETS_RULES_PATH,
        processed_files_path=planet_test_files,
        submitted_files_path=planet_test_files,
        reference_data_loader=None,
    )

    sub_id = uuid4().hex

    submitted_files = list(dve_pipeline._get_submission_files_for_run())[0]

    submitted_file_info = dve_pipeline.audit_received_file(sub_id, *submitted_files)

    output_path = Path(planet_test_files, submitted_file_info.submission_id, "transform")

    errors = dve_pipeline.write_file_to_parquet(
        str(
            Path(
                planet_test_files,
                submitted_file_info.submission_id,
                submitted_file_info.file_name_with_ext,
            )
        ),
        submitted_file_info,
        planet_test_files,
        DataFrame
    )

    assert not errors
    assert output_path.joinpath("planets").exists()


def test_file_transformation(planet_test_files):  # pylint: disable=redefined-outer-name
    with tempfile.TemporaryDirectory() as tdir:
        dve_pipeline = BaseDVEPipeline(
            audit_tables=None,
            job_run_id=1,
            data_contract=None,
            step_implementations=None,
            rules_path=PLANETS_RULES_PATH,
            processed_files_path=tdir,
            submitted_files_path=planet_test_files,
            reference_data_loader=None,
        )
        sub_id = uuid4().hex

        submitted_files = list(dve_pipeline._get_submission_files_for_run())[0]

        submitted_file_info = dve_pipeline.audit_received_file(sub_id, *submitted_files)

        output_path = Path(tdir, submitted_file_info.submission_id, "transform")

        sub_info, sub_status = dve_pipeline.file_transformation(submitted_file_info)

        assert isinstance(sub_info, SubmissionInfo)
        assert not sub_status.processing_failed
        assert output_path.joinpath("planets").exists()
