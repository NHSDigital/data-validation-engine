"""Test DuckDBPipeline object methods"""
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

from datetime import datetime
from pathlib import Path
import shutil
import tempfile
from uuid import uuid4

import polars as pl

from dve.core_engine.backends.implementations.duckdb.auditing import DDBAuditingManager
from dve.core_engine.backends.implementations.duckdb.reference_data import DuckDBRefDataLoader
from dve.core_engine.models import SubmissionInfo
import dve.parser.file_handling as fh
from dve.pipeline.foundry_ddb_pipeline import FoundryDDBPipeline

from ..conftest import get_test_file_path
from ..fixtures import temp_ddb_conn  # pylint: disable=unused-import
from .pipeline_helpers import (  # pylint: disable=unused-import
    PLANETS_RULES_PATH,
    planet_test_files,
    movies_test_files
)

def test_foundry_runner_validation_fail(planet_test_files, temp_ddb_conn):
    db_file, conn = temp_ddb_conn
    processing_folder = planet_test_files
    sub_id = uuid4().hex
    sub_info = SubmissionInfo.from_metadata_file(submission_id=sub_id,
                                                 metadata_uri=processing_folder + "/planets_demo.metadata.json")
    sub_folder = processing_folder + f"/{sub_id}"
    
    shutil.copytree(planet_test_files, sub_folder)

    DuckDBRefDataLoader.connection = conn
    DuckDBRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)
    

    with DDBAuditingManager(db_file.as_uri(), None, conn) as audit_manager:
        dve_pipeline = FoundryDDBPipeline(
            processed_files_path=processing_folder,
            audit_tables=audit_manager,
            connection=conn,
            rules_path=get_test_file_path("planets/planets_ddb.dischema.json").as_posix(),
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )
        output_loc, report_uri, audit_files = dve_pipeline.run_pipeline(sub_info)
        assert fh.get_resource_exists(report_uri)
        assert not output_loc
        assert len(list(fh.iter_prefix(audit_files))) == 3
        

def test_foundry_runner_validation_success(movies_test_files, temp_ddb_conn):
    db_file, conn = temp_ddb_conn
    # add movies refdata to conn
    ref_db_file = Path(db_file.parent, f"movies_refdata.duckdb").as_posix()
    conn.sql(f"ATTACH '{ref_db_file}' AS movies_refdata")
    conn.read_parquet(get_test_file_path("movies/refdata/movies_sequels.parquet").as_posix()).to_table(f"movies_refdata.sequels")
    processing_folder = movies_test_files
    sub_id = uuid4().hex
    sub_info = SubmissionInfo(submission_id=sub_id,
                              dataset_id="movies",
                              file_name="good_movies",
                              file_extension="json",
                              submitting_org="TEST",
                              datetime_received=datetime(2025,11,5))
    sub_folder = processing_folder + f"/{sub_id}"
    
    shutil.copytree(movies_test_files, sub_folder)

    DuckDBRefDataLoader.connection = conn
    DuckDBRefDataLoader.dataset_config_uri = None
    

    with DDBAuditingManager(db_file.as_uri(), None, conn) as audit_manager:
        dve_pipeline = FoundryDDBPipeline(
            processed_files_path=processing_folder,
            audit_tables=audit_manager,
            connection=conn,
            rules_path=get_test_file_path("movies/movies_ddb.dischema.json").as_posix(),
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )
        output_loc, report_uri, audit_files = dve_pipeline.run_pipeline(sub_info)
        assert fh.get_resource_exists(report_uri)
        assert len(list(fh.iter_prefix(output_loc))) == 2
        assert len(list(fh.iter_prefix(audit_files))) == 3

def test_foundry_runner_error(planet_test_files, temp_ddb_conn):
    # using spark reader config - should error in file transformation - check gracefully handled
    db_file, conn = temp_ddb_conn
    processing_folder = planet_test_files
    sub_id = uuid4().hex
    sub_info = SubmissionInfo.from_metadata_file(submission_id=sub_id,
                                                 metadata_uri=processing_folder + "/planets_demo.metadata.json")
    sub_folder = processing_folder + f"/{sub_id}"
    
    shutil.copytree(planet_test_files, sub_folder)

    DuckDBRefDataLoader.connection = conn
    DuckDBRefDataLoader.dataset_config_uri = fh.get_parent(PLANETS_RULES_PATH)
    

    with DDBAuditingManager(db_file.as_uri(), None, conn) as audit_manager:
        dve_pipeline = FoundryDDBPipeline(
            processed_files_path=processing_folder,
            audit_tables=audit_manager,
            connection=conn,
            rules_path=get_test_file_path("planets/planets.dischema.json").as_posix(),
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )
        output_loc, report_uri, audit_files = dve_pipeline.run_pipeline(sub_info)
        assert not fh.get_resource_exists(report_uri)
        assert not output_loc

        perror_path = Path(
            processing_folder,
            sub_info.submission_id,
            "processing_errors",
            "processing_errors.jsonl"
        )
        assert perror_path.exists()
        perror_schema = {
            "step_name": pl.Utf8(),
            "error_location": pl.Utf8(),
            "error_level": pl.Utf8(),
            "error_message": pl.Utf8(),
            "error_traceback": pl.List(pl.Utf8()),
        }
        expected_error_df = (
            pl.DataFrame(
                [
                    {
                        "step_name": "file_transformation",
                        "error_location": "processing",
                        "error_level": "integrity",
                        "error_message": "ReaderLacksEntityTypeSupport()",
                        "error_traceback": None,
                    },
                ],
                perror_schema
            )
            .select(pl.col("step_name"), pl.col("error_location"), pl.col("error_message"))
        )
        actual_error_df = (
            pl.read_json(perror_path, schema=perror_schema)
            .select(pl.col("step_name"), pl.col("error_location"), pl.col("error_message"))
        )
        assert actual_error_df.equals(expected_error_df)

        assert len(list(fh.iter_prefix(audit_files))) == 2


def test_foundry_runner_with_submitted_files_path(movies_test_files, temp_ddb_conn):
    db_file, conn = temp_ddb_conn
    ref_db_file = Path(db_file.parent, "movies_refdata.duckdb").as_posix()
    conn.sql(f"ATTACH '{ref_db_file}' AS movies_refdata")
    conn.read_parquet(
        get_test_file_path("movies/refdata/movies_sequels.parquet").as_posix()
    ).to_table("movies_refdata.sequels")
    processing_folder = Path(tempfile.mkdtemp()).as_posix()
    submitted_files_path = Path(movies_test_files).as_posix()
    sub_id = uuid4().hex
    sub_info = SubmissionInfo(
        submission_id=sub_id,
        dataset_id="movies",
        file_name="good_movies",
        file_extension="json",
        submitting_org="TEST",
        datetime_received=datetime(2025,11,5)
    )

    DuckDBRefDataLoader.connection = conn
    DuckDBRefDataLoader.dataset_config_uri = None

    with DDBAuditingManager(db_file.as_uri(), None, conn) as audit_manager:
        dve_pipeline = FoundryDDBPipeline(
            processed_files_path=processing_folder,
            audit_tables=audit_manager,
            connection=conn,
            rules_path=get_test_file_path("movies/movies_ddb.dischema.json").as_posix(),
            submitted_files_path=submitted_files_path,
            reference_data_loader=DuckDBRefDataLoader,
        )
        output_loc, report_uri, audit_files = dve_pipeline.run_pipeline(sub_info)

        assert Path(processing_folder, sub_id, sub_info.file_name_with_ext).exists()
        assert fh.get_resource_exists(report_uri)
        assert len(list(fh.iter_prefix(output_loc))) == 2
        assert len(list(fh.iter_prefix(audit_files))) == 3
