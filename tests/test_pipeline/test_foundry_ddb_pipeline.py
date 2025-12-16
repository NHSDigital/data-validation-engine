"""Test DuckDBPipeline object methods"""
# pylint: disable=missing-function-docstring
# pylint: disable=protected-access

from datetime import datetime
from pathlib import Path
import shutil
from uuid import uuid4

import pytest

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
            audit_tables=audit_manager,
            connection=conn,
            rules_path=get_test_file_path("planets/planets_ddb.dischema.json").as_posix(),
            processed_files_path=processing_folder,
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )
        output_loc, report_uri, audit_files = dve_pipeline.run_pipeline(sub_info)
        assert fh.get_resource_exists(report_uri)
        assert not output_loc
        assert len(list(fh.iter_prefix(audit_files))) == 2
        

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
            audit_tables=audit_manager,
            connection=conn,
            rules_path=get_test_file_path("movies/movies_ddb.dischema.json").as_posix(),
            processed_files_path=processing_folder,
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )
        output_loc, report_uri, audit_files = dve_pipeline.run_pipeline(sub_info)
        assert fh.get_resource_exists(report_uri)
        assert len(list(fh.iter_prefix(output_loc))) == 2
        assert len(list(fh.iter_prefix(audit_files))) == 2

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
            audit_tables=audit_manager,
            connection=conn,
            rules_path=get_test_file_path("planets/planets.dischema.json").as_posix(),
            processed_files_path=processing_folder,
            submitted_files_path=None,
            reference_data_loader=DuckDBRefDataLoader,
        )
        output_loc, report_uri, audit_files = dve_pipeline.run_pipeline(sub_info)
        assert fh.get_resource_exists(report_uri)
        assert not output_loc
        assert len(list(fh.iter_prefix(audit_files))) == 2
