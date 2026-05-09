from pathlib import Path
import shutil
from uuid import uuid4

import pytest
from dve.core_engine.backends.exceptions import MissingRefDataEntity
from dve.core_engine.backends.implementations.duckdb.reference_data import DuckDBRefDataLoader
from dve.core_engine.backends.base.core import EntityManager
from dve.core_engine.backends.base.reference_data import ReferenceFile, ReferenceTable, ReferenceURI

from tempfile import TemporaryDirectory

from tests.conftest import get_test_file_path

@pytest.fixture(scope="module")
def temp_working_dir():
    with TemporaryDirectory(prefix="refdata_test") as tmp:
        refdata_path = get_test_file_path("movies/refdata")
        shutil.copytree(refdata_path.as_posix(), tmp, dirs_exist_ok=True)
        yield tmp


@pytest.fixture(scope="function")
def ddb_refdata_table(temp_ddb_conn):
    _, conn = temp_ddb_conn
    schema = "dve_" + uuid4().hex
    tbl = "movies_sequels"
    conn.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.read_parquet(get_test_file_path("movies/refdata/movies_sequels.parquet").as_posix()).to_table(f"{schema}.{tbl}")
    yield schema, tbl
    conn.sql(f"DROP TABLE IF EXISTS {schema}.{tbl}")
    conn.sql(f"DROP SCHEMA IF EXISTS {schema}")

def test_load_arrow_file(temp_working_dir, temp_ddb_conn):
    _, conn = temp_ddb_conn
    config = {
        "test_refdata": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow")
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = DuckDBRefDataLoader(connection=conn,
                                                                     reference_data_config=config,
                                                                     dataset_config_uri=temp_working_dir)
    
    test = duckdb_refdata_loader.load_file(config.get("test_refdata"))
    
    assert test.shape == (3, 3)

def test_load_parquet_file(temp_working_dir, temp_ddb_conn):
    _, conn = temp_ddb_conn
    config = {
        "test_refdata": ReferenceFile(type="filename",
                                      filename="./movies_sequels.parquet")
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = DuckDBRefDataLoader(connection=conn,
                                                                     reference_data_config=config,
                                                                     dataset_config_uri=temp_working_dir)
    
    test = duckdb_refdata_loader.load_file(config.get("test_refdata"))
    
    assert test.shape == (2, 3)

def test_load_uri_parquet(temp_working_dir, temp_ddb_conn):
    _, conn = temp_ddb_conn
    config = {
        "test_refdata": ReferenceURI(type="uri",
                                      uri=Path(temp_working_dir).joinpath("movies_sequels.parquet").as_posix())
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = DuckDBRefDataLoader(connection=conn,
                                                                     reference_data_config=config,
                                                                     dataset_config_uri=temp_working_dir)
    
    test = duckdb_refdata_loader.load_uri(config.get("test_refdata"))
    
    assert test.shape == (2, 3)

def test_load_uri_arrow(temp_working_dir, temp_ddb_conn):
    _, conn = temp_ddb_conn
    config = {
        "test_refdata": ReferenceURI(type="uri",
                                      uri=Path(temp_working_dir).joinpath("movies_sequels.arrow").as_posix())
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = DuckDBRefDataLoader(connection=conn,
                                                                     reference_data_config=config,
                                                                     dataset_config_uri=temp_working_dir)
    
    test = duckdb_refdata_loader.load_uri(config.get("test_refdata"))
    
    assert test.shape == (3, 3)

def test_table_read(temp_working_dir, temp_ddb_conn, ddb_refdata_table):
    _, conn = temp_ddb_conn
    db, tbl = ddb_refdata_table
    config = {
        "test_refdata": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = DuckDBRefDataLoader(connection=conn,
                                                                     reference_data_config=config,
                                                                     dataset_config_uri=temp_working_dir)
    
    test = duckdb_refdata_loader.load_table(config.get("test_refdata"))
    
    assert test.shape == (2, 3)

def test_via_entity_manager(temp_working_dir, temp_ddb_conn, ddb_refdata_table):
    _, conn = temp_ddb_conn
    db, tbl = ddb_refdata_table
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow"),
        "test_refdata_uri": ReferenceURI(type="uri",
                                      uri=Path(temp_working_dir).joinpath("movies_sequels.parquet").as_posix()),
        "test_refdata_table": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    refdata_loader: DuckDBRefDataLoader = DuckDBRefDataLoader(connection=conn,
                                                              reference_data_config=config,
                                                              dataset_config_uri=temp_working_dir)
    em = EntityManager({}, reference_data=refdata_loader)
    assert em.get("refdata_test_refdata_file").shape == (3, 3)
    assert em.get("refdata_test_refdata_uri").shape == (2, 3)
    assert em.get("refdata_test_refdata_table").shape == (2, 3)

def test_refdata_error(temp_working_dir, temp_ddb_conn):
    _, conn = temp_ddb_conn
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow")
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = DuckDBRefDataLoader(connection=conn,
                                                                     reference_data_config=config,
                                                                     dataset_config_uri=temp_working_dir)
    with pytest.raises(MissingRefDataEntity):
        duckdb_refdata_loader["missing_refdata"]