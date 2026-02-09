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
def ddb_refdata_loader(temp_working_dir, temp_ddb_conn):
    _, conn = temp_ddb_conn
    DuckDBRefDataLoader.connection = conn
    DuckDBRefDataLoader.dataset_config_uri = temp_working_dir
    yield DuckDBRefDataLoader, temp_working_dir

@pytest.fixture(scope="function")
def ddb_refdata_table(ddb_refdata_loader):
    refdata_loader, _ = ddb_refdata_loader
    schema = "dve_" + uuid4().hex
    tbl = "movies_sequels"
    refdata_loader.connection.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    refdata_loader.connection.read_parquet(get_test_file_path("movies/refdata/movies_sequels.parquet").as_posix()).to_table(f"{schema}.{tbl}")
    yield schema, tbl
    refdata_loader.connection.sql(f"DROP TABLE IF EXISTS {schema}.{tbl}")
    refdata_loader.connection.sql(f"DROP SCHEMA IF EXISTS {schema}")

def test_load_arrow_file(ddb_refdata_loader):
    refdata_loader, _ = ddb_refdata_loader
    config = {
        "test_refdata": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow")
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = refdata_loader(config)
    
    test = duckdb_refdata_loader.load_file(config.get("test_refdata"))
    
    assert test.shape == (3, 3)

def test_load_parquet_file(ddb_refdata_loader):
    refdata_loader, _ = ddb_refdata_loader
    config = {
        "test_refdata": ReferenceFile(type="filename",
                                      filename="./movies_sequels.parquet")
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = refdata_loader(config)
    
    test = duckdb_refdata_loader.load_file(config.get("test_refdata"))
    
    assert test.shape == (2, 3)

def test_load_uri_parquet(ddb_refdata_loader):
    refdata_dir: Path
    refdata_loader, refdata_dir = ddb_refdata_loader
    config = {
        "test_refdata": ReferenceURI(type="uri",
                                      uri=Path(refdata_dir).joinpath("movies_sequels.parquet").as_posix())
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = refdata_loader(config)
    
    test = duckdb_refdata_loader.load_uri(config.get("test_refdata"))
    
    assert test.shape == (2, 3)

def test_load_uri_arrow(ddb_refdata_loader):
    refdata_loader, refdata_dir = ddb_refdata_loader
    config = {
        "test_refdata": ReferenceURI(type="uri",
                                      uri=Path(refdata_dir).joinpath("movies_sequels.arrow").as_posix())
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = refdata_loader(config)
    
    test = duckdb_refdata_loader.load_uri(config.get("test_refdata"))
    
    assert test.shape == (3, 3)

def test_table_read(ddb_refdata_loader, ddb_refdata_table):
    refdata_loader, _ = ddb_refdata_loader
    db, tbl = ddb_refdata_table
    config = {
        "test_refdata": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = refdata_loader(config)
    
    test = duckdb_refdata_loader.load_table(config.get("test_refdata"))
    
    assert test.shape == (2, 3)

def test_via_entity_manager(ddb_refdata_loader, ddb_refdata_table):
    refdata_loader, refdata_dir = ddb_refdata_loader
    db, tbl = ddb_refdata_table
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow"),
        "test_refdata_uri": ReferenceURI(type="uri",
                                      uri=Path(refdata_dir).joinpath("movies_sequels.parquet").as_posix()),
        "test_refdata_table": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    em = EntityManager({}, reference_data=refdata_loader(config))
    assert em.get("refdata_test_refdata_file").shape == (3, 3)
    assert em.get("refdata_test_refdata_uri").shape == (2, 3)
    assert em.get("refdata_test_refdata_table").shape == (2, 3)

def test_refdata_error(ddb_refdata_loader):
    refdata_loader, refdata_dir = ddb_refdata_loader
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow")
    }
    duckdb_refdata_loader: DuckDBRefDataLoader = refdata_loader(config)
    with pytest.raises(MissingRefDataEntity):
        duckdb_refdata_loader["missing_refdata"]