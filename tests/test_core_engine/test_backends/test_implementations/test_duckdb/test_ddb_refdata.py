import shutil

import duckdb
import pytest
from dve.core_engine.backends.implementations.duckdb.reference_data import DuckDBRefDataLoader
from dve.core_engine.backends.base.reference_data import ReferenceFile
from tempfile import TemporaryDirectory

from tests.conftest import get_test_file_path

@pytest.fixture(scope="module")
def temp_working_dir():
    with TemporaryDirectory(prefix="refdata_test") as tmp:
        refdata_path = get_test_file_path("movies/refdata/movies_sequels.arrow")
        shutil.copy(refdata_path.as_posix(), tmp)
        yield tmp

@pytest.fixture(scope="module")
def ddb_refdata_loader(temp_working_dir):
    DuckDBRefDataLoader.connection = duckdb.connect()
    DuckDBRefDataLoader.dataset_config_uri = temp_working_dir
    yield DuckDBRefDataLoader

def test_load_arrow_file(ddb_refdata_loader):
    config = {
        "test_refdata": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow")
    }
    refdata_loader: DuckDBRefDataLoader = ddb_refdata_loader(config)
    
    test = refdata_loader.load_file(config.get("test_refdata"))
    
    assert test.shape == (2, 3)