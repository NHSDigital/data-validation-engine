from pathlib import Path
import shutil

import pytest
from dve.core_engine.backends.exceptions import MissingRefDataEntity, RefdataLacksFileExtensionSupport
from dve.core_engine.backends.implementations.spark.reference_data import SparkRefDataLoader
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
def spark_refdata_loader(spark, temp_working_dir):
    SparkRefDataLoader.spark = spark
    SparkRefDataLoader.dataset_config_uri = temp_working_dir
    yield SparkRefDataLoader, temp_working_dir

@pytest.fixture(scope="function")
def spark_refdata_table(spark_refdata_loader, spark_test_database):
    refdata_loader, _ = spark_refdata_loader
    tbl = "movies_sequels"
    refdata_loader.spark.read.parquet(get_test_file_path("movies/refdata/movies_sequels.parquet").as_posix()).write.saveAsTable(f"{spark_test_database}.{tbl}")
    yield spark_test_database, tbl
    refdata_loader.spark.sql(f"DROP TABLE IF EXISTS {spark_test_database}.{tbl}")


def test_load_parquet_file(spark_refdata_loader):
    refdata_loader, _ = spark_refdata_loader
    config = {
        "test_refdata": ReferenceFile(type="filename",
                                      filename="./movies_sequels.parquet")
    }
    spk_refdata_loader: SparkRefDataLoader = refdata_loader(config)
    
    test = spk_refdata_loader.load_file(config.get("test_refdata"))
    
    assert test.count() == 2

def test_load_uri_parquet(spark_refdata_loader):
    refdata_dir: Path
    refdata_loader, refdata_dir = spark_refdata_loader
    config = {
        "test_refdata": ReferenceURI(type="uri",
                                      uri=Path(refdata_dir).joinpath("movies_sequels.parquet").as_posix())
    }
    spk_refdata_loader: SparkRefDataLoader = refdata_loader(config)
    
    test = spk_refdata_loader.load_uri(config.get("test_refdata"))
    
    assert test.count() == 2

def test_table_read(spark_refdata_loader, spark_refdata_table):
    refdata_loader, _ = spark_refdata_loader
    db, tbl = spark_refdata_table
    config = {
        "test_refdata": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    spk_refdata_loader: SparkRefDataLoader = refdata_loader(config)
    
    test = spk_refdata_loader.load_table(config.get("test_refdata"))
    
    assert test.count() == 2

def test_via_entity_manager(spark_refdata_loader, spark_refdata_table):
    refdata_loader, refdata_dir = spark_refdata_loader
    db, tbl = spark_refdata_table
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.parquet"),
        "test_refdata_uri": ReferenceURI(type="uri",
                                      uri=Path(refdata_dir).joinpath("movies_sequels.parquet").as_posix()),
        "test_refdata_table": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    em = EntityManager({}, reference_data=refdata_loader(config))
    assert em.get("refdata_test_refdata_file").count() == 2
    assert em.get("refdata_test_refdata_uri").count() == 2
    assert em.get("refdata_test_refdata_table").count() == 2

def test_refdata_error(spark_refdata_loader):
    refdata_loader, _ = spark_refdata_loader
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow")
    }
    em = EntityManager({}, reference_data=refdata_loader(config))
    with pytest.raises(MissingRefDataEntity):
        em["refdata_missing"]
        em["refdata_test_refdata_file"]
    