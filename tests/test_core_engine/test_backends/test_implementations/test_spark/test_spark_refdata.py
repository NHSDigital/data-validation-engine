from pathlib import Path
import shutil

import pytest
from dve.core_engine.backends.exceptions import MissingRefDataEntity
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
def spark_refdata_table(spark, spark_test_database):
    tbl = "movies_sequels"
    spark.read.parquet(get_test_file_path("movies/refdata/movies_sequels.parquet").as_posix()).write.saveAsTable(f"{spark_test_database}.{tbl}")
    yield spark_test_database, tbl
    spark.sql(f"DROP TABLE IF EXISTS {spark_test_database}.{tbl}")


def test_load_parquet_file(spark, temp_working_dir):
    config = {
        "test_refdata": ReferenceFile(type="filename",
                                      filename="./movies_sequels.parquet")
    }
    spk_refdata_loader: SparkRefDataLoader = SparkRefDataLoader(spark=spark,
                                                                reference_data_config=config,
                                                                dataset_config_uri=temp_working_dir)
    
    test = spk_refdata_loader.load_file(config.get("test_refdata"))
    
    assert test.count() == 2

def test_load_uri_parquet(spark, temp_working_dir):
    config = {
        "test_refdata": ReferenceURI(type="uri",
                                      uri=Path(temp_working_dir).joinpath("movies_sequels.parquet").as_posix())
    }
    spk_refdata_loader: SparkRefDataLoader = SparkRefDataLoader(spark=spark,
                                                                reference_data_config=config,
                                                                dataset_config_uri=temp_working_dir)
    
    test = spk_refdata_loader.load_uri(config.get("test_refdata"))
    
    assert test.count() == 2

def test_table_read(spark, temp_working_dir, spark_refdata_table):
    db, tbl = spark_refdata_table
    config = {
        "test_refdata": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    spk_refdata_loader: SparkRefDataLoader = SparkRefDataLoader(spark=spark,
                                                                reference_data_config=config,
                                                                dataset_config_uri=temp_working_dir)
    
    test = spk_refdata_loader.load_table(config.get("test_refdata"))
    
    assert test.count() == 2

def test_via_entity_manager(spark, temp_working_dir, spark_refdata_table):
    db, tbl = spark_refdata_table
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.parquet"),
        "test_refdata_uri": ReferenceURI(type="uri",
                                      uri=Path(temp_working_dir).joinpath("movies_sequels.parquet").as_posix()),
        "test_refdata_table": ReferenceTable(type="table",
                                      table_name=tbl,
                                      database=db)
    }
    
    spk_refdata_loader: SparkRefDataLoader = SparkRefDataLoader(spark=spark,
                                                                reference_data_config=config,
                                                                dataset_config_uri=temp_working_dir)
    em = EntityManager({}, reference_data=spk_refdata_loader)
    assert em.get("refdata_test_refdata_file").count() == 2
    assert em.get("refdata_test_refdata_uri").count() == 2
    assert em.get("refdata_test_refdata_table").count() == 2

def test_refdata_error(spark, temp_working_dir):
    config = {
        "test_refdata_file": ReferenceFile(type="filename",
                                      filename="./movies_sequels.arrow")
    }
    
    spk_refdata_loader: SparkRefDataLoader = SparkRefDataLoader(spark=spark,
                                                                reference_data_config=config,
                                                                dataset_config_uri=temp_working_dir)
    em = EntityManager({}, reference_data=spk_refdata_loader)
    with pytest.raises(MissingRefDataEntity):
        em["refdata_missing"]
        em["refdata_test_refdata_file"]
    