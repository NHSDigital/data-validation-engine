"""Test that UDFs can be registered and called from Spark SQL."""

# pylint: disable=redefined-outer-name
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row

from dve.core_engine.backends.base.core import EntityManager
from dve.core_engine.backends.implementations.duckdb.rules import DuckDBStepImplemetations
from dve.core_engine.backends.implementations.spark.rules import SparkStepImplementations
from dve.core_engine.backends.metadata.rules import ColumnAddition
from tests.test_core_engine.test_backends.fixtures import duckdb_connection

from ..fixtures import spark  # pylint: disable=unused-import


def test_register_udfs_spark(spark: SparkSession):
    """Test that UDFs can be registered as spark functions."""

    step_impl = SparkStepImplementations.register_udfs(spark)

    df: DataFrame = spark.createDataFrame(  # type: ignore
        [{"Key": "Over", "A": 10}, {"Key": "Under", "A": 2}]
    )

    config_1 = ColumnAddition(
        entity_name="test_df",
        new_entity_name="test_df2",
        column_name="Is_Over",
        expression="over_5(A)",
    )

    entities = EntityManager({"test_df": df})
    step_impl.evaluate(entities, config=config_1)

    result = entities["test_df2"]
    assert set(result.collect()) == {
        Row(A=10, Key="Over", Is_Over=True),
        Row(A=2, Key="Under", Is_Over=False),
    }

    another_df: DataFrame = spark.createDataFrame(
        [{"nhs_no": "9651130636"}, {"nhs_no": "9173829205"}]
    )

    config_2 = ColumnAddition(
        entity_name="another_df",
        new_entity_name="another_df2",
        column_name="mod11_check",
        expression="nhsno_mod11_check(nhs_no)",
    )

    entities["another_df"] = another_df

    step_impl.evaluate(entities, config=config_2)

    result = entities["another_df2"]
    assert set(result.collect()) == {
        Row(nhs_no="9651130636", mod11_check=True),
        Row(nhs_no="9173829205", mod11_check=False),
    }

    assert "over_5" in step_impl.registered_functions
    assert "nhsno_mod11_check" in step_impl.registered_functions


def test_register_udfs_duckdb(duckdb_connection):
    """Test that UDFs can be registered as duckdb functions."""
    step_impl = DuckDBStepImplemetations.register_udfs(duckdb_connection)

    df: DataFrame = pd.DataFrame(  # type: ignore
        [{"Key": "Over", "A": 10}, {"Key": "Under", "A": 2}]
    )
    rel = step_impl._connection.from_df(df)

    config_1 = ColumnAddition(
        entity_name="test_rel",
        new_entity_name="test_rel2",
        column_name="Is_Over",
        expression="over_5(A)",
    )

    entities = EntityManager({"test_rel": rel})
    step_impl.evaluate(entities, config=config_1)

    result = entities["test_rel2"]
    assert sorted(result.df().to_dict(orient="records"), key=lambda x: x.get("A")) == [
        {"A": 2, "Key": "Under", "Is_Over": False},
        {"A": 10, "Key": "Over", "Is_Over": True},
    ]

    another_df: pd.DataFrame = pd.DataFrame([{"nhs_no": "9651130636"}, {"nhs_no": "9173829205"}])

    another_rel = step_impl._connection.from_df(another_df)
    config_2 = ColumnAddition(
        entity_name="another_rel",
        new_entity_name="another_rel2",
        column_name="mod11_check",
        expression="nhsno_mod11_check(nhs_no)",
    )

    entities["another_rel"] = another_rel

    step_impl.evaluate(entities, config=config_2)

    result = entities["another_rel2"]
    assert sorted(result.df().to_dict(orient="records"), key=lambda x: x.get("nhs_no")) == [
        {"nhs_no": "9173829205", "mod11_check": False},
        {"nhs_no": "9651130636", "mod11_check": True},
    ]
    assert "over_5" in step_impl.registered_functions
    assert "nhsno_mod11_check" in step_impl.registered_functions
