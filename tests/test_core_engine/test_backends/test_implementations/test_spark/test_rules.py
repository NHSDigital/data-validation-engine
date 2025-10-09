"""Test Spark backend steps."""

# pylint: disable=redefined-outer-name,unused-import,line-too-long
from pathlib import Path
from typing import List, Optional, Set, Tuple, Type

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    ArrayType,
    DateType,
    LongType,
    Row,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from dve.core_engine.backends.base.core import EntityManager
from dve.core_engine.backends.exceptions import MissingEntity
from dve.core_engine.backends.implementations.spark.rules import SparkStepImplementations
from dve.core_engine.backends.metadata.rules import (
    Aggregation,
    AntiJoin,
    ColumnAddition,
    ColumnRemoval,
    ConfirmJoinHasMatch,
    CopyEntity,
    EntityRemoval,
    HeaderJoin,
    InnerJoin,
    LeftJoin,
    OneToOneJoin,
    OrphanIdentification,
    RenameEntity,
    SelectColumns,
    SemiJoin,
    TableUnion,
)
from dve.core_engine.type_hints import MultipleExpressions
from tests.test_core_engine.test_backends.fixtures import (
    largest_satellites_df,
    nested_typecast_parquet,
    planets_df,
    satellites_df,
    simple_typecast_parquet,
    value_literal_1_header,
)

SPARK_STEP_BACKEND = SparkStepImplementations()
"""The backend for the spark steps."""


def test_column_addition(planets_df: DataFrame):
    """Test that columns can be added to entities."""
    entities = EntityManager({"planets": planets_df})
    rule = ColumnAddition(
        entity_name="planets",
        column_name="literal_one",
        expression=1,
        new_entity_name="added",
    )
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert (set(entities["added"].columns) - set(entities["planets"].columns)) == {"literal_one"}

    assert rule.get_required_entities() == {"planets"}
    assert rule.get_created_entities() == {"added"}
    assert rule.get_removed_entities() == set()


def test_column_addition_missing_entity():
    """Test that columns can be added to entities."""
    entities = EntityManager({})
    rule = ColumnAddition(
        entity_name="planets",
        column_name="literal_one",
        expression=1,
        new_entity_name="added",
    )
    messages, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert not success, "Rule expected to fail"
    assert len(messages) == 1, "Expected single message"
    assert messages[0].error_message.startswith("Missing entity 'planets' required by")


def test_column_removal(planets_df: DataFrame):
    """Test that columns can be removed from entities."""
    entities = EntityManager({"planets": planets_df})
    rule = ColumnRemoval(entity_name="planets", column_name="mass")
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert (set(planets_df.columns) - set(entities["planets"].columns)) == {"mass"}


@pytest.mark.parametrize(
    "column_spec",
    [
        ["planet", "mass"],
        {"planet": "planet", "mass": "mass"},
        "planet, mass",
        "planet AS planet, mass AS mass",
    ],
)
def test_select_columns(planets_df: DataFrame, column_spec: MultipleExpressions):
    """Test that columns can be selected from columns."""
    rule = SelectColumns(entity_name="planets", columns=column_spec)
    entities = EntityManager({"planets": planets_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    planets_df = entities["planets"]
    assert planets_df.columns == ["planet", "mass"]


def test_select_columns_expressions(planets_df: DataFrame):
    """Test that columns can be selected from columns using expressions."""
    rule = SelectColumns(
        entity_name="planets",
        columns={"planet": "planet", "upper(planet)": "upper_planet"},
    )
    entities = EntityManager({"planets": planets_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    planets_df = entities["planets"]
    assert all(row.planet.upper() == row.upper_planet for row in planets_df.collect())


def test_select_columns_expressions(planets_df: DataFrame):
    """Test that columns can be selected from columns using expressions."""
    rule = SelectColumns(
        entity_name="planets",
        columns={"planet": "planet", "upper(planet)": "upper_planet"},
    )
    entities = EntityManager({"planets": planets_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    planets_df = entities["planets"]
    assert all(row.planet.upper() == row.upper_planet for row in planets_df.collect())


def test_select_columns_from_refdata(planets_df: DataFrame):
    """Test that columns can be selected from reference data entries."""
    rule = SelectColumns(
        entity_name="refdata_planets",
        columns={"planet": "planet", "upper(planet)": "upper_planet"},
        new_entity_name="planets",
    )
    entities = EntityManager({}, {"planets": planets_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    planets_df = entities["planets"]
    assert all(row.planet.upper() == row.upper_planet for row in planets_df.collect())


def test_cannot_override_refdata(planets_df: DataFrame):
    """Test that reference data cannot be overridden."""
    rule = SelectColumns(
        entity_name="refdata_planets",
        columns={"planet": "planet", "upper(planet)": "upper_planet"},
    )
    entities = EntityManager({}, {"planets": planets_df})

    messages, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert not success, "Rule expected to fail"
    assert len(messages) == 1, "Expected single message"
    assert messages[0].error_message.startswith("Constraint violated in ")
    assert messages[0].error_message.endswith(
        ": reference data entry 'planets' must not be mutated"
    )


def test_select_with_expression_requiring_multialias():
    """Test that columns can be selected when the expression requires multiple aliases."""
    intlist_df = SparkSession.builder.getOrCreate().createDataFrame([Row(intlist=[1, 2, 3])])

    rule = SelectColumns(entity_name="intlist", columns={"posexplode(intlist)": ["Index", "Value"]})
    entities = EntityManager({"intlist": intlist_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    posexploded_intlist_df = entities["intlist"]

    assert set(posexploded_intlist_df.columns) == {"Index", "Value"}
    assert posexploded_intlist_df.count() == 3


@pytest.mark.parametrize("group_by_spec", ["planet", ["planet"], {"planet": "planet"}])
@pytest.mark.parametrize(
    "agg_spec",
    ["max_by(name, gm) AS name", ["max_by(name, gm) AS name"], {"max_by(name, gm)": "name"}],
)
def test_aggregation(
    satellites_df: DataFrame,
    largest_satellites_df: DataFrame,
    group_by_spec: MultipleExpressions,
    agg_spec: MultipleExpressions,
):
    """Test that the aggregations work as expected."""
    rule = Aggregation(entity_name="satellites", group_by=group_by_spec, agg_columns=agg_spec)
    entities = EntityManager({"satellites": satellites_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    actual = entities["satellites"]

    actual_rows = sorted(actual.collect(), key=lambda row: row.planet)
    expected = largest_satellites_df.select(*map(col, actual.columns))
    expected_rows = sorted(expected.collect(), key=lambda row: row.planet)
    assert actual_rows == expected_rows


@pytest.mark.parametrize("pivot_values", [None, ["Earth", "Mars"]])
def test_aggregation_pivot(planets_df: DataFrame, pivot_values: Optional[List[str]]):
    """Test that the aggregations work as expected with a pivot."""
    # Creating a dataset with a column for each planet, and a single row
    # containing the masses.
    rule = Aggregation(
        entity_name="planets",
        group_by={},
        pivot_column="planet",
        pivot_values=pivot_values,
        agg_columns="FIRST(mass)",
    )
    entities = EntityManager({"planets": planets_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    actual = entities["planets"]
    actual_record = actual.first().asDict()

    all_rows = planets_df.collect()
    expected_record = {}
    for row in all_rows:
        planet, mass = row.planet, row.mass
        if pivot_values and planet not in pivot_values:
            continue

        expected_record[planet] = mass

    assert actual_record == expected_record


def test_aggregation_raises_no_pivot_col_but_values():
    """
    Ensure that an aggregation cannot be created with a pivot column and no
    values.

    """
    with pytest.raises(ValueError):
        Aggregation(
            entity_name="planets",
            group_by={},
            pivot_values=["Earth", "Mars"],
            agg_columns="FIRST(mass)",
        )


def test_entity_removal(planets_df: DataFrame):
    """Test that entities can be removed."""
    rule = EntityRemoval(entity_name="planets")
    entities = EntityManager({"planets": planets_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert not entities

    assert rule.get_required_entities() == set()
    assert rule.get_created_entities() == set()
    assert rule.get_removed_entities() == {"planets"}


def test_rename_entity(planets_df: DataFrame):
    """Test that entities can be renamed."""
    rule = RenameEntity(entity_name="planets", new_entity_name="planets_renamed")
    entities = EntityManager({"planets": planets_df})

    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert entities == {"planets_renamed": planets_df}

    assert rule.get_required_entities() == {"planets"}
    assert rule.get_created_entities() == {"planets_renamed"}
    assert rule.get_removed_entities() == {"planets"}


def test_copy_entity(planets_df: DataFrame):
    """Test that entities can be copied."""
    rule = CopyEntity(entity_name="planets", new_entity_name="planets_renamed")
    entities = EntityManager({"planets": planets_df})

    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert entities == {"planets": planets_df, "planets_renamed": planets_df}

    assert rule.get_required_entities() == {"planets"}
    assert rule.get_created_entities() == {"planets_renamed"}
    assert rule.get_removed_entities() == set()


def test_missing_entity_removal():
    """Test that missing entities for removal succeds as expected."""
    rule = EntityRemoval(entity_name="planets")
    _, success = SPARK_STEP_BACKEND.evaluate({}, config=rule)
    assert success, "Rule application failed"


@pytest.mark.parametrize("join_type", [LeftJoin, OneToOneJoin, InnerJoin])
@pytest.mark.parametrize(
    "new_columns_spec",
    [
        "satellites.name AS satellite",
        ["satellites.name AS satellite"],
        {"satellites.name": "satellite"},
    ],
)
def test_join_planets_satellites(
    planets_df: DataFrame,
    largest_satellites_df: DataFrame,
    new_columns_spec: MultipleExpressions,
    join_type: Type[LeftJoin],
):
    """Test a basic join from satellites to planets."""
    join = join_type(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns=new_columns_spec,
    )
    entities = EntityManager({"planets": planets_df, "satellites": largest_satellites_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=join)
    assert success, "Rule application failed"
    actual_df = entities["planets"].select(col("planet"), col("satellite"))
    actual_rows = sorted(actual_df.collect(), key=lambda row: row.planet)

    expected_df = (
        planets_df.alias("planets")
        .join(
            largest_satellites_df.alias("satellites"),
            on=col("planets.planet") == col("satellites.planet"),
            how="inner" if join_type == InnerJoin else "left",
        )
        .select(col("planets.planet").alias("planet"), col("satellites.name").alias("satellite"))
    )
    expected_rows = sorted(expected_df.collect(), key=lambda row: row.planet)

    assert actual_rows == expected_rows


@pytest.mark.parametrize("join_type", [LeftJoin, OneToOneJoin, InnerJoin])
@pytest.mark.parametrize(
    "new_columns_spec",
    [
        "satellites.name AS satellite, 1 AS literal_one",
        ["satellites.name AS satellite", "1 AS literal_one"],
        {"satellites.name": "satellite", "1": "literal_one"},
    ],
)
def test_join_can_overwrite_existing_col(
    planets_df: DataFrame,
    largest_satellites_df: DataFrame,
    new_columns_spec: MultipleExpressions,
    join_type: Type[LeftJoin],
):
    """Test a basic join from satellites to planets can overwrite an existing column."""
    # Set a column with a name we plan to overwrite so we can test overwriting it.
    planets_df = planets_df.withColumn("satellite", lit("a satellite"))

    join = join_type(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns=new_columns_spec,
    )
    entities = EntityManager({"planets": planets_df, "satellites": largest_satellites_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=join)
    assert success, "Rule application failed"
    actual_df = entities["planets"].select(
        col("planet"),
        col("satellite"),
        col("literal_one"),
    )
    actual_rows = sorted(actual_df.collect(), key=lambda row: row.planet)

    expected_df = (
        planets_df.alias("planets")
        .join(
            largest_satellites_df.alias("satellites"),
            on=col("planets.planet") == col("satellites.planet"),
            how="inner" if join_type == InnerJoin else "left",
        )
        .select(
            col("planets.planet").alias("planet"),
            col("satellites.name").alias("satellite"),
            lit(1).alias("literal_one"),
        )
    )
    expected_rows = sorted(expected_df.collect(), key=lambda row: row.planet)

    assert actual_rows == expected_rows


@pytest.mark.parametrize("join_type", [LeftJoin, OneToOneJoin, InnerJoin])
@pytest.mark.parametrize(
    "new_columns_spec",
    [
        "*",
        "satellites.*",
    ],
)
def test_join_can_take_all_cols(
    planets_df: DataFrame,
    largest_satellites_df: DataFrame,
    new_columns_spec: MultipleExpressions,
    join_type: Type[LeftJoin],
):
    """Test a basic join from satellites to planets can overwrite an existing column."""
    # Set a column with a name we plan to overwrite so we can test overwriting it.
    planets_df = planets_df.withColumn("satellite_planet", lit("a satellite"))
    # Ensure we don't have any other column names the same in both by prefixing.
    largest_satellites_df = largest_satellites_df.select(
        *[
            col(column_name).alias(f"satellite_{column_name}")
            for column_name in largest_satellites_df.columns
        ]
    )

    join = join_type(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.satellite_planet",
        new_columns=new_columns_spec,
    )
    entities = EntityManager({"planets": planets_df, "satellites": largest_satellites_df})
    _, success = SPARK_STEP_BACKEND.evaluate(entities, config=join)
    assert success, "Join application failed"
    actual_df = entities["planets"]
    actual_rows = sorted(actual_df.collect(), key=lambda row: row.planet)

    expected_df = (
        planets_df.drop("satellite_planet")
        .alias("planets")
        .join(
            largest_satellites_df.alias("satellites"),
            on=col("planets.planet") == col("satellites.satellite_planet"),
            how="inner" if join_type == InnerJoin else "left",
        )
        .select("planets.*", "satellites.*")
    )
    expected_rows = sorted(expected_df.collect(), key=lambda row: row.planet)

    assert actual_rows == expected_rows


def test_one_to_one_join_multi_matches_raises(planets_df: DataFrame, satellites_df: DataFrame):
    """Test that a join which results in multiple records being pulled through will fail."""
    join = OneToOneJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns={"satellites.name": "satellite"},
    )
    entities = EntityManager({"planets": planets_df, "satellites": satellites_df})
    with pytest.raises(ValueError, match="Multiple matches for some records.+"):
        SPARK_STEP_BACKEND.one_to_one_join(entities, config=join)


def test_left_join_multi_matches_does_not_raise(planets_df: DataFrame, satellites_df: DataFrame):
    """Test that a join which results in multiple records being pulled through will succeed if it's not one to one."""
    join = LeftJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns={"satellites.name": "satellite"},
    )
    entities = EntityManager({"planets": planets_df, "satellites": satellites_df})
    SPARK_STEP_BACKEND.evaluate(entities, config=join)


def test_semi_join_works(planets_df: DataFrame, satellites_df: DataFrame):
    """Test that a join which results in multiple records being pulled through will fail."""
    join = SemiJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_entity_name="planets_with_satellite",
    )
    entities = EntityManager({"planets": planets_df, "satellites": satellites_df})
    SPARK_STEP_BACKEND.evaluate(entities, config=join)
    entity = entities["planets_with_satellite"]
    actual = sorted([row.planet for row in entity.collect()])
    expected = ["Earth", "Jupiter", "Mars", "Neptune", "Pluto", "Saturn", "Uranus"]
    assert actual == expected
    assert entity.schema == planets_df.schema


def test_anti_join_works(planets_df: DataFrame, satellites_df: DataFrame):
    """Test that a join which results in multiple records being pulled through will fail."""
    join = AntiJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_entity_name="planets_without_satellite",
    )
    entities = EntityManager({"planets": planets_df, "satellites": satellites_df})
    SPARK_STEP_BACKEND.evaluate(entities, config=join)
    entity = entities["planets_without_satellite"]
    actual = sorted([row.planet for row in entity.collect()])
    expected = ["Mercury", "Venus"]
    assert actual == expected
    assert entity.schema == planets_df.schema


def test_join_missing_entities_raises(planets_df: DataFrame, satellites_df: DataFrame):
    """Test that trying to join missing entities raises correctly."""
    join = OneToOneJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns={"satellites.name": "satellite"},
    )

    entities = EntityManager({"planets": planets_df})
    with pytest.raises(MissingEntity):
        SPARK_STEP_BACKEND.one_to_one_join(entities, config=join)
    entities = EntityManager({"satellites": satellites_df})
    with pytest.raises(MissingEntity):
        SPARK_STEP_BACKEND.one_to_one_join(entities, config=join)


def test_header_join_planets(planets_df: DataFrame, value_literal_1_header: DataFrame):
    """Test adding a header to the planets dataset."""
    header_join = HeaderJoin(
        entity_name="planets",
        target_name="header",
        new_entity_name="planets_header",
        header_column_name="_Header",
    )
    entities = EntityManager({"planets": planets_df, "header": value_literal_1_header})
    SPARK_STEP_BACKEND.evaluate(entities, config=header_join)

    header_value = value_literal_1_header.first()
    planet_rows = sorted(entities["planets"].collect(), key=lambda row: row.planet)
    actual_rows = sorted(entities["planets_header"].collect(), key=lambda row: row.planet)

    for planet_row, actual_row in zip(planet_rows, actual_rows):
        planet_dict = planet_row.asDict()
        actual_dict = actual_row.asDict()

        assert actual_dict.pop("_Header") == header_value, "Header does not match expected header"
        assert planet_dict == actual_dict, "More than header changed as result of header join"


def test_header_multi_rows_raises(planets_df: DataFrame, value_literal_1_header: DataFrame):
    """Ensure that adding a 'header' with multiple rows to the planets dataset raises an error."""
    header_join = HeaderJoin(
        entity_name="planets",
        target_name="header",
        new_entity_name="planets_header",
        header_column_name="_Header",
    )
    entities = EntityManager(
        {
            "planets": planets_df,
            "header": value_literal_1_header.unionAll(value_literal_1_header),
        }
    )
    with pytest.raises(
        ValueError, match="Unable to join header '.+' to '.+' as it contains multiple entries"
    ):
        SPARK_STEP_BACKEND.join_header(entities, config=header_join)


def test_orphans_planets_satellites(planets_df: DataFrame, largest_satellites_df: DataFrame):
    """Test a basic orphan idenfitication from satellites to planets."""
    # Each satellite _must_ have a planet.
    join = OrphanIdentification(
        entity_name="satellites",
        target_name="planets",
        join_condition="satellites.planet == planets.planet",
    )
    entities = EntityManager(
        {
            "planets": planets_df.filter(col("Planet") != lit("Mars")),
            "satellites": largest_satellites_df,
        }
    )

    SPARK_STEP_BACKEND.evaluate(entities, config=join)
    actual_df = entities["satellites"].filter(col("IsOrphaned")).select(col("name"))
    actual_rows = sorted(actual_df.collect(), key=lambda row: row.name)

    expected_df = largest_satellites_df.filter(col("Planet") == lit("Mars")).select(col("name"))
    expected_rows = sorted(expected_df.collect(), key=lambda row: row.name)

    assert actual_rows == expected_rows


def test_chained_orphans_planets_satellites(
    planets_df: DataFrame, largest_satellites_df: DataFrame
):
    """Test a basic chained orphan idenfitication from satellites to planets."""
    join = OrphanIdentification(
        entity_name="satellites",
        target_name="planets",
        join_condition="satellites.planet == planets.planet",
    )
    entities = EntityManager(
        {
            "planets": planets_df.filter(col("planet") != lit("Mars")),
            "satellites": largest_satellites_df,
        }
    )
    SPARK_STEP_BACKEND.evaluate(entities, config=join)
    entities["planets"] = planets_df.filter(col("planet") != lit("Earth"))
    SPARK_STEP_BACKEND.evaluate(entities, config=join)

    actual_df = entities["satellites"].filter(col("IsOrphaned")).select(col("name"))
    actual_rows = sorted(actual_df.collect(), key=lambda row: row.name)

    expected_df = largest_satellites_df.filter(
        col("Planet").isin(lit("Mars"), lit("Earth"))
    ).select(col("name"))
    expected_rows = sorted(expected_df.collect(), key=lambda row: row.name)

    assert actual_rows == expected_rows


def test_orphans_missing_entities_raises(planets_df: DataFrame, satellites_df: DataFrame):
    """Test that trying to join orphans from missing entities raises correctly."""
    join = OrphanIdentification(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
    )

    entities = EntityManager({"planets": planets_df})
    with pytest.raises(MissingEntity):
        SPARK_STEP_BACKEND.identify_orphans(entities, config=join)
    entities = EntityManager({"satellites": satellites_df})
    with pytest.raises(MissingEntity):
        SPARK_STEP_BACKEND.identify_orphans(entities, config=join)


def test_has_match_planets_satellites(planets_df: DataFrame, largest_satellites_df: DataFrame):
    """Test a 'has_match' join between planets to satellites."""
    join = ConfirmJoinHasMatch(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        column_name="planetHasSatellite",
        new_entity_name="planet_with_match_col",
    )
    entities = EntityManager({"planets": planets_df, "satellites": largest_satellites_df})
    SPARK_STEP_BACKEND.evaluate(entities, config=join)
    assert entities["planets"].count() == entities["planet_with_match_col"].count()

    actual_df = entities["planet_with_match_col"].select(col("planet"), col("planetHasSatellite"))
    actual = [(row.planet, row.planetHasSatellite) for row in actual_df.collect()]
    actual.sort(key=lambda row: row[0])

    all_planets: Set[str] = {str(row.planet) for row in planets_df.collect()}
    planets_with_satellite: Set[str] = {str(row.planet) for row in largest_satellites_df.collect()}
    expected: List[Tuple[str, bool]] = []
    for planet in sorted(all_planets):
        expected.append((planet, planet in planets_with_satellite))

    assert actual == expected


def test_table_union_planets_satellites(planets_df: DataFrame, satellites_df: DataFrame):
    """Test that we can union two tables."""
    planets_df = planets_df.select("planet", "mass", "escape_velocity")
    satellites_df = satellites_df.select(
        col("name").alias("satellite"), col("gm").alias("MASS"), col("magnitude")
    )

    union = TableUnion(
        entity_name="planets",
        target_name="satellites",
        new_entity_name="union_planets_satellites",
    )

    planets_entities = EntityManager({"planets": planets_df})
    with pytest.raises(MissingEntity):
        SPARK_STEP_BACKEND.union(planets_entities, config=union)

    satellites_entities = EntityManager({"satellites": satellites_df})
    with pytest.raises(MissingEntity):
        SPARK_STEP_BACKEND.union(satellites_entities, config=union)

    entities = EntityManager({"planets": planets_df, "satellites": satellites_df})
    SPARK_STEP_BACKEND.evaluate(entities, config=union)
    actual = entities.pop("union_planets_satellites")

    expected_planets = planets_df.select(
        col("planet"),
        col("mass"),
        col("escape_velocity"),
        lit(None).alias("satellite"),
        lit(None).alias("magnitude"),
    )
    expected_satellites = satellites_df.select(
        lit(None).alias("planet"),
        col("MASS").alias("mass"),
        lit(None).alias("escape_velocity"),
        col("satellite"),
        col("magnitude"),
    )
    expected = expected_planets.union(expected_satellites)

    assert actual.columns == expected.columns
    sort_key = (
        lambda row: row.planet or row.satellite  # pylint: disable=unnecessary-lambda-assignment
    )
    assert sorted(actual.collect(), key=sort_key) == sorted(expected.collect(), key=sort_key)


# TODO: Make this pass!
@pytest.mark.xfail(
    reason="Current implementation cannot handle refdata tables with multiple join condition matches"
)
def test_has_match_planets_satellites_multiple_matches(
    planets_df: DataFrame, satellites_df: DataFrame
):
    """
    Test a 'has_match' join between planets to satellites.

    This uses the full satellites DataFrame to ensure that no row duplication
    occurs.

    """
    join = ConfirmJoinHasMatch(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        column_name="planetHasSatellite",
        new_entity_name="planet_with_match_col",
    )
    entities = EntityManager({"planets": planets_df, "satellites": satellites_df})
    SPARK_STEP_BACKEND.evaluate(entities, config=join)
    assert entities["planets"].count() == entities["planet_with_match_col"].count()

    actual_df = entities["planet_with_match_col"].select(col("planet"), col("planetHasSatellite"))
    actual = [(row.planet, row.planetHasSatellite) for row in actual_df.collect()]
    actual.sort(key=lambda row: row[0])

    all_planets: Set[str] = {str(row.planet) for row in planets_df.collect()}
    planets_with_satellite: Set[str] = {str(row.planet) for row in satellites_df.collect()}
    expected: List[Tuple[str, bool]] = []
    for planet in sorted(all_planets):
        expected.append((planet, planet in planets_with_satellite))

    assert actual == expected


def test_read_and_write_simple_parquet(simple_typecast_parquet):
    parquet_uri, data = simple_typecast_parquet
    entity: DataFrame = SPARK_STEP_BACKEND.read_parquet(path=parquet_uri)
    assert entity.count() == 2
    assert entity.schema == StructType(
        [
            StructField("id", LongType()),
            StructField("datefield", DateType()),
            StructField("strfield", StringType()),
            StructField("datetimefield", TimestampType()),
        ]
    )
    config = ColumnAddition(entity_name="test_ent", column_name="times_ten", expression="id*10")
    entities = EntityManager(entities={"test_ent": entity})
    SPARK_STEP_BACKEND.evaluate(entities, config=config)
    target_loc: Path = Path(parquet_uri).parent.joinpath("ddb_step_backend_simple_output.parquet")
    SPARK_STEP_BACKEND.write_parquet(entities["test_ent"], target_location=target_loc.as_posix())
    check: DataFrame = SPARK_STEP_BACKEND.read_parquet(path=target_loc.as_posix())
    assert check.count() == 2
    assert check.schema == StructType(
        [
            StructField("id", LongType()),
            StructField("datefield", DateType()),
            StructField("strfield", StringType()),
            StructField("datetimefield", TimestampType()),
            StructField("times_ten", LongType()),
        ]
    )


def test_read_and_write_nested_parquet(nested_typecast_parquet):
    parquet_uri, data = nested_typecast_parquet
    entity: DataFrame = SPARK_STEP_BACKEND.read_parquet(path=parquet_uri)
    assert entity.count() == 2
    assert entity.schema == StructType(
        [
            StructField("id", LongType()),
            StructField("strfield", StringType()),
            StructField("datetimefield", TimestampType()),
            StructField(
                "subfield",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType()),
                            StructField("substrfield", StringType()),
                            StructField("subarrayfield", ArrayType(DateType())),
                        ]
                    )
                ),
            ),
        ]
    )
    config = ColumnRemoval(entity_name="test_ent", column_name="strfield")
    entities = EntityManager(entities={"test_ent": entity})
    SPARK_STEP_BACKEND.evaluate(entities, config=config)
    target_loc: Path = Path(parquet_uri).parent.joinpath("ddb_step_backend_nested_output.parquet")
    SPARK_STEP_BACKEND.write_parquet(entities["test_ent"], target_location=target_loc.as_posix())
    check: DataFrame = SPARK_STEP_BACKEND.read_parquet(path=target_loc.as_posix())
    assert check.count() == 2
    assert check.schema == StructType(
        [
            StructField("id", LongType()),
            StructField("datetimefield", TimestampType()),
            StructField(
                "subfield",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType()),
                            StructField("substrfield", StringType()),
                            StructField("subarrayfield", ArrayType(DateType())),
                        ]
                    )
                ),
            ),
        ]
    )
