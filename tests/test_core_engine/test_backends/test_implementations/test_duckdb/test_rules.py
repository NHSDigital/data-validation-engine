"""Test DuckDB backend steps."""

# pylint: disable=redefined-outer-name,unused-import,line-too-long
from pathlib import Path
from typing import Iterator, List, Optional, Set, Tuple, Type

import numpy as np
import polars as pl
import pytest
from duckdb import (
    ColumnExpression,
    ConstantExpression,
    DuckDBPyRelation,
    StarExpression,
    default_connection,
)

from dve.core_engine.backends.base.core import EntityManager
from dve.core_engine.backends.exceptions import MissingEntity
from dve.core_engine.backends.implementations.duckdb.rules import DuckDBStepImplementations
from dve.core_engine.backends.metadata.reporting import ReportingConfig
from dve.core_engine.backends.metadata.rules import (
    Aggregation,
    AntiJoin,
    ColumnAddition,
    ColumnRemoval,
    ConfirmJoinHasMatch,
    CopyEntity,
    EntityRemoval,
    HeaderJoin,
    ImmediateFilter,
    InnerJoin,
    LeftJoin,
    Notification,
    OneToOneJoin,
    OrphanIdentification,
    RenameEntity,
    SelectColumns,
    SemiJoin,
    TableUnion,
)
from dve.core_engine.type_hints import MultipleExpressions
from tests.test_core_engine.test_backends.fixtures import (
    duckdb_connection,
    largest_satellites_rel,
    nested_typecast_parquet,
    planets_rel,
    satellites_rel,
    simple_typecast_parquet,
)

DUCKDB_STEP_BACKEND = DuckDBStepImplementations(default_connection)
"""The backend for the duckdb steps."""


@pytest.fixture(scope="function")
def value_literal_1_header(duckdb_connection) -> Iterator[DuckDBPyRelation]:
    """
    A Relation with integer column 'Value' containing a single row with a
    literal value of 1.

    """
    yield duckdb_connection.sql("SELECT 1 AS Value")


def test_column_addition(planets_rel: DuckDBPyRelation):
    """Test that columns can be added to entities."""
    entities = EntityManager({"planets": planets_rel})
    rule = ColumnAddition(
        entity_name="planets",
        column_name="literal_one",
        expression=1,
        new_entity_name="added",
    )
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
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
    messages, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert not success, "Rule expected to fail"
    assert len(messages) == 1, "Expected single message"
    assert messages[0].error_message.startswith("Missing entity 'planets' required by")


def test_column_removal(planets_rel: DuckDBPyRelation):
    """Test that columns can be removed from entities."""
    entities = EntityManager({"planets": planets_rel})
    rule = ColumnRemoval(entity_name="planets", column_name="mass")
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert (set(planets_rel.columns) - set(entities["planets"].columns)) == {"mass"}


@pytest.mark.parametrize(
    "column_spec",
    [
        ["planet", "mass"],
        {"planet": "planet", "mass": "mass"},
        "planet, mass",
        "planet AS planet, mass AS mass",
    ],
)
def test_select_columns(planets_rel: DuckDBPyRelation, column_spec: MultipleExpressions):
    """Test that columns can be selected from columns."""
    rule = SelectColumns(entity_name="planets", columns=column_spec)
    entities = EntityManager({"planets": planets_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    planets_df = entities["planets"]
    assert planets_df.columns == ["planet", "mass"]


def test_select_columns_expressions(planets_rel: DuckDBPyRelation):
    """Test that columns can be selected from columns using expressions."""
    rule = SelectColumns(
        entity_name="planets",
        columns={"planet": "planet", "upper(planet)": "upper_planet"},
    )
    entities = EntityManager({"planets": planets_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    planets_rel = entities["planets"]
    assert all(
        row["planet"].upper() == row["upper_planet"]
        for row in planets_rel.df().to_dict(orient="records")
    )


def test_select_columns_expressions_from_refdata(planets_rel: DuckDBPyRelation):
    """Test that columns can be selected from columns using expressions."""
    rule = SelectColumns(
        entity_name="refdata_planets",
        columns={"planet": "planet", "upper(planet)": "upper_planet"},
        new_entity_name="planets",
    )
    entities = EntityManager({}, {"planets": planets_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    planets_rel = entities["planets"]
    assert all(
        row["planet"].upper() == row["upper_planet"]
        for row in planets_rel.df().to_dict(orient="records")
    )


def test_cannot_override_refdata(planets_rel: DuckDBPyRelation):
    """Test that reference data cannot be overridden."""
    rule = SelectColumns(
        entity_name="refdata_planets",
        columns={"planet": "planet", "upper(planet)": "upper_planet"},
    )
    entities = EntityManager({}, {"planets": planets_rel})

    messages, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert not success, "Rule expected to fail"
    assert len(messages) == 1, "Expected single message"
    assert messages[0].error_message.startswith("Constraint violated in ")
    assert messages[0].error_message.endswith(
        ": reference data entry 'planets' must not be mutated"
    )


# duckdb doesn't appear to support multialiasing (nor does it have native functionality
# to return multiple columns from a single function) so skipping this test


@pytest.mark.parametrize("group_by_spec", ["planet", ["planet"], {"planet": "planet"}])
@pytest.mark.parametrize(
    "agg_spec",
    ["max_by(name, gm) AS name", ["max_by(name, gm) AS name"], {"max_by(name, gm)": "name"}],
)
def test_aggregation(
    satellites_rel: DuckDBPyRelation,
    largest_satellites_rel: DuckDBPyRelation,
    group_by_spec: MultipleExpressions,
    agg_spec: MultipleExpressions,
):
    """Test that the aggregations work as expected."""
    rule = Aggregation(entity_name="satellites", group_by=group_by_spec, agg_columns=agg_spec)
    entities = EntityManager({"satellites": satellites_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    actual = entities["satellites"]

    actual_rows = sorted(actual.df().to_dict(orient="records"), key=lambda row: row["planet"])
    expected = largest_satellites_rel.select(*map(ColumnExpression, actual.columns))
    expected_rows = sorted(expected.df().to_dict(orient="records"), key=lambda row: row["planet"])
    assert actual_rows == expected_rows


@pytest.mark.parametrize("pivot_values", [None, ["Earth", "Mars"]])
def test_aggregation_pivot(planets_rel: DuckDBPyRelation, pivot_values: Optional[List[str]]):
    """Test that the aggregations work as expected with a pivot."""
    # Creating a dataset with a column for each planet, and a single row
    # containing the masses.
    rule = Aggregation(
        entity_name="planets",
        group_by={},
        pivot_column="planet",
        pivot_values=pivot_values,
        agg_columns=["mass"],
        agg_function="first",
    )
    entities = EntityManager({"planets": planets_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    actual = entities["planets"]
    actual_record = dict(zip(actual.columns, actual.fetchone()))

    all_rows = planets_rel.df().to_dict(orient="records")
    expected_record = {}
    for row in all_rows:
        planet, mass = row["planet"], row["mass"]
        if pivot_values and planet not in pivot_values:
            continue

        expected_record[planet] = mass

    assert actual_record == expected_record


def test_aggregation_raises_agg_function_but_no_agg_cols():
    """
    Ensure that an aggregation cannot be created with a pivot column and no
    values.

    """
    with pytest.raises(ValueError):
        Aggregation(
            entity_name="planets", group_by={}, pivot_values=["Earth", "Mars"], agg_function="first"
        )


def test_entity_removal(planets_rel: DuckDBPyRelation):
    """Test that entities can be removed."""
    rule = EntityRemoval(entity_name="planets")
    entities = EntityManager({"planets": planets_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert not entities

    assert rule.get_required_entities() == set()
    assert rule.get_created_entities() == set()
    assert rule.get_removed_entities() == {"planets"}


def test_rename_entity(planets_rel: DuckDBPyRelation):
    """Test that entities can be renamed."""
    rule = RenameEntity(entity_name="planets", new_entity_name="planets_renamed")
    entities = EntityManager({"planets": planets_rel})

    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert entities == {"planets_renamed": planets_rel}

    assert rule.get_required_entities() == {"planets"}
    assert rule.get_created_entities() == {"planets_renamed"}
    assert rule.get_removed_entities() == {"planets"}


def test_copy_entity(planets_rel: DuckDBPyRelation):
    """Test that entities can be copied."""
    rule = CopyEntity(entity_name="planets", new_entity_name="planets_renamed")
    entities = EntityManager({"planets": planets_rel})

    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=rule)
    assert success, "Rule application failed"
    assert entities == {"planets": planets_rel, "planets_renamed": planets_rel}

    assert rule.get_required_entities() == {"planets"}
    assert rule.get_created_entities() == {"planets_renamed"}
    assert rule.get_removed_entities() == set()


def test_missing_entity_removal():
    """Test that missing entities for removal succeds as expected."""
    rule = EntityRemoval(entity_name="planets")
    _, success = DUCKDB_STEP_BACKEND.evaluate({}, config=rule)
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
    planets_rel: DuckDBPyRelation,
    largest_satellites_rel: DuckDBPyRelation,
    new_columns_spec: MultipleExpressions,
    join_type: Type[LeftJoin],
):
    """Test a basic join from satellites to planets."""
    join = join_type(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet = satellites.planet",
        new_columns=new_columns_spec,
    )
    entities = EntityManager({"planets": planets_rel, "satellites": largest_satellites_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    assert success, "Rule application failed"
    actual_rel = entities["planets"].select(
        ColumnExpression("planet"), ColumnExpression("satellite")
    )
    actual_rows = sorted(actual_rel.df().to_dict(orient="records"), key=lambda row: row["planet"])

    expected_rel = (
        planets_rel.set_alias("planets")
        .join(
            largest_satellites_rel.set_alias("satellites"),
            condition="planets.planet = satellites.planet",
            how="inner" if join_type == InnerJoin else "left",
        )
        .select(
            ColumnExpression("planets.planet").alias("planet"),
            ColumnExpression("satellites.name").alias("satellite"),
        )
    )
    expected_rows = sorted(
        expected_rel.df().to_dict(orient="records"), key=lambda row: row["planet"]
    )

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
    planets_rel: DuckDBPyRelation,
    largest_satellites_rel: DuckDBPyRelation,
    new_columns_spec: MultipleExpressions,
    join_type: Type[LeftJoin],
):
    """Test a basic join from satellites to planets can overwrite an existing column."""
    # Set a column with a name we plan to overwrite so we can test overwriting it.
    planets_rel = planets_rel.select(
        StarExpression(), ConstantExpression("a satellite").alias("satellite")
    )

    join = join_type(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns=new_columns_spec,
    )
    entities = EntityManager({"planets": planets_rel, "satellites": largest_satellites_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    assert success, "Rule application failed"
    actual_rel = entities["planets"].select(
        ColumnExpression("planet"),
        ColumnExpression("satellite"),
        ColumnExpression("literal_one"),
    )
    actual_rows = sorted(actual_rel.df().to_dict(orient="records"), key=lambda row: row["planet"])

    expected_rel = (
        planets_rel.set_alias("planets")
        .join(
            largest_satellites_rel.set_alias("satellites"),
            condition="planets.planet = satellites.planet",
            how="inner" if join_type == InnerJoin else "left",
        )
        .select(
            ColumnExpression("planets.planet").alias("planet"),
            ColumnExpression("satellites.name").alias("satellite"),
            ConstantExpression(1).alias("literal_one"),
        )
    )
    expected_rows = sorted(
        expected_rel.df().to_dict(orient="records"), key=lambda row: row["planet"]
    )

    assert actual_rows == expected_rows


@pytest.mark.parametrize("join_type", [LeftJoin, OneToOneJoin, InnerJoin])
def test_join_can_take_all_cols(
    planets_rel: DuckDBPyRelation,
    largest_satellites_rel: DuckDBPyRelation,
    join_type: Type[LeftJoin],
):
    """Test a basic join from satellites to planets can overwrite an existing column."""
    # Set a column with a name we plan to overwrite so we can test overwriting it.
    planets_rel = planets_rel.select(
        StarExpression(), ConstantExpression("a satellite").alias("satellite_planet")
    )
    # Ensure we don't have any other column names the same in both by prefixing.
    largest_satellites_rel = largest_satellites_rel.select(
        *[
            ColumnExpression(column_name).alias(f"satellite_{column_name}")
            for column_name in largest_satellites_rel.columns
        ]
    )

    join = join_type(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.satellite_planet",
        new_columns="satellites.*",
    )
    entities = EntityManager({"planets": planets_rel, "satellites": largest_satellites_rel})
    _, success = DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    assert success, "Join application failed"
    actual_rel = entities["planets"]
    actual_rows = sorted(actual_rel.df().to_dict(orient="records"), key=lambda row: row["planet"])

    expected_rel = (
        planets_rel.select(StarExpression(exclude=["satellite_planet"]))
        .set_alias("planets")
        .join(
            largest_satellites_rel.set_alias("satellites"),
            condition="planets.planet = satellites.satellite_planet",
            how="inner" if join_type == InnerJoin else "left",
        )
    )
    expected_rows = sorted(
        expected_rel.df().to_dict(orient="records"), key=lambda row: row["planet"]
    )

    assert actual_rows == expected_rows


def test_one_to_one_join_multi_matches_raises(
    planets_rel: DuckDBPyRelation, satellites_rel: DuckDBPyRelation
):
    """Test that a join which results in multiple records being pulled through will fail."""
    join = OneToOneJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns={"satellites.name": "satellite"},
    )
    entities = EntityManager({"planets": planets_rel, "satellites": satellites_rel})
    with pytest.raises(ValueError, match="Multiple matches for some records.*"):
        DUCKDB_STEP_BACKEND.one_to_one_join(entities, config=join)


def test_left_join_multi_matches_does_not_raise(
    planets_rel: DuckDBPyRelation, satellites_rel: DuckDBPyRelation
):
    """Test that a join which results in multiple records being pulled through will succeed if it's not one to one."""
    join = LeftJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns={"satellites.name": "satellite"},
    )
    entities = EntityManager({"planets": planets_rel, "satellites": satellites_rel})
    DUCKDB_STEP_BACKEND.evaluate(entities, config=join)


def test_semi_join_works(planets_rel: DuckDBPyRelation, satellites_rel: DuckDBPyRelation):
    """Test that a join which results in multiple records being pulled through will fail."""
    join = SemiJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_entity_name="planets_with_satellite",
    )
    entities = EntityManager({"planets": planets_rel, "satellites": satellites_rel})
    DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    entity = entities["planets_with_satellite"]
    actual = sorted([row["planet"] for row in entity.df().to_dict(orient="records")])
    expected = ["Earth", "Jupiter", "Mars", "Neptune", "Pluto", "Saturn", "Uranus"]
    assert actual == expected
    assert dict(zip(entity.columns, entity.dtypes)) == dict(
        zip(planets_rel.columns, planets_rel.dtypes)
    )


def test_anti_join_works(planets_rel: DuckDBPyRelation, satellites_rel: DuckDBPyRelation):
    """Test that a join which results in multiple records being pulled through will fail."""
    join = AntiJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_entity_name="planets_without_satellite",
    )
    entities = EntityManager({"planets": planets_rel, "satellites": satellites_rel})
    DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    entity = entities["planets_without_satellite"]
    actual = sorted([row["planet"] for row in entity.df().to_dict(orient="records")])
    expected = ["Mercury", "Venus"]
    assert actual == expected
    assert dict(zip(entity.columns, entity.dtypes)) == dict(
        zip(planets_rel.columns, planets_rel.dtypes)
    )


def test_join_missing_entities_raises(
    planets_rel: DuckDBPyRelation, satellites_rel: DuckDBPyRelation
):
    """Test that trying to join missing entities raises correctly."""
    join = OneToOneJoin(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        new_columns={"satellites.name": "satellite"},
    )

    entities = EntityManager({"planets": planets_rel})
    with pytest.raises(MissingEntity):
        DUCKDB_STEP_BACKEND.one_to_one_join(entities, config=join)
    entities = EntityManager({"satellites": satellites_rel})
    with pytest.raises(MissingEntity):
        DUCKDB_STEP_BACKEND.one_to_one_join(entities, config=join)


def test_header_join_planets(
    planets_rel: DuckDBPyRelation, value_literal_1_header: DuckDBPyRelation
):
    """Test adding a header to the planets dataset."""
    header_join = HeaderJoin(
        entity_name="planets",
        target_name="header",
        new_entity_name="planets_header",
        header_column_name="_Header",
    )
    entities = EntityManager({"planets": planets_rel, "header": value_literal_1_header})
    # currently failing impl - investigate tomorrow
    DUCKDB_STEP_BACKEND.evaluate(entities, config=header_join)

    header_value = value_literal_1_header.df().to_dict(orient="records")[0]
    planet_rows = sorted(
        entities["planets"].df().to_dict(orient="records"), key=lambda row: row["planet"]
    )
    actual_rows = sorted(
        entities["planets_header"].df().to_dict(orient="records"), key=lambda row: row["planet"]
    )

    for planet_row, actual_row in zip(planet_rows, actual_rows):
        assert actual_row.pop("_Header") == header_value, "Header does not match expected header"
        assert planet_row == actual_row, "More than header changed as result of header join"


def test_header_multi_rows_raises(
    planets_rel: DuckDBPyRelation, value_literal_1_header: DuckDBPyRelation
):
    """Ensure that adding a 'header' with multiple rows to the planets dataset raises an error."""
    header_join = HeaderJoin(
        entity_name="planets",
        target_name="header",
        new_entity_name="planets_header",
        header_column_name="_Header",
    )
    entities = EntityManager(
        {
            "planets": planets_rel,
            "header": value_literal_1_header.union(value_literal_1_header),
        }
    )
    with pytest.raises(
        ValueError, match="Unable to join header '.+' to '.+' as it contains multiple entries"
    ):
        DUCKDB_STEP_BACKEND.join_header(entities, config=header_join)


def test_orphans_planets_satellites(
    planets_rel: DuckDBPyRelation, largest_satellites_rel: DuckDBPyRelation
):
    """Test a basic orphan idenfitication from satellites to planets."""
    # Each satellite _must_ have a planet.
    join = OrphanIdentification(
        entity_name="satellites",
        target_name="planets",
        join_condition="satellites.planet == planets.planet",
    )
    entities = EntityManager(
        {
            "planets": planets_rel.filter(ColumnExpression("Planet") != ConstantExpression("Mars")),
            "satellites": largest_satellites_rel,
        }
    )

    DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    actual_rel = (
        entities["satellites"]
        .filter(ColumnExpression("IsOrphaned"))
        .select(ColumnExpression("name"))
    )
    actual_rows = sorted(actual_rel.df().to_dict(orient="records"), key=lambda row: row["name"])

    expected_rel = largest_satellites_rel.filter(
        ColumnExpression("Planet") == ConstantExpression("Mars")
    ).select(ColumnExpression("name"))
    expected_rows = sorted(expected_rel.df().to_dict(orient="records"), key=lambda row: row["name"])

    assert actual_rows == expected_rows


def test_chained_orphans_planets_satellites(
    planets_rel: DuckDBPyRelation, largest_satellites_rel: DuckDBPyRelation
):
    """Test a basic chained orphan idenfitication from satellites to planets."""
    join = OrphanIdentification(
        entity_name="satellites",
        target_name="planets",
        join_condition="satellites.planet == planets.planet",
    )
    entities = EntityManager(
        {
            "planets": planets_rel.filter(ColumnExpression("planet") != ConstantExpression("Mars")),
            "satellites": largest_satellites_rel,
        }
    )
    DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    entities["planets"] = planets_rel.filter(
        ColumnExpression("planet") != ConstantExpression("Earth")
    )
    DUCKDB_STEP_BACKEND.evaluate(entities, config=join)

    actual_rel = (
        entities["satellites"]
        .filter(ColumnExpression("IsOrphaned"))
        .select(ColumnExpression("name"))
    )
    actual_rows = sorted(actual_rel.df().to_dict(orient="records"), key=lambda row: row["name"])

    expected_rel = largest_satellites_rel.filter(
        ColumnExpression("Planet").isin(ConstantExpression("Mars"), ConstantExpression("Earth"))
    ).select(ColumnExpression("name"))
    expected_rows = sorted(expected_rel.df().to_dict(orient="records"), key=lambda row: row["name"])

    assert actual_rows == expected_rows


def test_orphans_missing_entities_raises(
    planets_rel: DuckDBPyRelation, satellites_rel: DuckDBPyRelation
):
    """Test that trying to join orphans from missing entities raises correctly."""
    join = OrphanIdentification(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
    )

    entities = EntityManager({"planets": planets_rel})
    with pytest.raises(MissingEntity):
        DUCKDB_STEP_BACKEND.identify_orphans(entities, config=join)
    entities = EntityManager({"satellites": satellites_rel})
    with pytest.raises(MissingEntity):
        DUCKDB_STEP_BACKEND.identify_orphans(entities, config=join)


def test_has_match_planets_satellites(
    planets_rel: DuckDBPyRelation, largest_satellites_rel: DuckDBPyRelation
):
    """Test a 'has_match' join between planets to satellites."""
    join = ConfirmJoinHasMatch(
        entity_name="planets",
        target_name="satellites",
        join_condition="planets.planet == satellites.planet",
        column_name="planetHasSatellite",
        new_entity_name="planet_with_match_col",
    )
    entities = EntityManager({"planets": planets_rel, "satellites": largest_satellites_rel})
    DUCKDB_STEP_BACKEND.evaluate(entities, config=join)
    assert (
        entities["planets"].count("*").fetchone()[0]
        == entities["planet_with_match_col"].count("*").fetchone()[0]
    )

    actual_rel = entities["planet_with_match_col"].select(
        ColumnExpression("planet"), ColumnExpression("planetHasSatellite")
    )
    actual = [
        (row["planet"], row["planetHasSatellite"])
        for row in actual_rel.df().to_dict(orient="records")
    ]
    actual.sort(key=lambda row: row[0])

    all_planets: Set[str] = {
        str(row["planet"]) for row in planets_rel.df().to_dict(orient="records")
    }
    planets_with_satellite: Set[str] = {
        str(row["planet"]) for row in largest_satellites_rel.df().to_dict(orient="records")
    }
    expected: List[Tuple[str, bool]] = []
    for planet in sorted(all_planets):
        expected.append((planet, planet in planets_with_satellite))

    assert actual == expected


def test_table_union_planets_satellites(
    planets_rel: DuckDBPyRelation, satellites_rel: DuckDBPyRelation
):
    """Test that we can union two tables."""
    planets_rel = planets_rel.select("planet", "mass", "escape_velocity")
    satellites_rel = satellites_rel.select(
        ColumnExpression("name").alias("satellite"),
        ColumnExpression("gm").alias("MASS"),
        ColumnExpression("magnitude"),
    )

    union = TableUnion(
        entity_name="planets",
        target_name="satellites",
        new_entity_name="union_planets_satellites",
    )

    planets_entities = EntityManager({"planets": planets_rel})
    with pytest.raises(MissingEntity):
        DUCKDB_STEP_BACKEND.union(planets_entities, config=union)

    satellites_entities = EntityManager({"satellites": satellites_rel})
    with pytest.raises(MissingEntity):
        DUCKDB_STEP_BACKEND.union(satellites_entities, config=union)

    entities = EntityManager({"planets": planets_rel, "satellites": satellites_rel})
    DUCKDB_STEP_BACKEND.evaluate(entities, config=union)
    actual = entities.pop("union_planets_satellites")

    expected_planets = planets_rel.select(
        ColumnExpression("planet"),
        ColumnExpression("mass"),
        ColumnExpression("escape_velocity"),
        ConstantExpression(None).alias("satellite"),
        ConstantExpression(None).alias("magnitude"),
    )
    expected_satellites = satellites_rel.select(
        ConstantExpression(None).alias("planet"),
        ColumnExpression("MASS").alias("mass"),
        ConstantExpression(None).alias("escape_velocity"),
        ColumnExpression("satellite"),
        ColumnExpression("magnitude"),
    )
    expected = expected_planets.union(expected_satellites)

    assert actual.columns == expected.columns
    sort_key = (
        lambda row: row["planet"]
        or row["satellite"]  # pylint: disable=unnecessary-lambda-assignment
    )
    actual_vals = sorted(
        actual.df().replace({np.nan: None}).to_dict(orient="records"), key=sort_key
    )
    expected_vals = sorted(
        expected.df().replace({np.nan: None}).to_dict(orient="records"), key=sort_key
    )
    assert actual_vals == expected_vals


def test_planet_non_notify_filter(planets_rel: DuckDBPyRelation):
    config = ImmediateFilter(
        entity_name="planets",
        new_entity_name="planets_filtered",
        expression="planet in ('Earth', 'Mars')",
    )
    entities = EntityManager({"planets": planets_rel})

    DUCKDB_STEP_BACKEND.evaluate(entities, config=config)

    filtered_entity: DuckDBPyRelation = entities["planets_filtered"]

    assert filtered_entity.count("*").fetchone()[0] == 2
    assert sorted(filtered_entity.planet.fetchall()) == [("Earth",), ("Mars",)]


def test_planets_notify(planets_rel: DuckDBPyRelation):
    config = Notification(
        entity_name="planets",
        expression="has_ring_system == 'Yes'",
        excluded_columns=["mass", "diameter"],
        reporting=ReportingConfig(
            code="TEST", message="this is a test", location="planet, has_ring_system"
        ),
    )
    entities = EntityManager({"planets": planets_rel})
    messages = DUCKDB_STEP_BACKEND.evaluate(entities, config=config)

    assert len(messages[0]) == 4


def test_read_and_write_simple_parquet(simple_typecast_parquet):
    parquet_uri, data = simple_typecast_parquet
    entity: DuckDBPyRelation = DUCKDB_STEP_BACKEND.read_parquet(path=parquet_uri)
    assert entity.count("*").fetchone()[0] == 2
    assert dict(zip(entity.columns, entity.dtypes)) == {
        "id": "BIGINT",
        "datefield": "DATE",
        "strfield": "VARCHAR",
        "datetimefield": "TIMESTAMP",
    }
    config = ColumnAddition(entity_name="test_ent", column_name="times_ten", expression="id*10")
    entities = EntityManager(entities={"test_ent": entity})
    DUCKDB_STEP_BACKEND.evaluate(entities, config=config)
    target_loc: Path = Path(parquet_uri).parent.joinpath("ddb_step_backend_simple_output.parquet")
    DUCKDB_STEP_BACKEND.write_parquet(entities["test_ent"], target_location=target_loc.as_posix())
    check: DuckDBPyRelation = DUCKDB_STEP_BACKEND.read_parquet(path=target_loc.as_posix())
    assert check.count("*").fetchone()[0] == 2
    assert dict(zip(check.columns, check.dtypes)) == {
        "id": "BIGINT",
        "datefield": "DATE",
        "strfield": "VARCHAR",
        "datetimefield": "TIMESTAMP",
        "times_ten": "BIGINT",
    }


def test_read_and_write_nested_parquet(nested_typecast_parquet):
    parquet_uri, data = nested_typecast_parquet
    entity: DuckDBPyRelation = DUCKDB_STEP_BACKEND.read_parquet(path=parquet_uri)
    assert entity.count("*").fetchone()[0] == 2
    assert dict(zip(entity.columns, entity.dtypes)) == {
        "id": "BIGINT",
        "strfield": "VARCHAR",
        "datetimefield": "TIMESTAMP",
        "subfield": "STRUCT(id BIGINT, substrfield VARCHAR, subarrayfield DATE[])[]",
    }
    config = ColumnRemoval(entity_name="test_ent", column_name="strfield")
    entities = EntityManager(entities={"test_ent": entity})
    DUCKDB_STEP_BACKEND.evaluate(entities, config=config)
    target_loc: Path = Path(parquet_uri).parent.joinpath("ddb_step_backend_nested_output.parquet")
    DUCKDB_STEP_BACKEND.write_parquet(entities["test_ent"], target_location=target_loc.as_posix())
    check: DuckDBPyRelation = DUCKDB_STEP_BACKEND.read_parquet(path=target_loc.as_posix())
    assert check.count("*").fetchone()[0] == 2
    assert dict(zip(check.columns, check.dtypes)) == {
        "id": "BIGINT",
        "datetimefield": "TIMESTAMP",
        "subfield": "STRUCT(id BIGINT, substrfield VARCHAR, subarrayfield DATE[])[]",
    }
