"""Business rule definitions for duckdb backend"""

from collections.abc import Callable
from typing import get_type_hints
from uuid import uuid4

from duckdb import (
    ColumnExpression,
    ConstantExpression,
    DuckDBPyConnection,
    DuckDBPyRelation,
    StarExpression,
)
from duckdb.typing import DuckDBPyType

from dve.core_engine.backends.base.rules import (
    BaseStepImplementations,
    ColumnAddition,
    ColumnRemoval,
    SelectColumns,
)
from dve.core_engine.backends.exceptions import ConstraintError
from dve.core_engine.backends.implementations.duckdb.duckdb_helpers import (
    DDBStruct,
    duckdb_read_parquet,
    duckdb_rel_to_dictionaries,
    duckdb_write_parquet,
    get_all_registered_udfs,
    get_duckdb_type_from_annotation,
)
from dve.core_engine.backends.implementations.duckdb.types import (
    DuckDBEntities,
    Joined,
    Source,
    Target,
)
from dve.core_engine.backends.implementations.duckdb.utilities import parse_multiple_expressions
from dve.core_engine.backends.metadata.rules import (
    AbstractConditionalJoin,
    AbstractNewColumnConditionalJoin,
    Aggregation,
    AntiJoin,
    ConfirmJoinHasMatch,
    HeaderJoin,
    ImmediateFilter,
    InnerJoin,
    LeftJoin,
    Notification,
    OneToOneJoin,
    OrphanIdentification,
    SemiJoin,
    TableUnion,
)
from dve.core_engine.constants import ROWID_COLUMN_NAME
from dve.core_engine.functions import implementations as functions
from dve.core_engine.message import FeedbackMessage
from dve.core_engine.templating import template_object
from dve.core_engine.type_hints import Messages


@duckdb_write_parquet
@duckdb_read_parquet
class DuckDBStepImplementations(BaseStepImplementations[DuckDBPyRelation]):
    """An implementation of transformation steps in duckdb."""

    def __init__(self, connection: DuckDBPyConnection, **kwargs):
        self._connection = connection
        self.registered_functions = get_all_registered_udfs(self._connection)
        super().__init__(**kwargs)

    @property
    def connection(self) -> DuckDBPyConnection:
        """The duckdb connection"""
        return self._connection

    @classmethod
    def register_udfs(  # type: ignore
        cls, connection: DuckDBPyConnection, **kwargs
    ):  # pylint: disable=arguments-differ
        """Method to register all custom dve functions for use during business rules application"""
        _registered_functions: set[str] = get_all_registered_udfs(connection)
        _available_functions: dict[str, Callable] = {
            func_name: func
            for func_name, func in vars(functions).items()
            if callable(func) and func.__module__ == "dve.core_engine.functions.implementations"
        }

        _unregistered_functions: set[str] = set(_available_functions).difference(
            _registered_functions
        )

        for function_name in _unregistered_functions:
            _func = _available_functions.get(function_name)
            _annotations = get_type_hints(_func)
            _return_type: DuckDBPyType = get_duckdb_type_from_annotation(_annotations["return"])

            connection.create_function(
                function_name,
                _func,  # type: ignore
                return_type=str(_return_type),  # type: ignore
            )
        if _unregistered_functions:
            _sql = "INSERT INTO dve_udfs (function_name) VALUES" + ",".join(
                [f"('{f_name}',)" for f_name in _unregistered_functions]
            )  # pylint: disable=line-too-long
            connection.sql(_sql)
        return cls(connection=connection, **kwargs)

    @staticmethod
    def add_row_id(entity: DuckDBPyRelation) -> DuckDBPyRelation:
        """Adds a row identifier to the Relation"""
        if ROWID_COLUMN_NAME not in entity.columns:
            entity = entity.project(f"*, ROW_NUMBER() OVER () as {ROWID_COLUMN_NAME}")
        return entity

    @staticmethod
    def drop_row_id(entity: DuckDBPyRelation) -> DuckDBPyRelation:
        """Drops the row identiifer from a Relation"""
        if ROWID_COLUMN_NAME in entity.columns:
            entity = entity.select(StarExpression(exclude=[ROWID_COLUMN_NAME]))
        return entity

    def add(self, entities: DuckDBEntities, *, config: ColumnAddition) -> Messages:
        """A transformation step which adds a column to an entity."""
        entity: DuckDBPyRelation = entities[config.entity_name]
        entity = entity.select(f"*, {config.expression} as {config.column_name}")
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def remove(self, entities: DuckDBEntities, *, config: ColumnRemoval) -> Messages:
        """A transformation step which removes a column from an entity."""
        entity: DuckDBPyRelation = entities[config.entity_name]
        entity = entity.select(StarExpression(exclude=[config.column_name]))
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def select(self, entities: DuckDBEntities, *, config: SelectColumns) -> Messages:
        """A transformation step which selects columns from an entity."""
        entity: DuckDBPyRelation = entities[config.entity_name]
        entity = entity.select(", ".join(parse_multiple_expressions(config.columns)))
        entities[config.new_entity_name or config.entity_name] = entity
        if config.distinct:
            entity = entity.distinct()
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def group_by(self, entities: DuckDBEntities, *, config: Aggregation) -> Messages:
        """A transformation step which performs an aggregation on an entity."""

        def _add_cnst_field(rel: DuckDBPyRelation) -> tuple[str, DuckDBPyRelation]:
            """Add a constant field for use as an index to allow for pivoting with no group"""
            fld_name = f"fld_{uuid4().hex[0:8]}"
            return fld_name, rel.select(
                StarExpression(exclude=[]), ConstantExpression(1).alias(fld_name)
            )

        entity: DuckDBPyRelation = entities[config.entity_name]

        group_cols = parse_multiple_expressions(config.group_by)
        agg_cols = parse_multiple_expressions(config.agg_columns)

        if config.pivot_column:
            if not group_cols:
                const_fld, entity = _add_cnst_field(entity)
            group_pl = entity.pl().pivot(
                columns=[config.pivot_column],
                values=agg_cols,
                index=(group_cols or [const_fld]),
                aggregate_function=config.agg_function,
            )
            if const_fld in group_pl.columns:
                group_pl = group_pl.drop(const_fld)
            if config.pivot_values:
                group_pl = group_pl.select(group_cols + config.pivot_values)
            group = self._connection.sql("select * from group_pl")
        else:
            group = entity.aggregate(", ".join(group_cols + agg_cols))

        entities[config.new_entity_name or config.entity_name] = group
        return []

    def _resolve_join_name_conflicts(
        self, source_rel: Source, joined_rel: Joined, config: AbstractNewColumnConditionalJoin
    ) -> Joined:
        """Resolve name conflicts in joined DataFrames."""
        # Need to ensure we keep source columns but these can be overridden by
        # new computed keys.
        # Start with new computed names (in ddb - earlier columns take preference)
        columns = parse_multiple_expressions(config.new_columns)
        columns.extend(
            parse_multiple_expressions(
                [f"{config.entity_name}.{column_name}" for column_name in source_rel.columns]
            )
        )
        # Select them from the join. There may be duplicates here for overridden fields.
        result_all_cols = joined_rel.select(", ".join(columns))

        # Now need to handle the existence of dupes.
        # we keep any specified in config.new_columns as duckdb can only access most left-hand
        # field where name clashes.
        # Need to be careful with case sensitivity - duckdb (by default) ignores case.
        temp_column_names = []
        concrete_to_temp_mapping = {}
        case_mapping = {}
        for index, column_name in enumerate(result_all_cols.columns):
            temp_name = str(index)

            # Duckdb case insensitive by default
            # iterate through column names and keep only earliest casing
            uppercase_name = column_name.upper()
            if uppercase_name not in case_mapping:
                case_mapping[uppercase_name] = column_name
                concrete_to_temp_mapping[uppercase_name] = temp_name

            # Store the temp name, and the mapping between uppercase name and temp name.
            temp_column_names.append(temp_name)

        # Rename with the indices, so we can deduplicate column names.
        result_temp_names = result_all_cols.select(
            *[
                ColumnExpression(column_name).alias(str(index))
                for index, column_name in enumerate(result_all_cols.columns)
            ]
        )
        # Keep only the earliest column for each (case insensitive) column name.
        earliest_temp_names = result_temp_names.select(
            *[ColumnExpression(temp_name) for temp_name in concrete_to_temp_mapping.values()]
        )
        # Rename those fields to their 'proper' names (respect user-supplied case).
        return earliest_temp_names.select(
            *[
                ColumnExpression(temp_name).alias(case_mapping[upp_name])
                for upp_name, temp_name in concrete_to_temp_mapping.items()
            ]
        )

    def _perform_join(
        self, entities: DuckDBEntities, config: AbstractConditionalJoin
    ) -> tuple[Source, Target, Joined]:
        """Perform a conditional join between source and target, returning the
        source, target and joined DataFrames.

        """
        source_rel: DuckDBPyRelation = entities[config.entity_name]
        source_rel = source_rel.set_alias(config.entity_name)
        target_rel: DuckDBPyRelation = entities[config.target_name]
        target_rel = target_rel.set_alias(config.target_name)

        if isinstance(config, InnerJoin):
            join_type = "inner"
        elif isinstance(config, SemiJoin):
            join_type = "semi"
        elif isinstance(config, AntiJoin):
            join_type = "anti"
        else:
            join_type = "left"

        joined_rel = source_rel.join(target_rel, condition=config.join_condition, how=join_type)

        return source_rel, target_rel, joined_rel

    def has_match(self, entities: DuckDBEntities, *, config: ConfirmJoinHasMatch) -> Messages:
        """Add a boolean column to a source entity, indicating whether it matches
        a target for the given condition.
        """
        source_rel, _, joined_rel = self._perform_join(entities, config)
        entity = joined_rel.select(
            f"*, COALESCE({config.join_condition}, FALSE) AS {config.column_name}"
        )

        if config.perform_integrity_check:
            joined_count = joined_rel.count("*").fetchone()[0]  # type: ignore
            source_count = source_rel.count("*").fetchone()[0]  # type: ignore
            if joined_count != source_count:
                raise ConstraintError(
                    f"Multiple matches for some records from {config.entity_name!r} for "
                    + f"condition {config.join_condition!r}",
                    constraint=(
                        f"records in source entity ({config.entity_name!r}) must match at most "
                        + f"a single record in the target ({config.target_name})"
                    ),
                )

        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def left_join(self, entities: DuckDBEntities, *, config: LeftJoin) -> Messages:
        """Perform a left join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """
        source_rel, _, joined_rel = self._perform_join(entities, config)

        entities[config.new_entity_name or config.entity_name] = self._resolve_join_name_conflicts(
            source_rel, joined_rel, config
        )
        return []

    def inner_join(self, entities: DuckDBEntities, *, config: InnerJoin) -> Messages:
        """Perform an inner join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """
        source_rel, _, joined_rel = self._perform_join(entities, config)
        entities[config.new_entity_name or config.entity_name] = self._resolve_join_name_conflicts(
            source_rel, joined_rel, config
        )

        return []

    def one_to_one_join(self, entities: DuckDBEntities, *, config: OneToOneJoin) -> Messages:
        """Perform a join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        This will be a left join that enforces a one-to-one relationship.

        """
        source_rel: DuckDBPyRelation = entities[config.entity_name]
        messages = self.left_join(entities, config=config)
        joined_rel: DuckDBPyRelation = entities[config.new_entity_name or config.entity_name]

        if config.perform_integrity_check:
            if (
                joined_rel.count("*").fetchone()[0] != source_rel.count("*").fetchone()[0]  # type: ignore # pylint: disable=line-too-long
            ):
                raise ConstraintError(
                    f"Multiple matches for some records from {config.entity_name!r} for "
                    + f"condition {config.join_condition!r}",
                    constraint=(
                        f"records in source entity ({config.entity_name!r}) must match at most "
                        + f"a single record in the target ({config.target_name})"
                    ),
                )
        return messages

    def semi_join(self, entities: DuckDBEntities, *, config: SemiJoin) -> Messages:
        """Perform a semi join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """
        _, _, joined_rel = self._perform_join(entities, config)

        entities[config.new_entity_name or config.entity_name] = joined_rel
        return []

    def anti_join(self, entities: DuckDBEntities, *, config: AntiJoin) -> Messages:
        """Perform an anti join from a source entity to a target table, updating
        the source entity or creating a new joined entity.

        """
        _, _, joined_rel = self._perform_join(entities, config)

        entities[config.new_entity_name or config.entity_name] = joined_rel
        return []

    def join_header(self, entities: DuckDBEntities, *, config: HeaderJoin) -> Messages:
        """Add a 'header' entity to each row in the source entity. The header entity
        must contain only a single record.

        """
        source_rel: DuckDBPyRelation = entities[config.entity_name]
        source_rel = source_rel.set_alias(config.entity_name)
        target_rel: DuckDBPyRelation = entities[config.target_name]
        target_rel = target_rel.set_alias(config.target_name)

        target_rows = target_rel.pl().to_struct("header").to_list()
        n_target_rows = len(target_rows)
        if n_target_rows != 1:
            raise ConstraintError(
                f"Unable to join header {config.target_name!r} to {config.entity_name!r} "
                + f"as it contains multiple entries (expected 1, got {n_target_rows})",
                constraint=(
                    f"Header entity {config.target_name!r} must contain a single record "
                    + f"(contains {n_target_rows} records)"
                ),
            )

        target_schema = DDBStruct(dict(zip(target_rel.columns, target_rel.dtypes)))()

        joined_rel = source_rel.select(
            StarExpression(exclude=[]),
            ConstantExpression(target_rows[0]).cast(target_schema).alias(config.header_column_name),
        )

        entities[config.new_entity_name or config.entity_name] = joined_rel
        return []

    def identify_orphans(
        self, entities: DuckDBEntities, *, config: OrphanIdentification
    ) -> Messages:
        """Identify records in an entity which don't have at least one corresponding
        match in the target. A new boolean column will be added to `entity` ('IsOrphaned')
        indicating whether the condition matched.

        If there is already an 'IsOrphaned' column in the entity, this will be set to the
        logical OR of its current value and the value it would have been set to otherwise.

        """
        source_rel: DuckDBPyRelation = entities[config.entity_name]
        source_rel = source_rel.set_alias(config.entity_name)
        target_rel: DuckDBPyRelation = entities[config.target_name]
        target_rel = target_rel.set_alias(config.target_name)

        key_name = f"key_{uuid4().hex}"
        source_rel = source_rel.select(f"*, row_number() over () as {key_name}").set_alias(
            config.entity_name
        )
        match_name = f"matched_{uuid4().hex}"
        target_rel = target_rel.select(
            StarExpression(exclude=[]), ConstantExpression(1).alias(match_name)
        ).set_alias(config.target_name)

        joined_rel: DuckDBPyRelation = source_rel.join(
            target_rel, condition=config.join_condition, how="left"
        ).aggregate(f"{key_name}, coalesce(count({match_name})==0, TRUE) AS IsOrphaned")

        if "IsOrphaned" not in source_rel.columns:
            result: DuckDBPyRelation = source_rel.join(
                joined_rel, condition=key_name, how="left"
            ).select(StarExpression(exclude=[key_name]))
        else:
            result = source_rel.set_alias("source").join(
                joined_rel.set_alias("joined"),
                condition=f"source.{key_name} = joined.{key_name}",
                how="left",
            )

            columns = {name: f"source.{name}" for name in source_rel.columns}
            if "IsOrphaned" in source_rel.columns:
                columns["IsOrphaned"] = ColumnExpression("source.IsOrphaned") | ColumnExpression("joined.IsOrphaned")  # type: ignore # pylint: disable=line-too-long
            columns.pop(key_name, None)

            result = result.select(
                ",".join([f"{column} as {name}" for name, column in columns.items()])
            )

        entities[config.new_entity_name or config.entity_name] = result
        return []

    def union(self, entities: DuckDBEntities, *, config: TableUnion) -> Messages:
        """Union two entities together, taking the columns from each by name.

        Where columns have the same name, they must be the same type or coerceable.
        Where column casing differs, the casing from the `source` entity will be kept.

        Column order will be preserved, with columns from `source` taken first and extra
        columns in `target` added in order afterwards.

        """
        source_rel: DuckDBPyRelation = entities[config.entity_name]
        source_rel = source_rel.set_alias(config.entity_name)
        target_rel: DuckDBPyRelation = entities[config.target_name]
        target_rel = target_rel.set_alias(config.target_name)

        # Ensure all keys are present in both
        source_names = {column_name.upper(): column_name for column_name in source_rel.columns}
        target_names = {column_name.upper(): column_name for column_name in target_rel.columns}

        all_names = list(source_names.keys())
        for name in target_names:
            if name not in source_names:
                all_names.append(name)

        source_columns, target_columns = [], []
        for uppercase_name in all_names:
            source_name = source_names.get(uppercase_name)
            target_name = target_names.get(uppercase_name)

            if source_name and target_name:
                source_col = ColumnExpression(source_name)
                target_col = ColumnExpression(target_name).alias(source_name)
            elif source_name:
                source_col = ColumnExpression(source_name)
                target_col = ConstantExpression(None).alias(source_name)
            elif target_name:
                source_col = ConstantExpression(None).alias(target_name)
                target_col = ColumnExpression(target_name)
            else:
                continue

            source_columns.append(source_col)
            target_columns.append(target_col)

        source_rel = source_rel.select(*source_columns)
        target_rel = target_rel.select(*target_columns)
        entities[config.new_entity_name or config.entity_name] = source_rel.union(target_rel)
        return []

    def filter(self, entities: DuckDBEntities, *, config: ImmediateFilter) -> Messages:
        """Filter an entity immediately, and do not emit any messages.

        The synchronised filter stage will be implemented separately.

        """
        entity = entities[config.entity_name]
        entity = entity.filter(config.expression)
        entities[config.new_entity_name or config.entity_name] = entity
        return []

    def notify(self, entities: DuckDBEntities, *, config: Notification) -> Messages:
        """Emit a notification based on an expression. Where the expression is truthy,
        a nofication should be emitted according to the reporting config.

        This is not intended to be used directly, but is used in the implementation of
        the sync filters.

        """
        messages: Messages = []
        entity = entities[config.entity_name]

        matched = entity.filter(config.expression)
        if config.excluded_columns:
            matched = matched.select(StarExpression(exclude=config.excluded_columns))

        for chunk in duckdb_rel_to_dictionaries(matched):
            for record in chunk:
                # NOTE: only templates using values directly accessible in record - nothing nested
                # more complex extraction done in reporting module
                messages.append(
                    FeedbackMessage(
                        entity=config.reporting.reporting_entity_override or config.entity_name,
                        original_entity=config.entity_name,
                        record=record,  # type: ignore
                        error_location=config.reporting.legacy_location,
                        error_message=template_object(config.reporting.message,
                                                      record), # type: ignore
                        failure_type=config.reporting.legacy_error_type,
                        error_type=config.reporting.legacy_error_type,
                        error_code=config.reporting.code,
                        reporting_field=config.reporting.legacy_reporting_field,
                        reporting_field_name=config.reporting.reporting_field_override,
                        is_informational=config.reporting.emit in ("warning", "info"),
                        category=config.reporting.category,
                    )
                )
        return messages
