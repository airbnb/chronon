#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import json
import logging
import os
import typing
from collections import defaultdict
from functools import reduce
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ai.chronon.api import ttypes
from ai.chronon.group_by import get_output_col_names
from ai.chronon.lineage.lineage_metadata import (
    Config,
    ConfigType,
    LineageMetaData,
    TableType,
)
from ai.chronon.repo.validator import (
    extract_json_confs,
    get_group_by_output_columns,
    get_join_output_columns,
    get_pre_derived_external_features,
    get_pre_derived_group_by_columns,
    get_pre_derived_group_by_features,
    get_pre_derived_join_features,
    get_pre_derived_join_internal_features,
    get_pre_derived_source_keys,
    is_identifier,
)
from ai.chronon.utils import FeatureDisplayKeys, output_table_name, sanitize
from sqlglot import expressions as exp
from sqlglot import maybe_parse
from sqlglot.lineage import Node, to_node
from sqlglot.optimizer import build_scope, normalize_identifiers, qualify

logger = logging.getLogger(__name__)


class LineageParser:
    def __init__(self, schema_provider: Optional[Callable[[str], Dict[str, str]]] = None) -> None:
        """
        Take configurations and generate the following:
         1. Column-level lineage details.
         2. Output tables along with their associated columns.
         3. Feature with corresponding column mappings.

        :param schema_provider: A function that returns a schema dict given a table name.
        """

        self.base_path: Optional[str] = None
        self.team_conf: Optional[Dict[str, Any]] = None
        self.staging_queries: Dict[str, Any] = {}
        self.metadata: LineageMetaData = LineageMetaData()
        self.parsed_staging_query_tables = set()
        self.schema_provider = schema_provider

    def parse_lineage(self, base_path: str, config_filter: Optional[Set[str]] = None) -> LineageMetaData:
        """
        Parse lineage information for staging queries, group bys, and joins using the provided base path.

        :param base_path: Base directory path where configuration files reside.
        :param config_filter: Set of specific config names to process; if None, all are processed.
        :return: The parsed lineage metadata.
        """
        self.base_path = base_path
        teams_config = os.path.join(base_path, "teams.json")
        with open(teams_config) as team_infile:
            self.team_conf = json.load(team_infile)

        # Staging queries are lazily parsed when they are used.
        self.parse_configs(self.handle_staging_query, "production/staging_queries", ttypes.StagingQuery)

        # Parse all group bys and joins.
        self.parse_configs(self.parse_group_by, "production/group_bys", ttypes.GroupBy, config_filter)
        self.parse_configs(self.parse_join, "production/joins", ttypes.Join, config_filter)

        return self.metadata

    def parse_configs(
        self,
        parser: Callable[[Any], None],
        config_path: str,
        config_ttype: ttypes,
        config_filter: Optional[List[str]] = None,
    ) -> None:
        """
        Parse configs from a directory and process each using the provided parser.

        :param parser: Function to process a config.
        :param config_path: Relative path where configs are stored.
        :param config_ttype: Expected type of config objects.
        :param config_filter: Optional list of config names to include.
        """
        path = os.path.join(self.base_path, config_path)
        config_type = config_path.split("/")[-1]
        configs = []
        for root, dirs, files in os.walk(path):
            if root.endswith(config_path):
                continue
            configs.extend(extract_json_confs(config_ttype, root))

        for index, config in enumerate(configs):
            if isinstance(config, config_ttype):
                if config_filter and config.metaData.name not in config_filter:
                    continue
                try:
                    logger.info(f"({index}/{len(configs)}): Parse {config_type} {config.metaData.name} ...")
                    parser(config)
                except Exception as e:
                    self.metadata.unparsed_configs[config_type].append(config.metaData.name)
                    logger.exception(
                        f"An unexpected error occurred while parsing {config_type} {config.metaData.name}: {e}"
                    )

        logger.info(
            f"Total {len(configs)} configs for {config_type}."
            f" Unparsed = {len(self.metadata.unparsed_configs[config_type])}."
        )
        for name in self.metadata.unparsed_configs[config_type]:
            logger.info(f"Unparsed configs: {name}")

    @staticmethod
    def build_select_sql(table: str, selects: List[Tuple[Any, Any]], filter_expr: Optional[Any] = None) -> exp.Select:
        """
        Build a SQL SELECT statement.

        :param table: The table name.
        :param selects: A list of tuples representing select expressions (alias, column).
        :param filter_expr: An optional filter expression for the WHERE clause.
        :return: The built SQLGlot SELECT expression.
        """
        # Clean up any added quotes from select expressions.
        selects = [
            (
                k.replace("`", "") if isinstance(k, str) else k,
                v.replace("`", "") if isinstance(v, str) else v,
            )
            for k, v in selects
        ]
        sql = (
            exp.Select(
                expressions=[exp.alias_(v, k) for k, v in selects],
            )
            .from_(table)
            .where(filter_expr)
        )
        return sql

    def build_aggregate_sql(self, table: str, gb, selects: Optional[List[Tuple[Any, Any]]] = None) -> exp.Select:
        """
        Build a SQL aggregate statement applying AGG() on specified columns.

        :param table: The table name.
        :param gb: The group-by configuration object.
        :param selects: A list of tuples representing select expressions (alias, column).
        :return: The built SQLGlot aggregate SELECT expression.
        """
        expressions = []
        expressions.extend(gb.keyColumns)
        if gb.aggregations:
            for agg in gb.aggregations:
                op_name = ttypes.Operation._VALUES_TO_NAMES[agg.operation]
                agg_names = get_output_col_names(agg)
                for agg_name in agg_names:
                    expressions.append(f"AGG_{op_name}(`{agg.inputColumn}`) AS {agg_name}")
        elif selects:
            for input_column_name, input_column_expr in selects:
                expressions.append(input_column_name)

        expressions.sort()
        sql = exp.Select(expressions=expressions).from_(table)
        return sql

    @staticmethod
    def build_gb_derive_sql(table: str, gb: Any) -> exp.Select:
        """
        Build the SQL derivation query for a group-by configuration.

        :param table: The base table name.
        :param gb: The group-by configuration object.
        :return: The SQLGlot SELECT expression for group-by derivations.
        """
        pre_derived_columns = get_pre_derived_group_by_columns(gb)
        pre_derived_columns.sort()

        output_columns = get_group_by_output_columns(gb)
        output_columns.sort()

        derivation_columns = set(d.name for d in gb.derivations)

        base_sql = exp.Select(expressions=[exp.Column(this=column) for column in pre_derived_columns]).from_(table)

        sql = exp.subquery(base_sql, table).select()

        for derivation in gb.derivations:
            if derivation.name != "*":
                sql = sql.select(exp.alias_(derivation.expression, derivation.name))
        for column in [c for c in output_columns if c not in derivation_columns]:
            sql = sql.select(column)

        return sql

    @staticmethod
    def build_join_derive_sql(table: str, join: Any) -> exp.Select:
        """
        Build the SQL derivation query for a join configuration.

        :param table: The base table name.
        :param join: The join configuration object.
        :return: The SQLGlot SELECT expression for join derivations.
        """
        pre_derived_columns = [value for values in get_pre_derived_join_features(join).values() for value in values]
        pre_derived_columns.extend(get_pre_derived_source_keys(join.left))
        output_columns = get_join_output_columns(join)
        output_columns = (
            output_columns[FeatureDisplayKeys.SOURCE_KEYS] + output_columns[FeatureDisplayKeys.DERIVED_COLUMNS]
        )

        derivation_columns = set(d.name for d in join.derivations)

        pre_derived_columns.sort()
        output_columns.sort()

        base_sql = exp.Select()
        for column in pre_derived_columns:
            base_sql = base_sql.select(column)
            base_sql = base_sql.from_(table)

        sql = exp.subquery(base_sql, table).select()

        for derivation in join.derivations:
            if derivation.name != "*":
                sql = sql.select(exp.alias_(derivation.expression, derivation.name))
        for column in [c for c in output_columns if c not in derivation_columns]:
            sql = sql.select(column)

        return sql

    @staticmethod
    def base_table_name(table: str) -> str:
        """
        Extract the base table name from a table string with sub partitions.

        :param table: The table string containing sub partitions.
        :return: The base table name.
        """
        return table.split("/")[0]

    def parse_source(self, source: Any) -> Tuple[str, str, Dict[str, str]]:
        """
        Parse the source configuration to extract the table name, filter expression, and select mappings.

        :param source: The source configuration object.
        :return: A tuple containing:
                 - table (str): The table name.
                 - filter_expr (str): The combined filter expression.
                 - selects (Dict[str, str]): Mapping of select expressions.
        """
        if source.entities:
            table = self.base_table_name(source.entities.snapshotTable)
            wheres = source.entities.query.wheres or []
            selects = source.entities.query.selects or {}
        elif source.events:
            table = self.base_table_name(source.events.table)
            wheres = source.events.query.wheres or []
            selects = source.events.query.selects or {}
        else:
            table = self.object_table_name(source.joinSource.join)
            wheres = source.joinSource.query.wheres or []
            selects = source.joinSource.query.selects or {}
            self.parse_join(source.joinSource.join)

        filter_expr = " AND ".join([f"({where})" for where in sorted(wheres)])
        return table, filter_expr, selects

    def get_team(self, metadata: Dict[str, Any]) -> str:
        """
        Extract the team name from the given metadata.

        Retrieves the default team name from the 'team' key, and uses the 'team_override' if available.

        :param metadata: Dictionary containing team information and optional custom JSON.
        :return: The effective team name, possibly overridden by custom configuration.
        """
        # Retrieve the default team name from metadata.
        team = metadata["team"]

        # If 'customJson' is present and contains a 'team_override', use the override.
        if "customJson" in metadata and "team_override" in json.loads(metadata["customJson"]):
            team = json.loads(metadata["customJson"])["team_override"]

        return team

    def object_table_name(self, obj: Any) -> str:
        """
        Construct the fully qualified table name based on the object's team configuration.

        :param obj: The configuration object containing metaData.
        :return: The fully qualified table name.
        """
        if obj.metaData.outputNamespace:
            return output_table_name(obj, full_name=True)
        else:
            sanitized_table_name = sanitize(obj.metaData.name)
            team = self.get_team(obj.metaData)
            if "namespace" in self.team_conf[team]:
                db = self.team_conf[team]["namespace"]
            else:
                db = self.team_conf["default"]["namespace"]
            return db + "." + sanitized_table_name

    @staticmethod
    def check_source_select_non_empty(source: Any) -> Any:
        """
        Check if the source configuration contains non-empty select mappings.

        :param source: The source configuration object.
        :return: The select mappings if present.
        """
        if source.events:
            return source.events.query.selects
        elif source.entities:
            return source.entities.query.selects
        elif source.joinSource:
            return source.joinSource.query.selects

    def build_join_sql(self, join: Any) -> exp.Select:
        """
        Build the SQL LEFT JOIN statement based on the join configuration.

        :param join: The join configuration object.
        :return: The built SQLGlot SELECT expression representing the left join.
        """
        left_table_columns = [value for values in get_pre_derived_join_features(join).values() for value in values]
        left_table_keys = []
        if self.check_source_select_non_empty(join.left):
            left_table_keys = get_pre_derived_source_keys(join.left)
            left_table_columns.extend(left_table_keys)

        join_table = self.object_table_name(join)
        join_table_alias = sanitize(join.metaData.name)
        bootstrap_table = f"{join_table}_bootstrap"

        left_table_columns.sort()
        expressions = []
        for column in left_table_columns:
            if column in left_table_keys:
                expressions.append(exp.Column(this=f"{join_table_alias}.{column}"))
            else:
                expressions.append(exp.Column(this=f"{column}"))

        sql = exp.Select(expressions=expressions).from_(f"{bootstrap_table} AS {join_table_alias}")
        for jp in join.joinParts:
            gb = jp.groupBy
            gb_name = sanitize(gb.metaData.name)
            group_by_columns = get_group_by_output_columns(gb)
            prefix = jp.prefix + "_" if jp.prefix else ""
            join_part_table = f"{prefix}{gb_name}"

            group_by_selects = [(key_column, key_column) for key_column in gb.keyColumns]
            # rename column from join part table to join table by adding the prefix of the join parts
            for column in group_by_columns:
                if column not in gb.keyColumns:
                    group_by_selects.append((f"{join_part_table}_{column}", f"{column}"))

            # sort the column to make the order deterministic
            group_by_selects.sort()

            # select from the join part table
            gb_sql = self.build_select_sql(f"{join_table}_{join_part_table}", group_by_selects)
            conditions = []
            join_expression = []
            for key_column in gb.keyColumns:
                left_key = right_key = key_column
                if jp.keyMapping:
                    for select_key, gp_key in jp.keyMapping.items():
                        if key_column == gp_key:
                            # Verify select_key exists in left table columns since mistypings may occur.
                            if select_key in left_table_columns:
                                left_key = select_key
                                right_key = gp_key
                conditions.append(
                    exp.EQ(
                        this=exp.Column(this=left_key, table=join_table_alias),
                        expression=exp.Column(this=right_key, table=f"gb_{join_part_table}"),
                    )
                )
                join_expression.append(f"({join_table_alias}.{left_key} = gb_{join_part_table}.{right_key})")
            sql = sql.join(
                exp.Subquery(alias=f"gb_{join_part_table}", this=gb_sql),
                on=exp.and_(*conditions),
                join_type="left",
            )
        return sql

    @staticmethod
    def get_join_key_columns(join: Any) -> Set[str]:
        """
        Parse a join configuration and get the key columns for join output tables.

        :param join: The join configuration object.
        :return: Set of key columns.
        """
        key_columns = set()
        for jp in join.joinParts:
            gb = jp.groupBy
            for key_column in gb.keyColumns:
                if jp.keyMapping:
                    for select_key, gp_key in jp.keyMapping.items():
                        if key_column == gp_key:
                            key_columns.add(select_key)
                else:
                    key_columns.add(key_column)
        return key_columns

    def parse_join(self, join: Any) -> None:
        """
        Parse a join configuration and build lineage based on its SQL.

        :param join: The join configuration object.
        :return: None
        """
        # store config
        config_name = join.metaData.name
        config = Config(config_name, ConfigType.JOIN, join)
        self.metadata.configs[config_name] = config

        # source tables used to build lineage
        sources = dict()
        output_table = self.object_table_name(join)

        # build select sql for left
        table, filter_expr, selects = self.parse_source(join.left)
        source_keys = self.get_join_key_columns(join)
        if table in self.staging_queries:
            self.parse_staging_query(self.staging_queries[table])

        # Build lineage for source table --> boostrap table
        bootstrap_selects = [(k, v) for k, v in selects.items()]
        bootstrap_sql = self.build_select_sql(table, bootstrap_selects, filter_expr)
        bootstrap_table = f"{output_table}_bootstrap"
        bootstrap_sql = bootstrap_sql.sql(dialect="spark", pretty=True)
        sources["bootstrap_table"] = bootstrap_sql
        lineages = build_lineage(bootstrap_table, bootstrap_sql)
        self.metadata.store_table(
            config_name,
            bootstrap_table,
            TableType.JOIN_BOOTSTRAP,
            materialized=True,
            key_columns=set(source_keys),
        )
        self.metadata.store_lineage(lineages, output_table)

        for jp in join.joinParts:
            gb = jp.groupBy
            gb_table_name = sanitize(gb.metaData.name)
            prefix = jp.prefix + "_" if jp.prefix else ""
            join_part_table = f"{output_table}_{prefix}{gb_table_name}"
            self.parse_group_by(gb, join_part_table=join_part_table, join_config_name=config_name)

        # Build lineage for join
        join_sql = self.build_join_sql(join)

        sql = join_sql.sql(dialect="spark", pretty=True)
        sources["join_table"] = sql

        features = get_pre_derived_join_internal_features(join)

        # Build derivation SQL if derivations exist.
        if join.derivations:
            derived_features = [derivation.name for derivation in join.derivations]
            # If "*" is defined then append derived features, otherwise only derived features are used.
            if "*" in derived_features:
                derived_features.remove("*")
                features.extend(derived_features)
                # If it is a rename only derivation, then remove the original feature name
                removed_features = [
                    derivation.expression for derivation in join.derivations if is_identifier(derivation.expression)
                ]
                for removed_feature in removed_features:
                    if removed_feature in features:
                        features.remove(removed_feature)
            else:
                features = derived_features

            derive_sql = self.build_join_derive_sql("join_table", join)
            sql = derive_sql.sql(dialect="spark", pretty=True)
            sources["derive_table"] = sql

        # store feature
        for feature in features:
            self.metadata.store_feature(join.metaData.name, ConfigType.JOIN, feature, output_table)
        # no column lineage for external features
        external_features = get_pre_derived_external_features(join)
        for feature in external_features:
            self.metadata.store_feature(join.metaData.name, ConfigType.JOIN, feature)

        # store table
        self.metadata.store_table(
            config_name,
            output_table,
            TableType.JOIN,
            materialized=True,
            key_columns=set(source_keys),
        )

        # store lineage
        parsed_lineages = build_lineage(output_table, sql, sources)
        self.metadata.store_lineage(parsed_lineages, output_table)

    def handle_staging_query(self, staging_query: Any) -> None:
        """
        Store a staging query configuration.

        :param staging_query: The staging query configuration object.
        :return: None
        """
        table_name = output_table_name(staging_query, full_name=True)
        self.staging_queries[table_name] = staging_query

    def parse_staging_query(self, staging_query: Any) -> None:
        """
        Parse a staging query configuration and build its lineage.

        :param query: The staging query configuration object.
        :return: None
        """
        # store config
        config_name = staging_query.metaData.name
        config = Config(config_name, ConfigType.STAGING_QUERY, staging_query)
        self.metadata.configs[config_name] = config

        schema = dict()
        if self.schema_provider:
            tables = self.find_all_tables(staging_query.query)
            for table in tables:
                schema[table] = self.schema_provider(table)

        table_name = output_table_name(staging_query, full_name=True)

        # if we have already parsed this staging query output table then no need to parse again
        if table_name not in self.parsed_staging_query_tables:
            self.parsed_staging_query_tables.add(table_name)
            parsed_lineages = build_lineage(table_name, staging_query.query, schema=schema)
            self.metadata.store_table(config_name, table_name, TableType.STAGING_QUERY)
            self.metadata.store_lineage(parsed_lineages, table_name)

    @staticmethod
    def find_all_tables(sql):
        """
        Find references to physical tables in an expression.

        :param sql: The SQL expression to analyze.
        :return: A set of table names.
        """
        sql = exp.maybe_parse(sql)

        root = build_scope(sql)
        return {
            source.name
            for scope in root.traverse()
            for source in scope.sources.values()
            if isinstance(source, exp.Table)
        }

    def parse_group_by(
        self,
        gb: Any,
        join_part_table: Optional[str] = None,
        join_config_name: Optional[str] = None,
    ) -> None:
        """
        Parse a group-by configuration and build lineage.

        :param gb: The group-by configuration object.
        :param join_part_table: The join part table name for the group by.
        :param join_config_name: The join config name if it is parsing from a join.
        :return: None
        """
        # store config
        config_name = gb.metaData.name
        config = Config(config_name, ConfigType.GROUP_BY, gb)
        self.metadata.configs[config_name] = config

        # source tables used to build lineage
        sources = {}

        # build select sql
        select_sqls = []
        for source in gb.sources:
            table, filter_expr, selects = self.parse_source(source)
            if table in self.staging_queries:
                self.parse_staging_query(self.staging_queries[table])
            self.metadata.store_table(config_name, table, table_type=TableType.OTHER)
            sql = self.build_select_sql(table, selects.items(), filter_expr)
            select_sqls.append(sql)

        select_sql = reduce(exp.union, select_sqls)

        # Build aggregation SQL.
        key_columns = gb.keyColumns
        agg_sql = self.build_aggregate_sql("select_table", gb, selects=selects.items())

        sql = agg_sql.sql(pretty=True)
        sources["select_table"] = select_sql.sql(pretty=True)

        features = get_pre_derived_group_by_features(gb)
        if gb.derivations:
            derived_features = [derivation.name for derivation in gb.derivations]
            # If "*" is defined then append derived features, otherwise only derived features are used.
            if "*" in derived_features:
                derived_features.remove("*")
                features.extend(derived_features)
                # If it is a rename only derivation, then remove the original feature name
                removed_features = [
                    derivation.expression for derivation in gb.derivations if is_identifier(derivation.expression)
                ]
                for removed_feature in removed_features:
                    if removed_feature in features:
                        features.remove(removed_feature)
            else:
                features = derived_features

            derive_sql = self.build_gb_derive_sql("aggregate_table", gb)
            sources["aggregate_table"] = sql
            sql = derive_sql.sql(pretty=True)

        # If join_part_table is defined, then it generates a join part table.
        # If gb.backfill_start_date is defined, then it generates a group by backfill table.
        # If gb.online is True, then it generates an upload table.
        if join_part_table:
            # store table
            config_name = join_config_name or config_name
            self.metadata.store_table(
                config_name,
                join_part_table,
                TableType.JOIN_PART,
                materialized=True,
                key_columns=set(key_columns),
            )

            # store lineage
            parsed_lineages = build_lineage(join_part_table, sql, sources)
            self.metadata.store_lineage(parsed_lineages, join_part_table)
        else:
            if gb.metaData.online:
                output_table = f"{self.object_table_name(gb)}_upload"

                # store table
                self.metadata.store_table(
                    config_name,
                    output_table,
                    TableType.GROUP_BY_UPLOAD,
                    materialized=True,
                    key_columns={"key_json"},
                )

                # Put all key columns into the "key_json" column, and all values into the "value_json" column.
                lineages = build_lineage(output_table, sql, sources)
                updated_lineages = defaultdict(set)
                for (output_table, output_column), lineage in lineages.items():
                    if output_column in key_columns:
                        updated_lineages[(output_table, "key_json")].update(lineage)
                    else:
                        updated_lineages[(output_table, "value_json")].update(lineage)
                self.metadata.store_lineage(updated_lineages, output_table)

            # track feature lineage to the group_by backfill table
            output_table = self.object_table_name(gb)

            # store table
            self.metadata.store_table(
                config_name,
                output_table,
                TableType.GROUP_BY_BACKFILL,
                materialized=gb.backfillStartDate,
                key_columns=set(key_columns),
            )

            # store lineage
            parsed_lineages = build_lineage(output_table, sql, sources)
            self.metadata.store_lineage(parsed_lineages, output_table)

            # store feature
            for feature in features:
                self.metadata.store_feature(gb.metaData.name, ConfigType.GROUP_BY, feature, output_table)


def _get_col_node_name(node: Node, parent_node: Node) -> Tuple[str, str]:
    """
    Get the fully qualified column name from a SQLGlot Node.

    :param node: A SQLGlot Node object representing a column.
    :param node: A SQLGlot Node object representing the parent node.
    :return: The tuple of table name and column name.
    """
    # Use parent.expression if it is a complex type
    if isinstance(parent_node.expression.this, exp.Dot):
        full_name = parent_node.expression.this.sql(pretty=False, identify=False)
        return full_name.split(".")[0], ".".join(full_name.split(".")[1:])
    else:
        column_name = node.name.split(".")[-1]
        # Strip table alias from table expression if present.
        table_exp: exp.Expression = node.expression.copy()
        table_exp.args["alias"] = None
        return table_exp.sql(pretty=False, identify=False), column_name.replace('"', "")


def get_transform(expression: Any) -> Optional[str]:
    """
    Determine the transformation operation name from a SQLGlot expression.

    :param expression: A SQLGlot expression.
    :return: The transformation operation name, or None if the expression is an "Alias" operation.
    """
    if not expression.this:
        return None
    if expression.this.__class__.__name__ == "Anonymous":
        return expression.this.alias_or_name
    elif expression.this.__class__.__name__ == "Column":
        return expression.__class__.__name__ if expression.__class__.__name__ != "Alias" else None
    return expression.this.__class__.__name__


def append_transform(transforms: List[str], transform: str) -> List[str]:
    """
    Append a transform to the list if it is non-empty and not already the last item.

    :param transforms: A list of transform strings.
    :param transform: The transform string to append.
    :return: The updated list of transforms.
    """
    if transform and (not transforms or transforms[-1] != transform):
        return transforms + [transform]
    return transforms


def build_lineage(
    output_table: str,
    sql: str,
    sources: Dict[str, str] = None,
    schema: Dict[str, str] = None,
) -> Dict[Tuple[str, str], Set[Tuple[Tuple[str, str], Tuple[str]]]]:
    """
    Build the lineage mapping from the SQL query and its source queries.

    :param output_table: The fully qualified output table name.
    :param sql: The SQL query string.
    :param sources: Mapping of source names to SQL query strings.
    :param schema: Mapping of source names to source schema.
    :return: A dictionary mapping each output column to a list of input columns and operations to the output column.
    """
    dialect = "spark"
    expression = maybe_parse(sql, dialect=dialect)
    if sources:
        expression = exp.expand(
            expression,
            {k: typing.cast(exp.Query, maybe_parse(v, dialect=dialect)) for k, v in sources.items()},
            dialect=dialect,
        )
    expression = qualify.qualify(
        expression,
        dialect="spark",
        schema=schema,
        **{"validate_qualify_columns": False, "identify": False},
    )
    parsed_lineages = dict()
    scope = build_scope(expression)
    for output_column in [exp.alias_or_name for exp in scope.expression.expressions if exp.alias_or_name != "*"]:
        # Normalize the output column identifier.
        column = normalize_identifiers.normalize_identifiers(output_column, dialect=dialect).name
        column_node = to_node(column, scope, dialect, trim_selects=False)
        input_columns = set()
        transform = get_transform(column_node.expression)
        transforms = [transform] if transform else []
        queue = [(column_node, transforms)]
        while queue:
            parent, transforms = queue.pop(0)
            for child in parent.downstream:
                if isinstance(child.expression, exp.Table):
                    input_columns.add((_get_col_node_name(child, parent), tuple(transforms)))
                child_transform = get_transform(child.expression)
                queue.append((child, append_transform(transforms, child_transform)))
        parsed_lineages[(output_table, output_column)] = input_columns
    return parsed_lineages
