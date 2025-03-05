import json
import logging
import os
import typing
from functools import reduce
from typing import Any, Dict, List, Optional, Set, Tuple

from ai.chronon.api import ttypes
from ai.chronon.group_by import Accuracy, _get_op_suffix
from ai.chronon.lineage.lineage_metadata import LineageMetaData, TableType
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
)
from ai.chronon.utils import FeatureDisplayKeys, output_table_name, sanitize
from sqlglot import expressions as exp
from sqlglot import maybe_parse
from sqlglot.lineage import Node, to_node
from sqlglot.optimizer import build_scope, normalize_identifiers, qualify

logger = logging.getLogger(__name__)


class LineageParser:
    def __init__(self) -> None:
        """
        Initialize the LineageParser with empty lineage sets, configuration, and staging query storage.

        Attributes:
            lineages (Set[Tuple[str, Tuple[str, List[str]]]]): Set of lineage mappings as (input_column, (output_column, operation list)).
            unparsed_columns (Dict[str, List[str]]): Dictionary for columns where lineage could not be determined.
            base_path (Optional[str]): The base directory path for configuration files.
            team_conf (Optional[Dict[str, Any]]): Team configuration loaded from JSON.
            staging_query_tables (Dict[str, Any]): Mapping of staging query table names to their configuration.
        """

        self.base_path: Optional[str] = None
        self.team_conf: Optional[Dict[str, Any]] = None
        self.staging_query_tables: Dict[str, Any] = {}
        self.metadata: LineageMetaData = LineageMetaData()

    def parse_lineage(
        self, base_path: str, entities: Optional[Set[str]] = None
    ) -> Set[Tuple[Tuple[str, Tuple[str]], str]]:
        """
        Parse lineage information for staging queries, group bys, and joins using the provided base path.

        Args:
            base_path (str): Base directory path where configuration files reside.
            entities (Optional[Set[str]]): Set of specific entity names to process; if None, all are processed.

        Returns:
            None
        """
        self.base_path = base_path
        teams_config = os.path.join(base_path, "teams.json")
        with open(teams_config) as team_infile:
            self.team_conf = json.load(team_infile)

        self.parse_staging_queries()
        self.parse_group_bys(entities)
        self.parse_joins(entities)

        return self.metadata

    def parse_staging_queries(self) -> None:
        """
        Walk through the staging_queries directory and process each staging query.

        Returns:
            None
        """
        staging_queries_path = os.path.join(self.base_path, "production/staging_queries/")
        for root, dirs, files in os.walk(staging_queries_path):
            if root.endswith("staging_queries/"):
                continue
            staging_queries = extract_json_confs(ttypes.StagingQuery, root)
            for staging_query in staging_queries:
                try:
                    self.handle_staging_query(staging_query)
                except Exception as e:
                    logger.exception(
                        f"An unexpected error occurred while parsing group by {staging_query.metaData.name}: {e}"
                    )

    def parse_group_bys(self, entities: Optional[Set[str]] = None) -> None:
        """
        Walk through the group_bys directory and process each group-by configuration.

        Args:
            entities (Optional[Set[str]]): Set of specific group-by names to process.

        Returns:
            None
        """
        group_bys_path = os.path.join(self.base_path, "production/group_bys/")
        for root, dirs, files in os.walk(group_bys_path):
            if root.endswith("group_bys/"):
                continue
            all_group_bys = extract_json_confs(ttypes.GroupBy, root)
            for group_by in all_group_bys:
                if isinstance(group_by, ttypes.GroupBy):
                    if entities and group_by.metaData.name not in entities:
                        continue
                    try:
                        print(f"handle group by {group_by.metaData.name}")
                        self.parse_group_by(group_by)
                    except Exception as e:
                        logger.exception(
                            f"An unexpected error occurred while parsing group by {group_by.metaData.name}: {e}"
                        )

    def parse_joins(self, entities: Optional[Set[str]] = None) -> None:
        """
        Walk through the joins directory and process each join configuration.

        Args:
            entities (Optional[Set[str]]): Set of specific join names to process.

        Returns:
            None
        """
        joins_path = os.path.join(self.base_path, "production/joins/")
        for root, dirs, files in os.walk(joins_path):
            if root.endswith("joins/"):
                continue
            all_joins = extract_json_confs(ttypes.Join, root)
            for join in all_joins:
                if isinstance(join, ttypes.Join):
                    if entities and join.metaData.name not in entities:
                        continue
                    try:
                        self.parse_join(join)
                    except Exception as e:
                        logger.exception(f"An unexpected error occurred while parsing join {join.metaData.name}: {e}")

    @staticmethod
    def get_all_agg_exprs(aggregation: Any) -> List[str]:
        """
        Generate all aggregate expression names based on the aggregation's windows and buckets.

        Args:
            aggregation (Any): The aggregation configuration object.

        Returns:
            List[str]: A list of aggregate expression names.
        """
        base_name = f"{_get_op_suffix(aggregation.operation, aggregation.argMap)}"
        windowed_names: List[str] = []
        if aggregation.windows:
            for window in aggregation.windows:
                unit = ttypes.TimeUnit._VALUES_TO_NAMES[window.timeUnit].lower()[0]
                window_suffix = f"{window.length}{unit}"
                windowed_names.append(f"{base_name}_{window_suffix}")
        else:
            windowed_names = [base_name]

        bucketed_names = []
        if aggregation.buckets:
            for bucket in aggregation.buckets:
                bucketed_names.extend([f"{name}_by_{bucket}" for name in windowed_names])
        else:
            bucketed_names = windowed_names

        return bucketed_names

    @staticmethod
    def build_select_sql(table: str, selects: List[Tuple[Any, Any]], filter_expr: Optional[Any] = None) -> exp.Select:
        """
        Build a SQL SELECT statement.

        Args:
            table (str): The table name.
            selects (List[Tuple[Any, Any]]): A list of tuples representing select expressions (alias, column).
            filter_expr (Optional[Any]): An optional filter expression for the WHERE clause.

        Returns:
            exp.Select: The built SQLGlot SELECT expression.
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

    @staticmethod
    def build_aggregate_sql(
        table: str,
        key_columns: List[str],
        agg_columns: List[Tuple[Any, Any]],
        filter_expr: Optional[Any] = None,
    ) -> exp.Select:
        """
        Build a SQL aggregate statement applying AGG() on specified columns.

        Args:
            table (str): The table name.
            key_columns (List[Tuple[Any, Any]]): A list of tuples representing key columns.
            agg_columns (List[Tuple[Any, Any]]): A list of tuples representing aggregate expressions (alias, column).
            filter_expr (Optional[Any]): An optional filter expression for the WHERE clause.

        Returns:
            exp.Select: The built SQLGlot aggregate SELECT expression.
        """
        sql = (
            exp.Select(
                expressions=key_columns + [f"AGG({v}) AS {k}" for k, v in agg_columns],
            )
            .from_(table)
            .where(filter_expr)
        )
        return sql

    @staticmethod
    def build_gb_derive_sql(table: str, gb: Any) -> exp.Select:
        """
        Build the SQL derivation query for a group-by configuration.

        Args:
            table (str): The base table name.
            gb (Any): The group-by configuration object.

        Returns:
            exp.Select: The SQLGlot SELECT expression for group-by derivations.
        """
        pre_derived_columns = get_pre_derived_group_by_columns(gb)
        output_columns = get_group_by_output_columns(gb)
        output_columns.append("ds")
        derivation_columns = set(d.name for d in gb.derivations)

        base_sql = exp.Select(expressions=[exp.Column(this=column) for column in pre_derived_columns]).from_(table)

        sql = exp.subquery(base_sql, table).select()

        for derivation in gb.derivations:
            sql = sql.select(exp.alias_(derivation.expression, derivation.name))
        for column in [c for c in output_columns if c not in derivation_columns]:
            sql = sql.select(column)

        return sql

    @staticmethod
    def build_join_derive_sql(join: Any) -> exp.Select:
        """
        Build the SQL derivation query for a join configuration.

        Args:
            join (Any): The join configuration object.

        Returns:
            exp.Select: The SQLGlot SELECT expression for join derivations.
        """
        pre_derived_columns = {value for values in get_pre_derived_join_features(join).values() for value in values}
        pre_derived_columns.update(get_pre_derived_source_keys(join.left))
        output_columns = get_join_output_columns(join)
        output_columns = (
            output_columns[FeatureDisplayKeys.SOURCE_KEYS] + output_columns[FeatureDisplayKeys.DERIVED_COLUMNS]
        )

        derivation_columns = set(d.name for d in join.derivations)

        base_sql = exp.Select()
        for column in pre_derived_columns:
            base_sql = base_sql.select(column)
            base_sql = base_sql.from_("join_table")

        sql = exp.subquery(base_sql, "join_table").select()

        for derivation in join.derivations:
            if derivation.name not in ("ds", "*"):
                sql = sql.select(exp.alias_(derivation.expression, derivation.name))
        for column in [c for c in output_columns if c not in derivation_columns]:
            if column != "ds":
                sql = sql.select(column)

        return sql

    @staticmethod
    def base_table_name(table: str) -> str:
        """
        Extract the base table name from a given table string with sub partitions.

        Returns:
            str: The base table name.
        """
        return table.split("/")[0]

    def parse_source(self, source: Any) -> Tuple[str, str, str, Dict[str, str]]:
        """
        Parse the source configuration to extract the table name, filter expression, and select mappings.

        Args:
            source (Any): The source configuration object.

        Returns:
            Tuple[str, str, Dict[str, str]]:
                - table (str): The table name.
                - filter_expr (str): The combined filter expression.
                - table_type (str): Type of source table.
                - selects (Dict[str, str]): Mapping of select expressions.
        """
        table_type = "HIVE_TABLE"
        if source.entities:
            if source.entities.query and source.entities.query.mutationTimeColumn:
                table_type = "DB_EXPORT"
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
        return table, filter_expr, table_type, selects

    def get_team(self, metadata):
        team = metadata["team"]
        if "customJson" in metadata and "team_override" in json.loads(metadata["customJson"]):
            team = json.loads(metadata["customJson"])["team_override"]
        return team

    def object_table_name(self, obj: Any) -> str:
        """
        Construct the fully qualified table name based on the object's team configuration.

        Args:
            obj (Any): The configuration object containing metaData.

        Returns:
            str: The fully qualified table name.
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
        Check if the source configuration has non-empty select mappings.

        Args:
            source (Any): The source configuration object.

        Returns:
            Any: The select mappings if present.
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

        Args:
            join (Any): The join configuration object.

        Returns:
            exp.Select: The built SQLGlot SELECT expression representing the left join.
        """
        left_table_columns = {value for values in get_pre_derived_join_features(join).values() for value in values}
        if self.check_source_select_non_empty(join.left):
            left_table_columns.update(get_pre_derived_source_keys(join.left))

        # ts is not in join outputs
        if "ts" in left_table_columns:
            left_table_columns.remove("ts")
            left_table_columns.add("ds")

        join_table = self.object_table_name(join)
        join_table_alias = sanitize(join.metaData.name)
        bootstrap_table = f"{join_table}_bootstrap"
        sql = exp.Select(expressions=[exp.Column(this=column) for column in left_table_columns]).from_(
            f"{bootstrap_table} AS {join_table_alias}"
        )
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
                        expression=exp.Column(this=right_key, table=f"{prefix}{gb_name}"),
                    )
                )
                join_expression.append(f"({join_table_alias}.{left_key} = {join_part_table}.{right_key})")
            sql = sql.join(
                exp.Subquery(alias=f"{join_part_table}", this=gb_sql),
                on=exp.and_(*conditions),
                join_type="left",
            )
        return sql

    def parse_join(self, join: Any) -> dict[str, str | Any] | None:
        """
        Parse a join configuration and build lineage based on its SQL.

        Args:
            join (Any): The join configuration object.

        Returns:
            dict[str, str | Any]: Virtual queries and output tables.
        """
        # source tables used to build lineage
        sources = dict()

        output_table = self.object_table_name(join)

        # build select sql for left
        table, filter_expr, table_type, selects = self.parse_source(join.left)
        source_keys = get_pre_derived_source_keys(join.left)
        if table in self.staging_query_tables:
            sources[table] = self.staging_query_tables[table].query

        # Build lineage for source table --> boostrap table
        bootstrap_selects = [(k, v) for k, v in selects.items()]
        bootstrap_selects.append(("ds", "ds"))
        bootstrap_sql = self.build_select_sql(table, bootstrap_selects, filter_expr)
        bootstrap_table = f"{output_table}_bootstrap"
        bootstrap_sql = bootstrap_sql.sql(dialect="spark", pretty=True)
        sources["bootstrap_table"] = bootstrap_sql
        lineages = build_lineage(bootstrap_table, bootstrap_sql, sources={})
        self.metadata.store_table(bootstrap_table, TableType.JOIN_BOOTSTRAP)
        self.metadata.tables[bootstrap_table].key_columns = set(source_keys)
        self.metadata.store_lineage(lineages, output_table)

        for jp in join.joinParts:
            gb = jp.groupBy
            gb_table_name = sanitize(gb.metaData.name)
            prefix = jp.prefix + "_" if jp.prefix else ""
            join_part_table = f"{output_table}_{prefix}{gb_table_name}"
            self.parse_group_by(gb, join_part_table=join_part_table)

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
            else:
                features = derived_features

            derive_sql = self.build_join_derive_sql(join)
            sql = derive_sql.sql(dialect="spark", pretty=True)
            sources["derive_table"] = sql

        # store feature
        for feature in features:
            self.metadata.store_feature(join.metaData.name, feature, output_table)
        # no column lineage for external features
        external_features = get_pre_derived_external_features(join)
        for feature in external_features:
            self.metadata.store_feature(join.metaData.name, feature)

        # store table
        self.metadata.store_table(output_table, TableType.JOIN)
        self.metadata.tables[output_table].key_columns = set(source_keys)

        # store lineage
        parsed_lineages = build_lineage(output_table, sql, sources)
        self.metadata.store_lineage(parsed_lineages, output_table)

    def handle_staging_query(self, staging_query: Any) -> None:
        """
        Handle and store a staging query configuration.

        Args:
            staging_query (Any): The staging query configuration object.

        Returns:
            None
        """
        table_name = output_table_name(staging_query, full_name=True)
        self.staging_query_tables[table_name] = staging_query
        self.metadata.store_table(table_name, TableType.STAGING_QUERY)

    def parse_group_by(self, gb: Any, join_part_table: str = None) -> dict[str, str | Any] | None:
        """
        Parse a group-by configuration and build lineage.

        Args:
            gb (Any): The group-by configuration object.
            join_part_table: The join part table name for the group by.

        Returns:
            None
        """

        # source tables used to build lineage
        sources = {}

        # build select sql
        select_sqls = []
        for source in gb.sources:
            table, filter_expr, table_type, selects = self.parse_source(source)
            if table in self.staging_query_tables:
                sources[table] = self.staging_query_tables[table].query
            sql = self.build_select_sql(table, selects.items(), filter_expr)
            select_sqls.append(sql)

        select_sql = reduce(exp.union, select_sqls)

        # Build aggregation SQL.
        key_columns = ["ds"]
        agg_columns = []
        if gb.aggregations:
            for agg in gb.aggregations:
                all_agg_exprs = self.get_all_agg_exprs(agg)
                for agg_expr in all_agg_exprs:
                    aggregation = f"{agg.inputColumn}_{agg_expr}"
                    agg_columns.append((aggregation, agg.inputColumn))
        else:
            for input_column_name, input_column_expr in selects.items():
                agg_columns.append((input_column_name, input_column_name))

        # Add key columns.
        for key_column in gb.keyColumns:
            key_columns.append(key_column)

        agg_sql = self.build_aggregate_sql("select_table", key_columns, agg_columns)

        sql = agg_sql.sql(pretty=True)
        sources["select_table"] = select_sql.sql(pretty=True)

        features = get_pre_derived_group_by_features(gb)
        if gb.derivations:
            derived_features = [derivation.name for derivation in gb.derivations]
            # If "*" is defined then append derived features, otherwise only derived features are used.
            if "*" in derived_features:
                derived_features.remove("*")
                features.extend(derived_features)
            else:
                features = derived_features

            derive_sql = self.build_gb_derive_sql("aggregate_table", gb)
            sources["aggregate_table"] = sql
            sql = derive_sql.sql(pretty=True)

        # If join_part_table is defined, then it generates a join part table.
        # If gb.backfill_start_date is defined, then it generates a backfill table.
        # If gb.online is True and SNAPSHOT accuracy, then it generates an upload table.
        # If gb.online is True and TEMPORAL accuracy, then no need to track lineage since the output table stores IR only.
        if join_part_table:
            # store feature
            for feature in features:
                self.metadata.store_feature(gb.metaData.name, feature, join_part_table)

            # store table
            self.metadata.store_table(join_part_table, TableType.JOIN_PART)
            self.metadata.tables[join_part_table].key_columns = set(key_columns)

            # store lineage
            parsed_lineages = build_lineage(join_part_table, sql, sources)
            self.metadata.store_lineage(parsed_lineages, join_part_table)
        else:
            output_table = None
            if gb.metaData.online and gb.accuracy == Accuracy.SNAPSHOT:
                output_table = f"{self.object_table_name(gb)}_upload"

                # store table
                self.metadata.store_table(output_table, TableType.GROUP_BY_UPLOAD)
                self.metadata.tables[output_table].key_columns = set(key_columns)

                # store lineage
                parsed_lineages = build_lineage(output_table, sql, sources)
                self.metadata.store_lineage(parsed_lineages, output_table)
            # track feature lineage to the backfill table if it is defined
            if gb.backfillStartDate:
                output_table = self.object_table_name(gb)

                # store table
                self.metadata.store_table(output_table, TableType.GROUP_BY_BACKFILL)
                self.metadata.tables[output_table].key_columns = set(key_columns)

                # store lineage
                parsed_lineages = build_lineage(output_table, sql, sources)
                self.metadata.store_lineage(parsed_lineages, output_table)

            # store feature
            for feature in features:
                self.metadata.store_feature(gb.metaData.name, feature, output_table)


def _get_col_node_name(node: Node) -> str:
    """
    Get the fully qualified column name from a SQLGlot Node.

    Args:
        node (Node): A SQLGlot Node object representing a column.

    Returns:
        str: The fully qualified column name.
    """
    if isinstance(node.expression, exp.Table):
        # Strip table alias from column if present.
        real_column_name = node.name.split(".")[-1]
        # Strip table alias from table expression if present.
        table_exp: exp.Expression = node.expression.copy()
        table_exp.args["alias"] = None
        return f"{table_exp.sql(pretty=False, identify=False)}.{real_column_name}".replace('"', "")
    return f"{node.name}"


def get_transform_operation(expression: Any) -> str:
    """
    Determine the transformation operation name from a SQLGlot expression.

    Args:
        expression (Any): A SQLGlot expression.

    Returns:
        str: The transformation operation name.
    """
    if expression.this.__class__.__name__ == "Anonymous":
        return expression.this.alias_or_name
    elif expression.this.__class__.__name__ == "Column":
        return expression.__class__.__name__
    return expression.this.__class__.__name__


def build_lineage(output_table: str, sql: str, sources: Dict[str, str]) -> Dict[str, Tuple[str, Tuple[str,]]]:
    """
    Build the lineage mapping from the SQL query and its source queries.

    Args:
        output_table (str): The fully qualified output table name.
        sql (str): The SQL query string.
        sources (Dict[str, str]): Mapping of source names to SQL query strings.

    Returns:
        Dict[str, List[Any]]: A dictionary mapping each output column
                              to a list of input column and operations to the output column.
    """
    dialect = "spark"
    expression = maybe_parse(sql, dialect=dialect)
    expression = exp.expand(
        expression,
        {k: typing.cast(exp.Query, maybe_parse(v, dialect=dialect)) for k, v in sources.items()},
        dialect=dialect,
    )
    expression = qualify.qualify(
        expression,
        dialect="spark",
        schema=None,
        **{"validate_qualify_columns": False, "identify": False},
    )
    parsed_lineages: Dict[str, Tuple[str, Tuple[str,]]] = {}
    scope = build_scope(expression)
    for output_column in [exp.alias_or_name for exp in scope.expression.expressions if exp.alias_or_name != "*"]:
        # Normalize the output column identifier.
        column = normalize_identifiers.normalize_identifiers(output_column, dialect=dialect).name
        column_lineage = to_node(column, scope, dialect, trim_selects=False)
        input_columns = []
        queue = [(column_lineage, [get_transform_operation(column_lineage.expression)])]
        while queue:
            parent, ops = queue.pop(0)
            for child in parent.downstream:
                if isinstance(child.expression, exp.Table):
                    input_columns.append((_get_col_node_name(child), tuple(ops)))
                child_op = get_transform_operation(child.expression)
                # store operation if it is not a duplicate one
                queue.append((child, ops + [child_op] if child_op != ops[-1] else ops))
        parsed_lineages[f"{output_table}.{output_column}"] = input_columns
    return parsed_lineages
