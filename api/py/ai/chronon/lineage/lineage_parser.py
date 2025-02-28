import json
import logging
import os
import typing
from collections import defaultdict
from functools import reduce
from typing import Any, Dict, List, Optional, Set, Tuple

from ai.chronon.api import ttypes
from ai.chronon.group_by import _get_op_suffix
from ai.chronon.repo.validator import (
    extract_json_confs,
    get_group_by_output_columns,
    get_join_output_columns,
    get_pre_derived_group_by_columns,
    get_pre_derived_join_features,
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
            processed_group_by (Set[str]): Set of group-by names already processed.
            processed_join (Set[str]): Set of join names already processed.
        """
        self.lineages: Set[Tuple[Tuple[str, Tuple[str]], str]] = set()
        self.unparsed_columns: Dict[str, List[str]] = defaultdict(list)
        self.base_path: Optional[str] = None
        self.team_conf: Optional[Dict[str, Any]] = None
        self.staging_query_tables: Dict[str, Any] = {}
        self.processed_group_by: Set[str] = set()
        self.processed_join: Set[str] = set()

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

        return self.lineages

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
        parsed, unparsed = 0, 0
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
                        parsed += 1
                    except Exception as e:
                        logger.exception(
                            f"An unexpected error occurred while parsing group by {group_by.metaData.name}: {e}"
                        )
                        unparsed += 1
        print("here")

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
        table: str, selects: List[Tuple[Any, Any]], filter_expr: Optional[Any] = None
    ) -> exp.Select:
        """
        Build a SQL aggregate statement applying SUM() on specified columns.

        Args:
            table (str): The table name.
            selects (List[Tuple[Any, Any]]): A list of tuples representing aggregate expressions (alias, column).
            filter_expr (Optional[Any]): An optional filter expression for the WHERE clause.

        Returns:
            exp.Select: The built SQLGlot aggregate SELECT expression.
        """
        sql = (
            exp.Select(
                expressions=[f"SUM({v}) AS {k}" for k, v in selects],
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
        output_columns = {value for values in get_join_output_columns(join).values() for value in values}

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

    def store_lineage(self, output_column: str, input_columns: Tuple[str, Tuple[str]]) -> None:
        """
        Store lineage information mapping input columns to an output column.

        Args:
            output_column (str): The output column name.
            input_columns (Tuple[str, Tuple[str]]): Tuple of input column names and operation paths to the output column .

        Returns:
            None
        """
        for input_column in input_columns:
            self.lineages.add((input_column, output_column))

    @staticmethod
    def base_table_name(table: str) -> str:
        """
        Extract the base table name from a given table string with sub partitions.

        Returns:
            str: The base table name.
        """
        return table.split("/")[0]

    def parse_source(self, source: Any) -> Tuple[str, str, Dict[str, str]]:
        """
        Parse the source configuration to extract the table name, filter expression, and select mappings.

        Args:
            source (Any): The source configuration object.

        Returns:
            Tuple[str, str, Dict[str, str]]:
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
            if "namespace" in self.team_conf[obj.metaData.team]:
                db = self.team_conf[obj.metaData.team]["namespace"]
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

    def build_left_join_sql(self, join: Any) -> exp.Select:
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

        sql = exp.Select(expressions=[exp.Column(this=column) for column in left_table_columns]).from_("select_table")
        for jp in join.joinParts:
            gb = jp.groupBy
            self.parse_group_by(gb)
            gb_name = sanitize(gb.metaData.name)
            gb_table_name = self.object_table_name(gb)
            group_by_columns = get_group_by_output_columns(gb)
            prefix = jp.prefix + "_" if jp.prefix else ""
            group_by_selects = []
            for column in group_by_columns:
                group_by_selects.append((f"{prefix}{gb_name}_{column}", f"{prefix}{gb_name}_{column}"))
            group_by_selects.extend([(key_column, key_column) for key_column in gb.keyColumns])
            gb_sql = self.build_select_sql(gb_table_name, group_by_selects)
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
                        this=exp.Column(this=left_key, table="select_table"),
                        expression=exp.Column(this=right_key, table=f"{prefix}{gb_name}"),
                    )
                )
                join_expression.append(f"(select_table.{left_key} = {prefix}{gb_name}.{right_key})")
            sql = sql.join(
                exp.Subquery(alias=f"{prefix}{gb_name}", this=gb_sql),
                on=exp.and_(*conditions),
                join_type="left",
            )
        return sql

    def parse_join(self, join: Any) -> None:
        """
        Parse a join configuration and build lineage based on its SQL.

        Args:
            join (Any): The join configuration object.

        Returns:
            None
        """
        if not join:
            return
        join_name = join.metaData.name
        if join_name in self.processed_join:
            return
        self.processed_join.add(join_name)

        # source tables used to build lineage
        sources = dict()

        # build select sql for left
        table, filter_expr, selects = self.parse_source(join.left)
        if table in self.staging_query_tables:
            sources[table] = self.staging_query_tables[table].query

        select_sql = self.build_select_sql(table, selects.items(), filter_expr)
        if not select_sql:
            return

        # build join sql
        join_sql = self.build_left_join_sql(join)

        # Build derivation SQL if derivations exist.
        derive_sql = self.build_join_derive_sql(join) if join.derivations else None

        output_table = self.object_table_name(join)
        output_columns = get_join_output_columns(join)
        output_columns = (
            output_columns[FeatureDisplayKeys.SOURCE_KEYS] + output_columns[FeatureDisplayKeys.OUTPUT_COLUMNS]
        )

        sql = join_sql.sql(dialect="spark", pretty=True)
        sources["select_table"] = select_sql.sql(dialect="spark", pretty=True)
        if derive_sql:
            sql = derive_sql.sql(dialect="spark", pretty=True)
            sources[("join_table")] = join_sql.sql(dialect="spark", pretty=True)

        parsed_lineages = build_lineage(output_table, output_columns, sql, sources)
        for output_column, input_columns in parsed_lineages.items():
            if not input_columns:
                self.unparsed_columns[join_name].append(output_column)
                continue
            self.store_lineage(output_column, input_columns)

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
        expression = maybe_parse(staging_query.query)
        try:
            qualify.qualify(
                expression,
                dialect="spark",
                schema=None,
                **{"validate_qualify_columns": False, "identify": False},
            )
        except Exception:
            print("here")

    def parse_group_by(self, gb: Any, extra: Optional[Set[Any]] = None) -> None:
        """
        Parse a group-by configuration and build lineage.

        Args:
            gb (Any): The group-by configuration object.
            extra (Optional[Set[Any]]): An extra parameter (unused, maintained for compatibility).

        Returns:
            None
        """
        if not gb:
            return
        gb_name = sanitize(gb.metaData.name)
        if gb_name in self.processed_group_by:
            return
        self.processed_group_by.add(gb_name)

        # source tables used to build lineage
        sources = {}

        # build select sql
        select_sqls = []
        for source in gb.sources:
            table, filter_expr, selects = self.parse_source(source)
            if table in self.staging_query_tables:
                sources[table] = self.staging_query_tables[table].query
            sql = self.build_select_sql(table, selects.items(), filter_expr)
            select_sqls.append(sql)

        select_sql = reduce(exp.union, select_sqls)

        # Build aggregation SQL.
        agg_columns = [("ds", "ds")]
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
            agg_columns.append((key_column, key_column))

        agg_sql = self.build_aggregate_sql("select_table", agg_columns)

        # Build derivation SQL if derivations exist.
        derive_sql = self.build_gb_derive_sql("transform_table", gb) if gb.derivations else None

        # build lineage
        output_table = self.object_table_name(gb)
        output_columns = get_group_by_output_columns(gb)

        sql = agg_sql.sql(pretty=True)
        sources["select_table"] = select_sql.sql(pretty=True)
        if derive_sql:
            sql = derive_sql.sql(pretty=True)
            sources["transform_table"] = agg_sql.sql(pretty=True)

        parsed_lineages = build_lineage(output_table, output_columns, sql, sources)
        for output_column, input_columns in parsed_lineages.items():
            if not input_columns:
                self.unparsed_columns[gb_name].append(output_column)
                continue
            self.store_lineage(output_column, input_columns)


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


def build_lineage(
    output_table: str, output_columns: List[str], sql: str, sources: Dict[str, str]
) -> Dict[str, Tuple[str, Tuple[str,]]]:
    """
    Build the lineage mapping from the SQL query and its source queries.

    Args:
        output_table (str): The fully qualified output table name.
        output_columns (List[str]): List of output column names.
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
    for output_column in output_columns:
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
