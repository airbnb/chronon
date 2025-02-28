import json
import logging
import os
import typing
from collections import defaultdict
from functools import reduce

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
    def __init__(self):
        # all lineages
        self.lineages = set()

        # columns can't determine lineage
        self.unparsed_columns = defaultdict(list)

        self.base_path = None
        self.team_conf = None

        # store staging_query table and sql
        self.staging_query_tables = dict()

        # processed group by names
        self.processed_group_by = set()

        # processed join names
        self.processed_join = set()

    def parse_lineage(self, base_path, entities=None):
        self.base_path = base_path
        teams_cofig = os.path.join(base_path, "teams.json")
        with open(teams_cofig) as team_infile:
            self.team_conf = json.load(team_infile)

        self.parse_staging_queries()
        # self.parse_group_bys(entities)
        self.parse_joins(entities)

    def parse_staging_queries(self):
        for root, dirs, files in os.walk(os.path.join(self.base_path, "production/staging_queries/")):
            if root.endswith("staging_queries/"):
                continue
            staging_queries = extract_json_confs(ttypes.StagingQuery, root)
            for staging_query in staging_queries:
                try:
                    self.handle_staging_query(staging_query)
                except Exception:
                    logger.error(f"failed to parse staging_query {staging_query.metaData.name}")

    def parse_group_bys(self, entities):
        for root, dirs, files in os.walk(os.path.join(self.base_path, "production/group_bys/")):
            if root.endswith("group_bys/"):
                continue
            all_group_bys = extract_json_confs(ttypes.GroupBy, root)
            for group_by in all_group_bys:
                if isinstance(group_by, ttypes.GroupBy):
                    if entities and group_by.metaData.name not in entities:
                        continue
                    try:
                        logger.info(f"handle group by {group_by.metaData.name}")
                        self.parse_group_by(group_by, set())
                    except Exception:
                        logger.error(f"failed to parse group by {group_by.metaData.name}")

    def parse_joins(self, entities):
        for root, dirs, files in os.walk(os.path.join(self.base_path, "production/joins/")):
            if root.endswith("joins/"):
                continue
            all_joins = extract_json_confs(ttypes.Join, root)
            for join in all_joins:
                if isinstance(join, ttypes.Join):
                    if entities and join.metaData.name not in entities:
                        continue
                    try:
                        self.parse_join(join, set())
                    except Exception:
                        print(f"failed to parse join {join.metaData.name}")

    def get_suffix(operation, argMap, window, bucket):
        suffix = _get_op_suffix(operation, argMap)
        if window:
            unit = ttypes.TimeUnit._VALUES_TO_NAMES[window.timeUnit].lower()[0]
            window_suffix = f"{window.length}{unit}"
            suffix = f"{suffix}_{window_suffix}"
        if bucket:
            suffix = f"{suffix}_by_{bucket}"
        return suffix

    def get_all_agg_exprs(self, aggregation):
        base_name = f"{_get_op_suffix(aggregation.operation, aggregation.argMap)}"
        windowed_names = []
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

    def build_select_sql(self, table, selects, filter_expr=None):
        # clean up added quotes
        selects = [
            (k.replace("`", "") if isinstance(k, str) else k, v.replace("`", "") if isinstance(v, str) else v)
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

    def build_aggregate_sql(self, table, selects, filter_expr=None):
        sql = (
            exp.Select(
                expressions=[f"SUM({v}) AS {k}" for k, v in selects],
            )
            .from_(table)
            .where(filter_expr)
        )
        return sql

    def build_gb_derive_sql(self, table, gb):
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

    def build_join_derive_sql(self, join):
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

    def store_lineage(self, output_column, input_columns, chronon_object):
        for input_column in input_columns:
            self.lineages.add((input_column, output_column))

    def base_table_name(self, table):
        return table.split("/")[0]

    def parse_source(self, source):
        if source.entities:
            table = self.base_table_name(source.entities.snapshotTable)
            wheres = source.entities.query.wheres or []
            filter_expr = " AND ".join([f"({where})" for where in sorted(wheres)])
            selects = source.entities.query.selects or {}
        elif source.events:
            table = self.base_table_name(source.events.table)
            wheres = source.events.query.wheres or []
            filter_expr = " AND ".join([f"({where})" for where in sorted(wheres)])
            selects = source.events.query.selects or {}
        else:
            table = self.object_table_name(source.joinSource.join)
            wheres = source.joinSource.query.wheres or []
            filter_expr = " AND ".join([f"({where})" for where in sorted(wheres)])
            selects = source.joinSource.query.selects or {}
            self.parse_join(source.joinSource.join)

        return table, filter_expr, selects

    def object_table_name(self, obj):
        if obj.metaData.outputNamespace:
            return output_table_name(obj, full_name=True)
        else:
            sanitized_table_name = sanitize(obj.metaData.name)
            if "namespace" in self.team_conf[obj.metaData.team]:
                db = self.team_conf[obj.metaData.team]["namespace"]
            else:
                db = self.team_conf["default"]["namespace"]
            return db + "." + sanitized_table_name

    def check_source_select_non_empty(self, source):
        if source.events:
            return source.events.query.selects
        elif source.entities:
            return source.entities.query.selects
        elif source.joinSource:
            return source.joinSource.query.selects

    def build_left_join_sql(self, join):
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
                            # verify select_key exists in left table columns, since there are mistyping that set the key mapping wrong
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

    def parse_join(self, join: ttypes.Join):
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

        # build derivation sql
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
            self.store_lineage(output_column, input_columns, join)

    def handle_staging_query(self, staging_query):
        table_name = output_table_name(staging_query, full_name=True)
        self.staging_query_tables[table_name] = staging_query

    def parse_group_by(self, gb):
        """
        Handle group by, and store lineage.
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

        # build aggregation sql
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

        # add key columns
        for key_column in gb.keyColumns:
            agg_columns.append((key_column, key_column))

        agg_sql = self.build_aggregate_sql("select_table", agg_columns)

        # build derive sql
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
            self.store_lineage(output_column, input_columns, gb)


def _get_col_node_name(node: Node) -> str:
    if isinstance(node.expression, exp.Table):
        # strip table alias from column if present
        real_column_name = node.name.split(".")[-1]

        # strip table alias from table exp if present
        table_exp = node.expression.copy()
        table_exp.args["alias"] = None
        return f"{table_exp.sql(pretty=False, identify=False)}.{real_column_name}".replace('"', "")
    return f"{node.name}"


def get_transform_operation(expression):
    if expression.this.__class__.__name__ == "Anonymous":
        return expression.this.alias_or_name
    elif expression.this.__class__.__name__ == "Column":
        return expression.__class__.__name__
    return expression.this.__class__.__name__


def build_lineage(output_table, output_columns, sql, sources):
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
    parsed_lineages = dict()
    scope = build_scope(expression)
    for output_column in output_columns:
        # don't use sqlglot.lineage since it needs to parse sql multiple times
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
