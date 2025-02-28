import os
import unittest

from ai.chronon import group_by
from ai.chronon.api import ttypes
from ai.chronon.api import ttypes as api
from ai.chronon.group_by import Derivation
from ai.chronon.join import Join
from ai.chronon.lineage.lineage_parser import LineageParser, build_lineage

TEST_BASE_PATH = os.path.join(os.path.dirname(__file__), "sample")


class TestLineageParser(unittest.TestCase):
    def setUp(self):
        self.parser = LineageParser()

        gb_event_source = ttypes.EventSource(
            table="gb_table",
            topic=None,
            query=ttypes.Query(
                startPartition="2024-01-01",
                selects={"subject": "subject", "event_id": "event", "cnt": 1},
                timeColumn="CAST(ts AS DOUBLE)",
            ),
        )

        self.gb = group_by.GroupBy(
            name="test_group_by",
            sources=gb_event_source,
            keys=["subject"],
            output_namespace="test_db",
            aggregations=group_by.Aggregations(
                random=ttypes.Aggregation(inputColumn="event_id", operation=ttypes.Operation.SUM),
                event_id=ttypes.Aggregation(operation=ttypes.Operation.LAST),
                cnt=ttypes.Aggregation(operation=ttypes.Operation.COUNT),
                percentile=group_by.Aggregation(
                    input_column="event_id",
                    operation=group_by.Operation.APPROX_PERCENTILE([0.5, 0.75]),
                ),
            ),
            derivations=[
                Derivation(name="*", expression="*"),
                Derivation(name="event_id_sum_plus_one", expression="event_id_sum + 1"),
                Derivation(name="event_id_last_renamed", expression="event_id_last"),
            ],
        )

        self.join = Join(
            left=api.Source(
                events=api.EventSource(
                    table="join_event_table",
                    query=api.Query(
                        startPartition="2020-04-09",
                        selects={
                            "subject": "subject_sql",
                            "event_id": "event_sql",
                        },
                        timeColumn="CAST(ts AS DOUBLE)",
                    ),
                ),
            ),
            output_namespace="test_db",
            right_parts=[api.JoinPart(self.gb)],
        )
        self.join.metaData.name = "test_join"

    def test_parse_group_by(self):
        self.parser.parse_group_by(self.gb)
        self.assertEqual(
            {
                (
                    ("gb_table.event", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_sum",
                ),
                (
                    ("gb_table.event", ("Add", "Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_sum_plus_one",
                ),
                (
                    ("gb_table.event", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_last_renamed",
                ),
                (
                    ("gb_table.subject", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.subject",
                ),
                (
                    ("gb_table.event", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_approx_percentile",
                ),
            },
            self.parser.lineages,
        )

    def test_parse_join(self):
        self.parser.parse_join(self.join)
        group_by_outputs = {to_ for from_, to_ in self.parser.lineages if "test_db.test_group_by" in to_}
        self.assertEqual(
            {
                "test_db.test_group_by.event_id_last_renamed",
                "test_db.test_group_by.event_id_sum",
                "test_db.test_group_by.subject",
                "test_db.test_group_by.event_id_approx_percentile",
                "test_db.test_group_by.event_id_sum_plus_one",
            },
            set(group_by_outputs),
        )

        join_outputs = {to_ for from_, to_ in self.parser.lineages if "test_db.test_join" in to_}
        self.assertEqual(
            {
                "test_db.test_join.test_group_by_event_id_sum",
                "test_db.test_join.test_group_by_event_id_sum_plus_one",
                "test_db.test_join.event_id",
                "test_db.test_join.ts",
                "test_db.test_join.test_group_by_cnt_count",
                "test_db.test_join.test_group_by_event_id_last_renamed",
                "test_db.test_join.test_group_by_event_id_approx_percentile",
            },
            join_outputs,
        )
        self.assertEqual(
            {
                (
                    ("gb_table.event", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_last_renamed",
                ),
                (
                    ("test_db.test_group_by.test_group_by_event_id_sum", ("Alias",)),
                    "test_db.test_join.test_group_by_event_id_sum",
                ),
                (
                    (
                        "test_db.test_group_by.test_group_by_event_id_sum_plus_one",
                        ("Alias",),
                    ),
                    "test_db.test_join.test_group_by_event_id_sum_plus_one",
                ),
                (
                    ("join_event_table.event_sql", ("Alias",)),
                    "test_db.test_join.event_id",
                ),
                (
                    ("gb_table.event", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_sum",
                ),
                (("join_event_table.ts", ("Alias", "TryCast")), "test_db.test_join.ts"),
                (
                    ("gb_table.subject", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.subject",
                ),
                (
                    ("test_db.test_group_by.test_group_by_cnt_count", ("Alias",)),
                    "test_db.test_join.test_group_by_cnt_count",
                ),
                (
                    ("gb_table.event", ("Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_approx_percentile",
                ),
                (
                    ("gb_table.event", ("Add", "Alias", "Sum", "Alias")),
                    "test_db.test_group_by.event_id_sum_plus_one",
                ),
                (
                    (
                        "test_db.test_group_by.test_group_by_event_id_last_renamed",
                        ("Alias",),
                    ),
                    "test_db.test_join.test_group_by_event_id_last_renamed",
                ),
                (
                    (
                        "test_db.test_group_by.test_group_by_event_id_approx_percentile",
                        ("Alias",),
                    ),
                    "test_db.test_join.test_group_by_event_id_approx_percentile",
                ),
            },
            self.parser.lineages,
        )

    def test_parse_lineage(self):
        # self.parser.parse_lineage("/Users/xiaohui_sun/work/ml_models/zipline", entities=['zipline_test.iceberg_realtime.v0'])
        self.parser.parse_lineage(TEST_BASE_PATH)
        self.assertIsNotNone(self.parser.lineages)

    def test_no_lineage(self):
        # Can't parse lineage since there is no specific column.
        lineage = build_lineage("output", ["a"], "SELECT COUNT(*) AS a FROM input", sources={})
        self.assertEqual(
            {"output.a": []},
            lineage,
        )

    def test_parse_operations(self):
        output_table = "output_table"
        output_columns = ["a", "b", "c", "d"]
        sql = """
            SELECT
             f AS a,
             e + f AS b,
             AGG(g) AS c,
             SUM(h) AS d
            FROM (
                SELECT e, f, g, h FROM input_table
            )
        """

        lineage = build_lineage(output_table, output_columns, sql, sources={})
        self.assertEqual(
            {
                "output_table.a": [("input_table.f", ("Alias",))],
                "output_table.b": [
                    ("input_table.f", ("Add", "Alias")),
                    ("input_table.e", ("Add", "Alias")),
                ],
                "output_table.c": [("input_table.g", ("AGG", "Alias"))],
                "output_table.d": [("input_table.h", ("Sum", "Alias"))],
            },
            lineage,
        )
