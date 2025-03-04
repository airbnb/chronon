import os
import unittest

from ai.chronon import group_by
from ai.chronon.api import ttypes
from ai.chronon.api import ttypes as api
from ai.chronon.group_by import Derivation
from ai.chronon.join import Join
from ai.chronon.lineage.lineage_parser import LineageParser

TEST_BASE_PATH = os.path.join(os.path.dirname(__file__), "../sample")


class TestParseJoin(unittest.TestCase):
    def setUp(self):
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

    def test_bootstrap_table(self):
        parser = LineageParser()
        parser.parse_join(self.join)

        self.assertEqual(
            {"subject", "ts", "event_id", "ds"},
            parser.metadata.tables["test_db.test_join_bootstrap"].columns,
        )

    def test_join_part_table(self):
        parser = LineageParser()
        parser.parse_join(self.join)

        self.assertEqual(
            {
                "cnt_count",
                "ds",
                "event_id_approx_percentile",
                "event_id_last_renamed",
                "event_id_sum",
                "event_id_sum_plus_one",
                "subject",
            },
            parser.metadata.tables["test_db.test_join_test_group_by"].columns,
        )

    def test_join_table(self):
        parser = LineageParser()
        parser.parse_join(self.join)

        self.assertEqual(
            {
                "ds",
                "event_id",
                "subject",
                "test_group_by_cnt_count",
                "test_group_by_event_id_approx_percentile",
                "test_group_by_event_id_last_renamed",
                "test_group_by_event_id_sum",
                "test_group_by_event_id_sum_plus_one",
            },
            parser.metadata.tables["test_db.test_join"].columns,
        )
