import os
import unittest

from ai.chronon import group_by
from ai.chronon.api import ttypes
from ai.chronon.group_by import Accuracy, Derivation
from ai.chronon.lineage.lineage_parser import LineageParser

TEST_BASE_PATH = os.path.join(os.path.dirname(__file__), "../sample")


class TestParseGroupBy(unittest.TestCase):
    def setUp(self):
        self.parser = LineageParser()

        gb_event_source = ttypes.EventSource(
            table="source.gb_table",
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

    def test_non_materialized(self):
        self.parser.parse_group_by(self.gb)
        self.assertFalse(self.parser.lineages)

    def test_backfill_table(self):
        self.gb.backfillStartDate = "2025-01-01"
        self.parser.parse_group_by(self.gb)
        self.assertEqual(
            {
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by.event_id_sum",
                ),
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by.event_id_approx_percentile",
                ),
                (
                    ("source.gb_table.event", ("Add", "Alias", "AGG", "Alias")),
                    "test_db.test_group_by.event_id_sum_plus_one",
                ),
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by.event_id_last_renamed",
                ),
                (
                    ("source.gb_table.subject", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by.subject",
                ),
            },
            self.parser.lineages,
        )

    def test_online_table(self):
        self.gb.metaData.online = True
        self.gb.accuracy = Accuracy.SNAPSHOT
        self.parser.parse_group_by(self.gb)
        self.assertEqual(
            {
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by_upload.event_id_approx_percentile",
                ),
                (
                    ("source.gb_table.subject", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by_upload.subject",
                ),
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by_upload.event_id_sum",
                ),
                (
                    ("source.gb_table.event", ("Add", "Alias", "AGG", "Alias")),
                    "test_db.test_group_by_upload.event_id_sum_plus_one",
                ),
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_db.test_group_by_upload.event_id_last_renamed",
                ),
            },
            self.parser.lineages,
        )

    def test_join_part_table(self):
        self.parser.parse_group_by(self.gb, join_part_table="test_join_jp_test_group_by")
        self.assertEqual(
            {
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_join_jp_test_group_by.event_id_sum",
                ),
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_join_jp_test_group_by.event_id_approx_percentile",
                ),
                (
                    ("source.gb_table.subject", ("Alias", "AGG", "Alias")),
                    "test_join_jp_test_group_by.subject",
                ),
                (
                    ("source.gb_table.event", ("Add", "Alias", "AGG", "Alias")),
                    "test_join_jp_test_group_by.event_id_sum_plus_one",
                ),
                (
                    ("source.gb_table.event", ("Alias", "AGG", "Alias")),
                    "test_join_jp_test_group_by.event_id_last_renamed",
                ),
            },
            self.parser.lineages,
        )
