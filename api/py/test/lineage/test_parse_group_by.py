import os
import unittest

from ai.chronon import group_by
from ai.chronon.api import ttypes
from ai.chronon.group_by import Accuracy, Derivation
from ai.chronon.lineage.lineage_parser import LineageParser

TEST_BASE_PATH = os.path.join(os.path.dirname(__file__), "../sample")


class TestParseGroupBy(unittest.TestCase):
    def setUp(self):
        gb_event_source = ttypes.EventSource(
            table="source.gb_table",
            topic=None,
            query=ttypes.Query(
                startPartition="2024-01-01",
                selects={"subject": "subject", "event_id": "event", "cnt": 1},
                timeColumn="CAST(ts AS DOUBLE)",
            ),
        )

        gb_event_source1 = ttypes.EventSource(
            table="source.gb_table1",
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

        self.gb_multiple_source = group_by.GroupBy(
            name="test_group_by_multiple_source",
            sources=[gb_event_source, gb_event_source1],
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
        )

    def test_non_materialized(self):
        parser = LineageParser()
        parser.parse_group_by(self.gb)
        self.assertFalse(parser.metadata.lineages)
        # only features are extracted, but no lineage
        self.assertEqual(
            {
                "test_group_by_cnt_count",
                "test_group_by_event_id_approx_percentile",
                "test_group_by_event_id_last",
                "test_group_by_event_id_sum",
            },
            set(parser.metadata.features.keys()),
        )

    def test_backfill_table(self):
        self.gb.backfillStartDate = "2025-01-01"
        parser = LineageParser()
        parser.parse_group_by(self.gb)
        self.assertEqual(
            {
                ("source.gb_table.subject", "test_db.test_group_by.subject", ""),
                ("source.gb_table.event", "test_db.test_group_by.event_id_sum", "AGG"),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by.event_id_last_renamed",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by.event_id_sum_plus_one",
                    "Add,AGG",
                ),
            },
            parser.metadata.lineages,
        )

    def test_online_table(self):
        self.gb.metaData.online = True
        self.gb.accuracy = Accuracy.SNAPSHOT
        parser = LineageParser()
        parser.parse_group_by(self.gb)
        self.assertEqual(
            {
                ("source.gb_table.subject", "test_db.test_group_by_upload.subject", ""),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_upload.event_id_sum_plus_one",
                    "Add,AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_upload.event_id_last_renamed",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_upload.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_upload.event_id_sum",
                    "AGG",
                ),
            },
            parser.metadata.lineages,
        )

    def test_join_part_table(self):
        parser = LineageParser()
        parser.parse_group_by(self.gb, join_part_table="test_join_jp_test_group_by")
        self.assertEqual(
            {
                (
                    "source.gb_table.event",
                    "test_join_jp_test_group_by.event_id_sum",
                    "AGG",
                ),
                ("source.gb_table.subject", "test_join_jp_test_group_by.subject", ""),
                (
                    "source.gb_table.event",
                    "test_join_jp_test_group_by.event_id_last_renamed",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_join_jp_test_group_by.event_id_sum_plus_one",
                    "Add,AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_join_jp_test_group_by.event_id_approx_percentile",
                    "AGG",
                ),
            },
            parser.metadata.lineages,
        )

    def test_multiple_sources(self):
        self.gb_multiple_source.backfillStartDate = "2025-01-01"
        parser = LineageParser()
        parser.parse_group_by(self.gb_multiple_source)
        self.assertEqual(
            {
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source.event_id_last",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source.event_id_sum",
                    "AGG",
                ),
                (
                    "source.gb_table.subject",
                    "test_db.test_group_by_multiple_source.subject",
                    "",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source.event_id_last",
                    "AGG",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source.event_id_sum",
                    "AGG",
                ),
                (
                    "source.gb_table1.subject",
                    "test_db.test_group_by_multiple_source.subject",
                    "",
                ),
            },
            parser.metadata.lineages,
        )
