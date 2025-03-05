import unittest

from ai.chronon import group_by
from ai.chronon.api import ttypes
from ai.chronon.group_by import Accuracy, Derivation
from ai.chronon.lineage.lineage_metadata import TableType
from ai.chronon.lineage.lineage_parser import LineageParser


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
            online=True,
            sources=[gb_event_source, gb_event_source1],
            keys=["subject"],
            backfill_start_date="2025-01-01",
            output_namespace="test_db",
            accuracy=Accuracy.SNAPSHOT,
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
                "test_group_by.cnt_count",
                "test_group_by.event_id_approx_percentile",
                "test_group_by.event_id_last",
                "test_group_by.event_id_last_renamed",
                "test_group_by.event_id_sum",
                "test_group_by.event_id_sum_plus_one",
            },
            set(parser.metadata.features.keys()),
        )

    def test_backfill_table(self):
        parser = LineageParser()
        parser.parse_group_by(self.gb_multiple_source)
        self.assertEqual(
            {
                (
                    "source.gb_table1.subject",
                    "test_db.test_group_by_multiple_source_upload.subject",
                    "",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source_upload.event_id_sum",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source.event_id_last",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source_upload.event_id_last",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source_upload.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source_upload.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source_upload.event_id_sum",
                    "AGG",
                ),
                (
                    "source.gb_table1.subject",
                    "test_db.test_group_by_multiple_source.subject",
                    "",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source.event_id_last",
                    "AGG",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source.event_id_sum",
                    "AGG",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source.event_id_approx_percentile",
                    "AGG",
                ),
                (
                    "source.gb_table.subject",
                    "test_db.test_group_by_multiple_source.subject",
                    "",
                ),
                (
                    "source.gb_table.event",
                    "test_db.test_group_by_multiple_source.event_id_sum",
                    "AGG",
                ),
                (
                    "source.gb_table1.event",
                    "test_db.test_group_by_multiple_source_upload.event_id_last",
                    "AGG",
                ),
                (
                    "source.gb_table.subject",
                    "test_db.test_group_by_multiple_source_upload.subject",
                    "",
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
                    "AGG,Add",
                ),
                (
                    "source.gb_table.event",
                    "test_join_jp_test_group_by.event_id_approx_percentile",
                    "AGG",
                ),
            },
            parser.metadata.lineages,
        )

    def test_parse_features(self):
        parser = LineageParser()
        parser.parse_group_by(self.gb_multiple_source)
        self.assertEqual(
            {
                "test_group_by_multiple_source.cnt_count",
                "test_group_by_multiple_source.event_id_approx_percentile",
                "test_group_by_multiple_source.event_id_last",
                "test_group_by_multiple_source.event_id_sum",
            },
            set(parser.metadata.features.keys()),
        )

        # parse group by with derived features
        parser = LineageParser()
        parser.parse_group_by(self.gb)
        self.assertEqual(
            {
                "test_group_by.cnt_count",
                "test_group_by.event_id_approx_percentile",
                "test_group_by.event_id_last",
                "test_group_by.event_id_last_renamed",
                "test_group_by.event_id_sum",
                "test_group_by.event_id_sum_plus_one",
            },
            set(parser.metadata.features.keys()),
        )

    def test_parse_tables(self):
        parser = LineageParser()
        parser.parse_group_by(self.gb_multiple_source)
        self.assertEqual(
            {
                "source.gb_table",
                "source.gb_table1",
                "test_db.test_group_by_multiple_source",
                "test_db.test_group_by_multiple_source_upload",
            },
            set(parser.metadata.tables.keys()),
        )
        self.assertEqual(
            TableType.GROUP_BY_BACKFILL, parser.metadata.tables["test_db.test_group_by_multiple_source"].table_type
        )
        self.assertEqual(
            TableType.GROUP_BY_UPLOAD, parser.metadata.tables["test_db.test_group_by_multiple_source_upload"].table_type
        )
        self.assertEqual({"ds", "subject"}, parser.metadata.tables["test_db.test_group_by_multiple_source"].key_columns)
