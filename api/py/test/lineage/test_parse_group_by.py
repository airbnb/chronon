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

import os
import unittest

from ai.chronon import group_by
from ai.chronon.api import ttypes
from ai.chronon.group_by import Accuracy, Derivation
from ai.chronon.lineage.lineage_metadata import ConfigType, TableType
from ai.chronon.lineage.lineage_parser import LineageParser
from helper import compare_lineages


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
                partitionColumn="ds",
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
                "test_group_by.event_id_last_renamed",
                "test_group_by.event_id_sum",
                "test_group_by.event_id_sum_plus_one",
            },
            set(parser.metadata.features.keys()),
        )

    def test_backfill_table(self):
        parser = LineageParser()
        parser.parse_group_by(self.gb_multiple_source)

        backfill_table_name = "test_db.test_group_by_multiple_source"
        self.assertTrue(backfill_table_name in parser.metadata.tables)
        self.assertEqual(
            TableType.GROUP_BY_BACKFILL,
            parser.metadata.tables[backfill_table_name].table_type,
        )
        self.assertEqual({"subject"}, parser.metadata.tables[backfill_table_name].key_columns)
        self.assertEqual(
            {
                "cnt_count",
                "event_id_approx_percentile",
                "event_id_last",
                "event_id_sum",
                "subject",
            },
            parser.metadata.tables[backfill_table_name].columns,
        )
        lineages = parser.metadata.filter_lineages(output_table=backfill_table_name)

        compare_lineages(
            self,
            {
                (
                    ("source.gb_table1", "event"),
                    (
                        "test_db.test_group_by_multiple_source",
                        "event_id_approx_percentile",
                    ),
                    ("AGG_APPROX_PERCENTILE",),
                ),
                (
                    ("source.gb_table1", "event"),
                    ("test_db.test_group_by_multiple_source", "event_id_sum"),
                    ("AGG_SUM",),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_group_by_multiple_source", "event_id_last"),
                    ("AGG_LAST",),
                ),
                (
                    ("source.gb_table1", "subject"),
                    ("test_db.test_group_by_multiple_source", "subject"),
                    (),
                ),
                (
                    ("source.gb_table", "subject"),
                    ("test_db.test_group_by_multiple_source", "subject"),
                    (),
                ),
                (
                    ("source.gb_table", "event"),
                    (
                        "test_db.test_group_by_multiple_source",
                        "event_id_approx_percentile",
                    ),
                    ("AGG_APPROX_PERCENTILE",),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_group_by_multiple_source", "event_id_sum"),
                    ("AGG_SUM",),
                ),
                (
                    ("source.gb_table1", "event"),
                    ("test_db.test_group_by_multiple_source", "event_id_last"),
                    ("AGG_LAST",),
                ),
            },
            lineages,
        )

    def test_join_part_table(self):
        parser = LineageParser()
        parser.parse_group_by(
            self.gb,
            join_part_table="test_db.test_join_jp_test_group_by",
            join_config_name="",
        )

        backfill_table_name = "test_db.test_join_jp_test_group_by"
        self.assertTrue(backfill_table_name in parser.metadata.tables)
        self.assertEqual(
            TableType.JOIN_PART,
            parser.metadata.tables[backfill_table_name].table_type,
        )
        self.assertEqual({"subject"}, parser.metadata.tables[backfill_table_name].key_columns)
        self.assertEqual(
            {
                "cnt_count",
                "event_id_approx_percentile",
                "event_id_last_renamed",
                "event_id_sum",
                "event_id_sum_plus_one",
                "subject",
            },
            parser.metadata.tables[backfill_table_name].columns,
        )
        lineages = parser.metadata.filter_lineages(output_table=backfill_table_name)
        compare_lineages(
            self,
            {
                (
                    ("source.gb_table", "subject"),
                    ("test_db.test_join_jp_test_group_by", "subject"),
                    (),
                ),
                (
                    ("source.gb_table", "event"),
                    (
                        "test_db.test_join_jp_test_group_by",
                        "event_id_approx_percentile",
                    ),
                    ("AGG_APPROX_PERCENTILE",),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_join_jp_test_group_by", "event_id_sum_plus_one"),
                    ("Add", "AGG_SUM"),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_join_jp_test_group_by", "event_id_last_renamed"),
                    ("AGG_LAST",),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_join_jp_test_group_by", "event_id_sum"),
                    ("AGG_SUM",),
                ),
            },
            lineages,
        )

    def test_upload_table(self):
        parser = LineageParser()
        parser.parse_group_by(self.gb_multiple_source)

        upload_table_name = "test_db.test_group_by_multiple_source_upload"
        self.assertTrue(upload_table_name in parser.metadata.tables)
        self.assertEqual(
            TableType.GROUP_BY_UPLOAD,
            parser.metadata.tables[upload_table_name].table_type,
        )
        self.assertEqual({"key_json"}, parser.metadata.tables[upload_table_name].key_columns)
        self.assertEqual(
            {
                "key_json",
                "value_json",
            },
            parser.metadata.tables[upload_table_name].columns,
        )
        lineages = parser.metadata.filter_lineages(output_table=upload_table_name)
        compare_lineages(
            self,
            {
                (
                    ("source.gb_table1", "event"),
                    ("test_db.test_group_by_multiple_source_upload", "value_json"),
                    ("AGG_LAST",),
                ),
                (
                    ("source.gb_table", "subject"),
                    ("test_db.test_group_by_multiple_source_upload", "key_json"),
                    (),
                ),
                (
                    ("source.gb_table1", "event"),
                    ("test_db.test_group_by_multiple_source_upload", "value_json"),
                    ("AGG_APPROX_PERCENTILE",),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_group_by_multiple_source_upload", "value_json"),
                    ("AGG_SUM",),
                ),
                (
                    ("source.gb_table1", "subject"),
                    ("test_db.test_group_by_multiple_source_upload", "key_json"),
                    (),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_group_by_multiple_source_upload", "value_json"),
                    ("AGG_LAST",),
                ),
                (
                    ("source.gb_table", "event"),
                    ("test_db.test_group_by_multiple_source_upload", "value_json"),
                    ("AGG_APPROX_PERCENTILE",),
                ),
                (
                    ("source.gb_table1", "event"),
                    ("test_db.test_group_by_multiple_source_upload", "value_json"),
                    ("AGG_SUM",),
                ),
            },
            lineages,
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

        group_by_feature = parser.metadata.features["test_group_by_multiple_source.event_id_last"]
        self.assertEqual("test_group_by_multiple_source", group_by_feature.config_name)
        self.assertEqual(ConfigType.GROUP_BY, group_by_feature.config_type)
        self.assertEqual("test_db.test_group_by_multiple_source", group_by_feature.table_name)
        self.assertEqual("event_id_last", group_by_feature.column_name)
        self.assertEqual("test_group_by_multiple_source.event_id_last", group_by_feature.feature_name)

        # parse group by with derived features
        parser = LineageParser()
        parser.parse_group_by(self.gb)
        self.assertEqual(
            {
                "test_group_by.cnt_count",
                "test_group_by.event_id_approx_percentile",
                "test_group_by.event_id_last_renamed",
                "test_group_by.event_id_sum",
                "test_group_by.event_id_sum_plus_one",
            },
            set(parser.metadata.features.keys()),
        )

    def test_build_aggregate_sql(self):
        parser = LineageParser()
        actual_sql = parser.build_aggregate_sql("agg_table", self.gb).sql(dialect="spark", pretty=True)
        expected_sql_path = os.path.join(os.path.dirname(__file__), "group_by_sqls/aggregate.sql")
        with open(expected_sql_path, "r") as infile:
            expected_sql = infile.read()
            self.assertEqual(expected_sql, actual_sql)

    def test_build_derive_sql(self):
        expected_sql_path = os.path.join(os.path.dirname(__file__), "group_by_sqls/derive.sql")
        actual_sql = LineageParser.build_gb_derive_sql("derive_table", self.gb).sql(dialect="spark", pretty=True)
        with open(expected_sql_path, "r") as infile:
            expected_sql = infile.read()
            self.assertEqual(expected_sql, actual_sql)
