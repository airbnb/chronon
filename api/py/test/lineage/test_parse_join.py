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
from ai.chronon.api import ttypes as api
from ai.chronon.group_by import Derivation
from ai.chronon.join import Join
from ai.chronon.lineage.lineage_metadata import ColumnTransform, TableType
from ai.chronon.lineage.lineage_parser import LineageParser


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
                            "subject": "subject",
                            "event_id": "event",
                        },
                        timeColumn="CAST(ts AS DOUBLE)",
                    ),
                ),
            ),
            output_namespace="test_db",
            right_parts=[api.JoinPart(self.gb)],
            derivations=[
                Derivation(name="*", expression="*"),
                Derivation(name="event_id_last_plus_one_join", expression="event_id_last + 1"),
            ],
        )
        self.join.metaData.name = "test_join"

    def test_bootstrap_table(self):
        parser = LineageParser()
        parser.parse_join(self.join)

        bootstrap_table_name = "test_db.test_join_bootstrap"
        self.assertTrue(bootstrap_table_name in parser.metadata.tables)
        self.assertEqual(
            TableType.JOIN_BOOTSTRAP,
            parser.metadata.tables[bootstrap_table_name].table_type,
        )
        self.assertEqual(
            {"event_id", "subject", "ts"},
            parser.metadata.tables[bootstrap_table_name].key_columns,
        )
        self.assertEqual(
            {"subject", "ts", "event_id"},
            parser.metadata.tables[bootstrap_table_name].columns,
        )
        lineages = parser.metadata.filter_lineages(output_table=bootstrap_table_name)
        self.compare_lineages(
            {
                ("join_event_table.ts", "test_db.test_join_bootstrap.ts", ("TryCast",)),
                ("join_event_table.event", "test_db.test_join_bootstrap.event_id", ()),
                ("join_event_table.subject", "test_db.test_join_bootstrap.subject", ()),
            },
            lineages,
        )

    def test_join_part_table(self):
        parser = LineageParser()
        parser.parse_join(self.join)

        join_part_table_name = "test_db.test_join_test_group_by"
        self.assertTrue(join_part_table_name in parser.metadata.tables)
        self.assertEqual(TableType.JOIN_PART, parser.metadata.tables[join_part_table_name].table_type)
        self.assertEqual({"subject"}, parser.metadata.tables[join_part_table_name].key_columns)

        self.assertEqual(
            {
                "cnt_count",
                "event_id_approx_percentile",
                "event_id_last_renamed",
                "event_id_sum",
                "event_id_sum_plus_one",
                "subject",
            },
            parser.metadata.tables[join_part_table_name].columns,
        )

        lineages = parser.metadata.filter_lineages(output_table=join_part_table_name)
        self.compare_lineages(
            {
                ("gb_table.event", "test_db.test_join_test_group_by.event_id_sum_plus_one", ("Add", "AGG_SUM")),
                (
                    "gb_table.event",
                    "test_db.test_join_test_group_by.event_id_approx_percentile",
                    ("AGG_APPROX_PERCENTILE",),
                ),
                ("gb_table.subject", "test_db.test_join_test_group_by.subject", ()),
                ("gb_table.event", "test_db.test_join_test_group_by.event_id_sum", ("AGG_SUM",)),
                ("gb_table.event", "test_db.test_join_test_group_by.event_id_last_renamed", ("AGG_LAST",)),
            },
            lineages,
        )

    def test_join_table(self):
        parser = LineageParser()
        parser.parse_join(self.join)

        join_table_name = "test_db.test_join"
        self.assertTrue(join_table_name in parser.metadata.tables)
        self.assertEqual(TableType.JOIN, parser.metadata.tables[join_table_name].table_type)
        self.assertEqual(
            {"event_id", "subject", "ts"},
            parser.metadata.tables[join_table_name].key_columns,
        )

        self.assertEqual(
            {
                "event_id",
                "event_id_last_plus_one_join",
                "subject",
                "test_group_by_cnt_count",
                "test_group_by_event_id_approx_percentile",
                "test_group_by_event_id_last_renamed",
                "test_group_by_event_id_sum",
                "test_group_by_event_id_sum_plus_one",
                "ts",
            },
            parser.metadata.tables[join_table_name].columns,
        )
        lineages = parser.metadata.filter_lineages(output_table=join_table_name)
        self.compare_lineages(
            {
                (
                    "test_db.test_join_test_group_by.event_id_sum",
                    "test_db.test_join.test_group_by_event_id_sum",
                    (),
                ),
                (
                    "test_db.test_join_test_group_by.cnt_count",
                    "test_db.test_join.test_group_by_cnt_count",
                    (),
                ),
                (
                    "test_db.test_join_bootstrap.event_id",
                    "test_db.test_join.event_id",
                    (),
                ),
                ("test_db.test_join_bootstrap.ts", "test_db.test_join.ts", ()),
                (
                    "test_db.test_join_test_group_by.event_id_approx_percentile",
                    "test_db.test_join.test_group_by_event_id_approx_percentile",
                    (),
                ),
                (
                    "test_db.test_join_test_group_by.subject",
                    "test_db.test_join.subject",
                    (),
                ),
                (
                    "test_db.test_join_test_group_by.event_id_last_renamed",
                    "test_db.test_join.test_group_by_event_id_last_renamed",
                    (),
                ),
                (
                    "test_db.test_join_test_group_by.event_id_sum_plus_one",
                    "test_db.test_join.test_group_by_event_id_sum_plus_one",
                    (),
                ),
            },
            lineages,
        )

    def test_parse_features(self):
        parser = LineageParser()
        parser.parse_join(self.join)
        self.assertEqual(
            {
                "test_group_by.event_id_approx_percentile",
                "test_join.test_group_by_event_id_approx_percentile",
                "test_group_by.event_id_sum_plus_one",
                "test_group_by.event_id_sum",
                "test_group_by.cnt_count",
                "test_join.test_group_by_event_id_sum_plus_one",
                "test_group_by.event_id_last",
                "test_join.test_group_by_event_id_sum",
                "test_join.event_id_last_plus_one_join",
                "test_join.test_group_by_cnt_count",
                "test_group_by.event_id_last_renamed",
                "test_join.test_group_by_event_id_last_renamed",
            },
            set(parser.metadata.features.keys()),
        )

    def test_build_join_sql(self):
        expected_sql_path = os.path.join(os.path.dirname(__file__), "join_sqls/join.sql")
        parser = LineageParser()
        actual_sql = parser.build_join_sql(self.join).sql(dialect="spark", pretty=True)
        with open(expected_sql_path, "r") as infile:
            expected_sql = infile.read()
            self.assertEqual(expected_sql, actual_sql)

    def test_build_derive_sql(self):
        expected_sql_path = os.path.join(os.path.dirname(__file__), "join_sqls/derive.sql")
        actual_sql = LineageParser.build_join_derive_sql("derive_table", self.join).sql(dialect="spark", pretty=True)
        with open(expected_sql_path, "r") as infile:
            expected_sql = infile.read()
            self.assertEqual(expected_sql, actual_sql)

    def compare_lineages(self, expected, actual):
        expected = sorted(expected)
        actual = sorted(actual, key=lambda t: (t.input_column, t.output_column, t.transforms))
        self.assertEqual(len(actual), len(expected))
        for lineage_expected, lineage_actual in zip(expected, actual):
            self.assertEqual(
                ColumnTransform(lineage_expected[0], lineage_expected[1], lineage_expected[2]), lineage_actual
            )
