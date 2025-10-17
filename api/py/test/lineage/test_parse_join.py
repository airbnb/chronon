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
from ai.chronon.join import DataType, ExternalPart, ExternalSource, Join
from ai.chronon.lineage.lineage_metadata import ConfigType, TableType
from ai.chronon.lineage.lineage_parser import LineageParser
from helper import compare_lineages


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
                Derivation(
                    name="test_group_by_event_id_last_renamed_plus_one_join",
                    expression="test_group_by_event_id_last_renamed + 1",
                ),
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
            {"subject"},
            parser.metadata.tables[bootstrap_table_name].key_columns,
        )
        self.assertEqual(
            {"subject", "ts", "event_id"},
            parser.metadata.tables[bootstrap_table_name].columns,
        )
        lineages = parser.metadata.filter_lineages(output_table=bootstrap_table_name)
        compare_lineages(
            self,
            {
                (
                    ("join_event_table", "ts"),
                    ("test_db.test_join_bootstrap", "ts"),
                    ("TryCast",),
                ),
                (
                    ("join_event_table", "event"),
                    ("test_db.test_join_bootstrap", "event_id"),
                    (),
                ),
                (
                    ("join_event_table", "subject"),
                    ("test_db.test_join_bootstrap", "subject"),
                    (),
                ),
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
        compare_lineages(
            self,
            {
                (
                    ("gb_table", "event"),
                    ("test_db.test_join_test_group_by", "event_id_sum_plus_one"),
                    ("AGG_SUM", "Add"),
                ),
                (
                    ("gb_table", "event"),
                    ("test_db.test_join_test_group_by", "event_id_approx_percentile"),
                    ("AGG_APPROX_PERCENTILE",),
                ),
                (
                    ("gb_table", "subject"),
                    ("test_db.test_join_test_group_by", "subject"),
                    (),
                ),
                (
                    ("gb_table", "event"),
                    ("test_db.test_join_test_group_by", "event_id_sum"),
                    ("AGG_SUM",),
                ),
                (
                    ("gb_table", "event"),
                    ("test_db.test_join_test_group_by", "event_id_last_renamed"),
                    ("AGG_LAST",),
                ),
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
            {"subject"},
            parser.metadata.tables[join_table_name].key_columns,
        )

        self.assertEqual(
            {
                "event_id",
                "test_group_by_event_id_last_renamed",
                "subject",
                "test_group_by_event_id_last_renamed_plus_one_join",
                "test_group_by_event_id_approx_percentile",
                "test_group_by_cnt_count",
                "test_group_by_event_id_sum",
                "ts",
                "test_group_by_event_id_sum_plus_one",
            },
            parser.metadata.tables[join_table_name].columns,
        )
        lineages = parser.metadata.filter_lineages(output_table=join_table_name)
        compare_lineages(
            self,
            {
                (
                    ("test_db.test_join_test_group_by", "event_id_sum"),
                    ("test_db.test_join", "test_group_by_event_id_sum"),
                    (),
                ),
                (
                    ("test_db.test_join_test_group_by", "cnt_count"),
                    ("test_db.test_join", "test_group_by_cnt_count"),
                    (),
                ),
                (
                    ("test_db.test_join_bootstrap", "event_id"),
                    ("test_db.test_join", "event_id"),
                    (),
                ),
                (
                    ("test_db.test_join_bootstrap", "ts"),
                    ("test_db.test_join", "ts"),
                    (),
                ),
                (
                    ("test_db.test_join_test_group_by", "event_id_approx_percentile"),
                    ("test_db.test_join", "test_group_by_event_id_approx_percentile"),
                    (),
                ),
                (
                    ("test_db.test_join_bootstrap", "subject"),
                    ("test_db.test_join", "subject"),
                    (),
                ),
                (
                    ("test_db.test_join_test_group_by", "event_id_last_renamed"),
                    (
                        "test_db.test_join",
                        "test_group_by_event_id_last_renamed",
                    ),
                    (),
                ),
                (
                    ("test_db.test_join_test_group_by", "event_id_last_renamed"),
                    (
                        "test_db.test_join",
                        "test_group_by_event_id_last_renamed_plus_one_join",
                    ),
                    ("Add",),
                ),
                (
                    ("test_db.test_join_test_group_by", "event_id_sum_plus_one"),
                    ("test_db.test_join", "test_group_by_event_id_sum_plus_one"),
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
                "test_join.test_group_by_cnt_count",
                "test_join.test_group_by_event_id_last_renamed_plus_one_join",
                "test_join.test_group_by_event_id_last_renamed",
                "test_join.test_group_by_event_id_approx_percentile",
                "test_join.test_group_by_event_id_sum_plus_one",
                "test_join.test_group_by_event_id_sum",
            },
            set(parser.metadata.features.keys()),
        )
        join_feature = parser.metadata.features["test_join.test_group_by_event_id_sum"]
        self.assertEqual("test_join", join_feature.config_name)
        self.assertEqual(ConfigType.JOIN, join_feature.config_type)
        self.assertEqual("test_db.test_join", join_feature.table_name)
        self.assertEqual("test_group_by_event_id_sum", join_feature.column_name)
        self.assertEqual("test_join.test_group_by_event_id_sum", join_feature.feature_name)

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

    def test_join_with_external_part_and_derivations(self):
        """Test that joins with external parts and derivations on external features work correctly.

        This test verifies the bug fix where:
        1. External features should not have lineage tracked
        2. Derivations that reference external features should also be treated as external (no lineage)
        3. Derivations that reference internal features should have lineage tracked
        """
        # Create a join with external parts and derivations on external features
        join_with_external = Join(
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
            online_external_parts=[
                ExternalPart(
                    ExternalSource(
                        name="test_external_source",
                        team="test_team",
                        key_fields=[("key", DataType.LONG)],
                        value_fields=[
                            ("value_str", DataType.STRING),
                            ("value_long", DataType.LONG),
                        ],
                    )
                )
            ],
            derivations=[
                Derivation(name="*", expression="*"),
                # Derivation referencing an external feature (simple rename)
                Derivation(name="external_value_renamed", expression="ext_test_external_source_value_str"),
                # Derivation with complex expression on external feature only
                Derivation(name="external_derived", expression="ext_test_external_source_value_long + 100"),
                # Derivation referencing an internal feature
                Derivation(
                    name="internal_derived",
                    expression="test_group_by_event_id_sum + 10",
                ),
            ],
        )
        join_with_external.metaData.name = "test_join_external"

        parser = LineageParser()
        parser.parse_join(join_with_external)

        join_table_name = "test_db.test_join_external"

        # Verify the table exists
        self.assertTrue(join_table_name in parser.metadata.tables)
        self.assertEqual(TableType.JOIN, parser.metadata.tables[join_table_name].table_type)

        # Verify columns include both internal and external features
        self.assertTrue("ext_test_external_source_value_str" in parser.metadata.tables[join_table_name].columns)
        self.assertTrue("ext_test_external_source_value_long" in parser.metadata.tables[join_table_name].columns)
        self.assertTrue("external_value_renamed" in parser.metadata.tables[join_table_name].columns)
        self.assertTrue("external_derived" in parser.metadata.tables[join_table_name].columns)
        self.assertTrue("internal_derived" in parser.metadata.tables[join_table_name].columns)

        # Get all features stored
        join_features = {k: v for k, v in parser.metadata.features.items() if k.startswith("test_join_external.")}

        # Verify external features are NOT in the features list (no lineage tracked)
        self.assertNotIn("test_join_external.ext_test_external_source_value_str", join_features)
        self.assertNotIn("test_join_external.ext_test_external_source_value_long", join_features)
        self.assertNotIn("test_join_external.external_value_renamed", join_features)
        self.assertNotIn("test_join_external.external_derived", join_features)

        # Verify internal features ARE in the features list (lineage tracked)
        self.assertIn("test_join_external.test_group_by_event_id_sum", join_features)
        self.assertIn("test_join_external.test_group_by_cnt_count", join_features)
        self.assertIn("test_join_external.internal_derived", join_features)

        # Verify lineage exists for internal features but not for external features
        lineages = parser.metadata.filter_lineages(output_table=join_table_name)
        lineage_output_columns = {lineage.output_column for lineage in lineages}

        # External feature columns should not have lineage
        self.assertNotIn("ext_test_external_source_value_str", lineage_output_columns)
        self.assertNotIn("ext_test_external_source_value_long", lineage_output_columns)
        self.assertNotIn("external_value_renamed", lineage_output_columns)
        self.assertNotIn("external_derived", lineage_output_columns)

        # Internal feature columns should have lineage
        self.assertIn("test_group_by_event_id_sum", lineage_output_columns)
        self.assertIn("test_group_by_cnt_count", lineage_output_columns)
        self.assertIn("internal_derived", lineage_output_columns)
