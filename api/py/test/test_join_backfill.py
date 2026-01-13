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

import unittest

from ai.chronon.repo.join_backfill import JoinBackfill, get_regular_and_external_join_parts
from ai.chronon.repo.external_join_part import ExternalJoinPart
from ai.chronon.utils import convert_json_to_obj, join_part_name


class TestJoinBackfill(unittest.TestCase):
    def setUp(self):
        conf_file = "api/py/test/sample/production/joins/sample_team/sample_join.v1"
        self.join_backfill = JoinBackfill(config_path=conf_file, start_date="2025-01-01", end_date="2025-01-01")

        conf_file = "api/py/test/sample/production/joins/sample_team/sample_chaining_join.v1"
        self.channing_join_backfill = JoinBackfill(
            config_path=conf_file, start_date="2025-01-01", end_date="2025-01-01"
        )

        conf_file = "api/py/test/sample/production/joins/sample_team/sample_join.v1"
        settings = {
            "default": {
                "spark_version": "3.1.1",
                "executor_memory": "4G",
                "driver_memory": "4G",
                "executor_cores": 2,
            },
            "left_table": {
                "executor_memory": "4G",
                "driver_memory": "6G",
                "executor_cores": 2,
            },
            "final_join": {
                "driver_memory": "9G",
            },
            "sample_team_sample_group_by_v1": {
                "executor_memory": "2G",
            },
        }
        self.join_backfill_override = JoinBackfill(
            config_path=conf_file, start_date="2025-01-01", end_date="2025-01-01", settings=settings
        )

    @staticmethod
    def _get_node(nodes, name):
        nodes = [node for node in nodes if node.name == name]
        assert len(nodes) == 1
        return nodes[0]

    def test_flow_nodes(self):
        flow = self.join_backfill.build_flow()
        self.assertEqual("sample_team.sample_join.v1", flow.name)
        self.assertEqual(
            {
                "compute_join__sample_namespace_sample_team_sample_join_v1",
                "compute_join__sample_team_sample_join_v1__left_table",
                "compute_join__sample_team_sample_join_v1__sample_team_sample_group_by_v1",
            },
            {node.name for node in flow.nodes},
        )

    def test_channing_flow_nodes(self):
        flow = self.channing_join_backfill.build_flow()
        self.assertEqual(
            {
                "compute_join__sample_team_sample_chaining_join_parent_join__"
                "sample_team_entity_sample_group_by_from_module_v1",
                "compute_join__sample_team_sample_chaining_join_parent_join__left_table",
                "compute_join__chronon_db_sample_team_sample_chaining_join_v1",
                "compute_join__sample_team_sample_chaining_join_v1__sample_team_sample_chaining_group_by",
                "compute_join__sample_team_sample_chaining_join_parent_join__" "sample_team_event_sample_group_by_v1",
                "compute_join__chronon_db_sample_team_sample_chaining_join_parent_join",
                "compute_join__sample_team_sample_chaining_join_v1__left_table",
            },
            {node.name for node in flow.nodes},
        )

    def test_settings_default(self):
        flow = self.channing_join_backfill.build_flow()
        join_node = self._get_node(flow.nodes, "compute_join__chronon_db_sample_team_sample_chaining_join_v1")
        self.assertEqual(
            {"driver_memory": "4G", "executor_cores": 2, "executor_memory": "4G", "spark_version": "3.1.1"},
            join_node.settings,
        )

    def test_settings_override(self):
        flow = self.join_backfill_override.build_flow()
        group_by_node = self._get_node(
            flow.nodes, "compute_join__sample_team_sample_join_v1__sample_team_sample_group_by_v1"
        )
        self.assertEqual(
            {"driver_memory": "4G", "executor_cores": 2, "executor_memory": "2G", "spark_version": "3.1.1"},
            group_by_node.settings,
        )

        join_node = self._get_node(flow.nodes, "compute_join__sample_namespace_sample_team_sample_join_v1")
        self.assertEqual(
            {"driver_memory": "9G", "executor_cores": 2, "executor_memory": "4G", "spark_version": "3.1.1"},
            join_node.settings,
        )

        left_table_node = self._get_node(flow.nodes, "compute_join__sample_team_sample_join_v1__left_table")
        self.assertEqual(
            {"driver_memory": "6G", "executor_cores": 2, "executor_memory": "4G", "spark_version": "3.1.1"},
            left_table_node.settings,
        )

    def test_command(self):
        flow = self.join_backfill.build_flow()
        group_by_node = self._get_node(
            flow.nodes, "compute_join__sample_team_sample_join_v1__sample_team_sample_group_by_v1"
        )
        expected_command = (
            "export SPARK_VERSION=3.1.1 && export EXECUTOR_MEMORY=4G && export DRIVER_MEMORY=4G && "
            "export EXECUTOR_CORES=2 && run.py --conf=production/joins/sample_team/sample_join.v1 "
            "--ds=2025-01-01 --mode=backfill --selected-join-parts=sample_team_sample_group_by_v1 "
            "--use-cached-left --start-ds=2025-01-01"
        )
        self.assertEqual(expected_command, group_by_node.command)

    def test_join_with_external_parts_with_offline_groupby(self):
        """Test that external parts with offlineGroupBy are included in the flow"""
        conf_file = (
            "api/py/test/sample/production/joins/sample_team/" "sample_join_with_derivations_on_external_parts.v1"
        )
        join_backfill = JoinBackfill(config_path=conf_file, start_date="2025-01-01", end_date="2025-01-01")

        flow = join_backfill.build_flow()
        node_names = {node.name for node in flow.nodes}

        # Verify we have the expected nodes: left, 2 regular parts, 1 external part, final = 5 nodes
        self.assertEqual(5, len(node_names), "Should have 5 nodes: left, 2 regular parts, external part, final")

        # Should have nodes for regular join parts (2 regular parts in the test file)
        self.assertTrue(
            any("sample_team_entity_sample_group_by_from_module_v1" in name for name in node_names),
            "Should have node for first regular join part",
        )
        self.assertTrue(
            any("sample_team_event_sample_group_by_v1" in name for name in node_names),
            "Should have node for second regular join part OR external part with offlineGroupBy",
        )

        # Should have node for external part with offlineGroupBy with 'ext_' prefix
        # The external part should have a name starting with 'ext_'
        external_nodes = [name for name in node_names if "ext_" in name]
        self.assertEqual(len(external_nodes), 1, "Should have exactly 1 external part node with 'ext_' prefix")

        # Verify left table and final join nodes exist
        self.assertTrue(any("left_table" in name for name in node_names), "Should have left table node")
        self.assertTrue(any("chronon_db_sample_team" in name for name in node_names), "Should have final join node")

    def test_join_without_join_parts(self):
        """Test that external parts with offlineGroupBy are included in the flow"""
        conf_file = "api/py/test/sample/production/joins/sample_team/" "sample_join_without_join_parts.v1"
        join_backfill = JoinBackfill(config_path=conf_file, start_date="2025-01-01", end_date="2025-01-01")

        flow = join_backfill.build_flow()
        node_names = {node.name for node in flow.nodes}

        # Verify we have the expected nodes
        self.assertEqual(2, len(node_names), "Should have 2 nodes: left, final")

        # Verify left table and final join nodes exist
        self.assertTrue(any("left_table" in name for name in node_names), "Should have left table node")
        self.assertTrue(
            any(
                "compute_join__chronon_db_sample_team_sample_join_without_join_parts_v1" in name for name in node_names
            ),
            "Should have final join node",
        )

        # Verify that left table is a dependency for final join
        final_node = next(
            node
            for node in flow.nodes
            if "compute_join__chronon_db_sample_team_sample_join_without_join_parts_v1" in node.name
        )
        left_node = next(node for node in flow.nodes if "left_table" in node.name)
        self.assertIn(left_node, final_node.dependencies, "Final join node should depend on left table node")

    def test_join_with_both_regular_and_external_parts(self):
        """Test join with both regular joinParts and external parts with offlineGroupBy."""
        conf_file = "api/py/test/sample/production/joins/sample_team/sample_join_with_derivations_on_external_parts.v1"
        with open(conf_file, "r") as f:
            import json

            config = f.read()
        join = convert_json_to_obj(json.loads(config))

        result = get_regular_and_external_join_parts(join)

        # Should have 2 regular parts + 1 external part
        self.assertEqual(len(result), 3, "Should have 2 regular parts and 1 external part")

        # First two should be regular
        regular_parts = [jp for jp in result if not isinstance(jp, ExternalJoinPart)]
        self.assertEqual(len(regular_parts), 2, "Should have 2 regular parts")

        # Last one should be external
        external_parts = [jp for jp in result if isinstance(jp, ExternalJoinPart)]
        self.assertEqual(len(external_parts), 1, "Should have 1 external part")


    def test_external_part_prefix_and_attributes(self):
        """Test external join part prefix construction and attribute copying."""
        import json

        # Test with user prefix
        join_with_prefix = convert_json_to_obj({
            "metaData": {"name": "test.join.v1"},
            "joinParts": [],
            "onlineExternalParts": [{
                "source": {
                    "metadata": {"name": "my.external.source"},
                    "offlineGroupBy": {
                        "metaData": {"name": "test.offline_gb.v1"},
                        "keyColumns": ["key1"],
                    },
                },
                "prefix": "custom_prefix",
                "keyMapping": {"left_key": "right_key"},
            }],
        })
        result_with = get_regular_and_external_join_parts(join_with_prefix)
        self.assertEqual(len(result_with), 1)
        self.assertIsInstance(result_with[0], ExternalJoinPart)
        self.assertEqual(result_with[0].external_join_full_prefix, "ext_custom_prefix_my_external_source")
        self.assertEqual(join_part_name(result_with[0]), "ext_custom_prefix_my_external_source")
        self.assertEqual(result_with[0].keyMapping, {"left_key": "right_key"})

        # Test without user prefix
        join_without_prefix = convert_json_to_obj({
            "metaData": {"name": "test.join.v1"},
            "joinParts": [],
            "onlineExternalParts": [{
                "source": {
                    "metadata": {"name": "my.external.source"},
                    "offlineGroupBy": {
                        "metaData": {"name": "test.offline_gb.v1"},
                        "keyColumns": ["key1"],
                    },
                },
            }],
        })
        result_without = get_regular_and_external_join_parts(join_without_prefix)
        self.assertEqual(len(result_without), 1)
        self.assertEqual(result_without[0].external_join_full_prefix, "ext_my_external_source")
        self.assertEqual(join_part_name(result_without[0]), "ext_my_external_source")

    def test_multiple_external_parts_with_offline_groupby(self):
        """Test join with multiple external parts that have offlineGroupBy."""
        import json

        join_dict = {
            "metaData": {"name": "test.join.v1"},
            "joinParts": [],
            "onlineExternalParts": [
                {
                    "source": {
                        "metadata": {"name": "external_source_1"},
                        "offlineGroupBy": {
                            "metaData": {"name": "test.offline_gb_1.v1"},
                            "keyColumns": ["key1"],
                        },
                    },
                    "prefix": "prefix_1",
                },
                {
                    "source": {
                        "metadata": {"name": "external_source_2"},
                        "offlineGroupBy": {
                            "metaData": {"name": "test.offline_gb_2.v1"},
                            "keyColumns": ["key2"],
                        },
                    },
                    "prefix": "prefix_2",
                },
            ],
        }
        join = convert_json_to_obj(join_dict)

        result = get_regular_and_external_join_parts(join)

        # Should have 2 external join parts
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ExternalJoinPart)
        self.assertIsInstance(result[1], ExternalJoinPart)

        # Verify the prefixes are correct
        self.assertEqual(result[0].external_join_full_prefix, "ext_prefix_1_external_source_1")
        self.assertEqual(result[1].external_join_full_prefix, "ext_prefix_2_external_source_2")

    def test_external_join_part_from_external_part_factory_method(self):
        """Test the ExternalJoinPart.from_external_part factory method directly."""
        # Create a mock external part with offlineGroupBy
        external_part = convert_json_to_obj({
            "source": {
                "metadata": {"name": "my.external.source"},
                "offlineGroupBy": {
                    "metaData": {"name": "test.offline_gb.v1"},
                    "keyColumns": ["key1", "key2"],
                    "sources": [{"events": {"table": "test_table"}}],
                },
            },
            "prefix": "test_prefix",
            "keyMapping": {"left_key": "right_key", "another_key": "mapped_key"},
        })

        # Use the factory method
        external_jp = ExternalJoinPart.from_external_part(external_part)

        # Verify it's an ExternalJoinPart
        self.assertIsInstance(external_jp, ExternalJoinPart)

        # Verify the full_prefix is computed correctly
        self.assertEqual(external_jp.external_join_full_prefix, "ext_test_prefix_my_external_source")

        # Verify the synthetic JoinPart has the correct attributes
        self.assertEqual(external_jp.groupBy.metaData.name, "test.offline_gb.v1")
        self.assertEqual(external_jp.keyMapping, {"left_key": "right_key", "another_key": "mapped_key"})
        self.assertEqual(external_jp.prefix, "test_prefix")

        # Verify join_part_name uses the external_join_full_prefix
        self.assertEqual(join_part_name(external_jp), "ext_test_prefix_my_external_source")

    def test_external_join_part_from_external_part_without_prefix(self):
        """Test the factory method when external part has no user-defined prefix."""
        external_part = convert_json_to_obj({
            "source": {
                "metadata": {"name": "another.source"},
                "offlineGroupBy": {
                    "metaData": {"name": "test.offline_gb.v1"},
                    "keyColumns": ["key1"],
                },
            },
            # No prefix and no keyMapping
        })

        external_jp = ExternalJoinPart.from_external_part(external_part)

        # Verify the full_prefix is computed correctly without user prefix
        self.assertEqual(external_jp.external_join_full_prefix, "ext_another_source")

        # Verify keyMapping and prefix are None
        self.assertIsNone(external_jp.keyMapping)
        self.assertIsNone(external_jp.prefix)
