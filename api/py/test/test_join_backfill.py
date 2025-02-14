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

from ai.chronon.repo.join_backfill import JoinBackfill


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
                "compute_join__sample_team_sample_chaining_join_parent_join__sample_team_entity_sample_group_by_from_module_v1",
                "compute_join__sample_team_sample_chaining_join_parent_join__left_table",
                "compute_join__chronon_db_sample_team_sample_chaining_join_v1",
                "compute_join__sample_team_sample_chaining_join_v1__sample_team_sample_chaining_group_by",
                "compute_join__sample_team_sample_chaining_join_parent_join__sample_team_event_sample_group_by_v1",
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
        self.assertEqual(
            "export SPARK_VERSION=3.1.1 && export EXECUTOR_MEMORY=4G && export DRIVER_MEMORY=4G && export EXECUTOR_CORES=2 && run.py --conf=production/joins/sample_team/sample_join.v1 --ds=2025-01-01 --mode=backfill --selected-join-parts=sample_team_sample_group_by_v1 --use-cached-left --start-ds=2025-01-01",
            group_by_node.command,
        )
