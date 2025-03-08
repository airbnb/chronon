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

from ai.chronon.lineage.lineage_parser import LineageParser, build_lineage

TEST_BASE_PATH = os.path.join(os.path.dirname(__file__), "../sample")


class TestParseLineage(unittest.TestCase):
    def test_parse_all_configs(self):
        parser = LineageParser()
        parser.parse_lineage(TEST_BASE_PATH)
        metadata = parser.metadata
        self.assertIsNotNone(metadata.features)
        self.assertIsNotNone(metadata.tables)
        self.assertIsNotNone(metadata.lineages)
        self.assertIsNotNone(metadata.unparsed_columns)

    def test_cannot_parse_lineage(self):
        # Can't parse lineage since there is no specific column.
        lineage = build_lineage("output", "SELECT COUNT(*) AS a FROM input")
        self.assertEqual(
            {"output.a": set()},
            lineage,
        )

    def test_parse_lineage(self):
        output_table = "output_table"
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

        lineage = build_lineage(output_table, sql)
        self.assertEqual(
            {
                "output_table.a": {("input_table.f", ())},
                "output_table.b": {
                    ("input_table.e", ("Add",)),
                    ("input_table.f", ("Add",)),
                },
                "output_table.c": {("input_table.g", ("AGG",))},
                "output_table.d": {("input_table.h", ("Sum",))},
            },
            lineage,
        )

    def test_build_select_sql(self):
        sql = LineageParser.build_select_sql(
            "input_table",
            [("guest", "guest_id"), ("host", "host_id")],
            "ds = '2025-01-01'",
        )
        self.assertEqual(
            "SELECT guest_id AS guest, host_id AS host FROM input_table WHERE ds = '2025-01-01'",
            sql.sql(dialect="spark"),
        )
