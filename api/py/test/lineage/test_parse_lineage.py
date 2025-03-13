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
        attributes = ["configs", "tables", "features", "lineages"]
        for attr in attributes:
            self.assertGreater(len(getattr(metadata, attr)), 0, f"{attr} should not be empty")

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

    def test_parse_lineage_complex_columns(self):
        output_table = "output_table"
        sql = """
            SELECT
             country AS country,
             address.city AS city,
             address.zipcode AS zipcode
            FROM
                input_table
        """

        lineage = build_lineage(output_table, sql)
        self.assertEqual(
            {
                "output_table.city": {("input_table.address.city", ("Dot",))},
                "output_table.country": {("input_table.country", ())},
                "output_table.zipcode": {("input_table.address.zipcode", ("Dot",))},
            },
            lineage,
        )

    def test_parse_lineage_without_schema(self):
        output_table = "output_table"
        sql = """
            SELECT
             a, b, c
            FROM
            (
                SELECT * FROM input_table
            )
        """

        lineage = build_lineage(output_table, sql)
        self.assertEqual(
            {
                "output_table.a": {("input_table.*", ())},
                "output_table.b": {("input_table.*", ())},
                "output_table.c": {("input_table.*", ())},
            },
            lineage,
        )

    def test_parse_lineage_with_schema(self):
        output_table = "output_table"
        sql = """
            SELECT
             a, b, c
            FROM
            (
                SELECT * FROM input_table
            )
        """

        lineage = build_lineage(
            output_table,
            sql,
            schema={"input_table": {"a": "INT", "b": "INT", "c": "INT", "d": "INT"}},
        )
        self.assertEqual(
            {
                "output_table.a": {("input_table.a", ())},
                "output_table.b": {("input_table.b", ())},
                "output_table.c": {("input_table.c", ())},
            },
            lineage,
        )

    def test_find_all_tables(self):
        sql = """
            SELECT
             a,
             b,
             c,
             d
            FROM (
                SELECT input_table1.id, a, b, c FROM input_table1
                INNER JOIN input_table2
                ON input_table1.id = input_table2.id
            ) input_table3
            INNER JOIN
            input_table4
            ON input_table3.id = input_table4.id
        """
        tables = LineageParser.find_all_tables(sql)
        self.assertEqual({"input_table1", "input_table2", "input_table4"}, tables)
