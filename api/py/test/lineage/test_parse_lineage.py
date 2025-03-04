import os
import unittest

from ai.chronon.lineage.lineage_parser import build_lineage

TEST_BASE_PATH = os.path.join(os.path.dirname(__file__), "../sample")


class TestParseLineage(unittest.TestCase):
    def test_cannot_parse_lineage(self):
        # Can't parse lineage since there is no specific column.
        lineage = build_lineage("output", "SELECT COUNT(*) AS a FROM input", sources={})
        self.assertEqual(
            {"output.a": []},
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

        lineage = build_lineage(output_table, sql, sources={})
        expected_lineage = {
            "output_table.a": [("input_table.f", ("Alias",))],
            "output_table.b": [
                ("input_table.f", ("Add", "Alias")),
                ("input_table.e", ("Add", "Alias")),
            ],
            "output_table.c": [("input_table.g", ("AGG", "Alias"))],
            "output_table.d": [("input_table.h", ("Sum", "Alias"))],
        }

        self.assertEqual(set(lineage.keys()), set(expected_lineage.keys()))

        for key in expected_lineage:
            self.assertCountEqual(lineage[key], expected_lineage[key])
