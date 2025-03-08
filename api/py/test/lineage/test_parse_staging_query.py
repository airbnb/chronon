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

from ai.chronon.api import ttypes
from ai.chronon.lineage.lineage_metadata import ColumnTransform, TableType
from ai.chronon.lineage.lineage_parser import LineageParser


class TestParseStagingQuery(unittest.TestCase):
    def setUp(self):
        query = """
            SELECT
                raw_subject, raw_event_id
            FROM
                staging_query_input
        """
        self.base_table = ttypes.StagingQuery(
            query=query,
            startPartition="2020-03-01",
            metaData=ttypes.MetaData(name="staging_table", outputNamespace="test"),
        )

    def test_staging_query_table(self):
        parser = LineageParser()
        parser.parse_staging_query(self.base_table)

        self.assertEqual(
            {"staging_query_input", "test.staging_table"},
            set(parser.metadata.tables.keys()),
        )
        self.assertEqual(TableType.OTHER, parser.metadata.tables["staging_query_input"].table_type)
        self.assertEqual(
            TableType.STAGING_QUERY,
            parser.metadata.tables["test.staging_table"].table_type,
        )

        self.assertEqual(
            {"raw_event_id", "raw_subject"},
            parser.metadata.tables["staging_query_input"].columns,
        )
        self.assertEqual(
            {"raw_event_id", "raw_subject"},
            parser.metadata.tables["test.staging_table"].columns,
        )

        self.compare_lineages(
            {
                (
                    "staging_query_input.raw_subject",
                    "test.staging_table.raw_subject",
                    (),
                ),
                (
                    "staging_query_input.raw_event_id",
                    "test.staging_table.raw_event_id",
                    (),
                ),
            },
            parser.metadata.lineages,
        )

    def compare_lineages(self, expected, actual):
        expected = sorted(expected)
        actual = sorted(actual, key=lambda t: (t.input_column, t.output_column, t.transforms))
        self.assertEqual(len(actual), len(expected))
        for lineage_expected, lineage_actual in zip(expected, actual):
            self.assertEqual(
                ColumnTransform(lineage_expected[0], lineage_expected[1], lineage_expected[2]),
                lineage_actual,
            )
