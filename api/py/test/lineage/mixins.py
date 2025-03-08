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

from ai.chronon.lineage.lineage_metadata import ColumnTransform


class LineageTestMixin:
    def compare_lineages(self, expected, actual):
        expected = sorted(expected)
        actual = sorted(actual, key=lambda t: (t.input_column, t.output_column, t.transforms))
        self.assertEqual(len(actual), len(expected))
        for lineage_expected, lineage_actual in zip(expected, actual):
            self.assertEqual(
                ColumnTransform(lineage_expected[0], lineage_expected[1], lineage_expected[2]), lineage_actual
            )
