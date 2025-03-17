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


def compare_lineages(testcase, expected, actual):
    expected = sorted(expected)
    actual = sorted(
        actual, key=lambda t: (t.input_table, t.input_column, t.output_table, t.output_column, t.transforms)
    )
    testcase.assertEqual(len(actual), len(expected))
    for exp, act in zip(expected, actual):
        testcase.assertEqual(ColumnTransform(exp[0][0], exp[0][1], exp[1][0], exp[1][1], exp[2]), act)
