"""
Run the flow for materialize.
"""

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

from ai.chronon.repo.compile import extract_and_convert
from click.testing import CliRunner


def test_basic_compile():
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team/'
    ])
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team'
    ])
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team/sample_join.py'
    ])
    assert result.exit_code == 0


def test_debug_compile():
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--chronon_root=test/sample',
        '--input_path=joins/sample_team/',
        '--debug'
    ])
    assert result.exit_code == 0


def test_failed_compile():
    """
    Should fail as it fails to find teams.
    """
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--input_path=joins/sample_team/',
    ])
    assert result.exit_code != 0

def test_failed_compile_missing_input_column():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, [
        '--zipline_root=test/sample',
        '--input_path=group_bys/sample_team/sample_group_by_missing_input_column.py',
        '--debug'
    ])
    assert result.exit_code != 0
