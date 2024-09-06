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

import os
import shutil

import pytest
from ai.chronon.repo.compile import extract_and_convert
from click.testing import CliRunner

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))


# Utility functions
def _get_full_file_path(relative_path):
    """Utility function to get the full file path based on a relative path."""
    return os.path.join(CURRENT_FILE_DIR, relative_path)


def _invoke_cli_with_params(runner, input_path):
    """Invoke the CLI command with consistent options and specified input_path."""
    result = runner.invoke(
        extract_and_convert,
        [
            "--chronon_root=test/sample",
            f"--input_path={input_path}",
            "--debug",
        ],
    )
    return result


def _assert_file_exists(full_file_path, message):
    """Assert that a file exists at the specified path."""
    assert os.path.isfile(full_file_path), message


@pytest.fixture
def specific_setup():
    # This setup code will only run for tests that request this fixture
    yield  # This is where the testing happens
    # Teardown phase: Cleanup after tests
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    directories_to_clean = ["sample/production/joins/unit_test/", "sample/production/group_bys/unit_test/"]

    for relative_path in directories_to_clean:
        full_path = os.path.join(current_file_dir, relative_path)
        if os.path.exists(full_path):
            shutil.rmtree(full_path)


def test_basic_compile():
    runner = CliRunner()
    result = runner.invoke(extract_and_convert, ["--chronon_root=test/sample", "--input_path=joins/sample_team/"])
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, ["--chronon_root=test/sample", "--input_path=joins/sample_team"])
    assert result.exit_code == 0
    result = runner.invoke(
        extract_and_convert, ["--chronon_root=test/sample", "--input_path=joins/sample_team/sample_join.py"]
    )
    assert result.exit_code == 0
    result = runner.invoke(
        extract_and_convert,
        ["--chronon_root=test/sample", "--input_path=group_bys/sample_team/sample_deprecation_group_by.py"],
    )
    assert result.exit_code == 0


def test_debug_compile():
    runner = CliRunner()
    result = runner.invoke(
        extract_and_convert, ["--chronon_root=test/sample", "--input_path=joins/sample_team/", "--debug"]
    )
    assert result.exit_code == 0


def test_failed_compile():
    """
    Should fail as it fails to find teams.
    """
    runner = CliRunner()
    result = runner.invoke(
        extract_and_convert,
        [
            "--input_path=joins/sample_team/",
        ],
    )
    assert result.exit_code != 0


def test_failed_compile_online_join_not_online_gb(specific_setup):
    """
    Raises an error when compiling an online join that references a group_by that is not online.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(runner, "joins/unit_test/sample_online_join_with_gb_not_online.py")

    error_message_expected = "Fix the following: ['unit_test.entity_sample_group_by.v1']"
    assert result.exit_code != 0, "Command unexpectedly succeeded, but an error was expected."
    assert error_message_expected in str(
        result.exception
    ), f"Got a different message than expected.: {result.exception}"
    assert isinstance(
        result.exception, AssertionError
    ), "Expected an AssertionError, but got a different exception or no exception."


def test_compiling_online_join_compiles_all_online_gb(specific_setup):
    """
    Test that compiling an online join correctly materializes all online group_bys.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(runner, "joins/unit_test/sample_online_join.py")
    assert result.exit_code == 0

    full_file_path = _get_full_file_path("sample/production/group_bys/unit_test/event_sample_group_by.v1")
    _assert_file_exists(full_file_path, "Expected the group_by to be materialized, but it was not.")


def test_compiling_join_compiles_online_bsd_gb(specific_setup):
    """
    Test that compiling a join correctly materializes online group_bys and those with backfillStartDate set.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(runner, "joins/unit_test/sample_join.py")
    assert result.exit_code == 0

    paths_to_check = [
        "sample/production/group_bys/unit_test/event_sample_group_by.v1",
        "sample/production/group_bys/unit_test/entity_sample_group_by.require_backfill",
    ]
    for path in paths_to_check:
        full_file_path = _get_full_file_path(path)
        _assert_file_exists(full_file_path, f"Expected {os.path.basename(path)} to be materialized, but it was not.")


def test_failed_compile_missing_input_column():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = runner.invoke(
        extract_and_convert,
        [
            "--chronon_root=test/sample",
            "--input_path=group_bys/sample_team/sample_group_by_missing_input_column.py",
            "--debug",
        ],
    )
    assert result.exit_code != 0
