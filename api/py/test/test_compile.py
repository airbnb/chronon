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

import json
import os
import re

import pytest
from ai.chronon.api.ttypes import GroupBy, Join
from ai.chronon.repo.compile import extract_and_convert
from ai.chronon.repo.serializer import json2thrift
from ai.chronon.utils import FeatureDisplayKeys
from click.testing import CliRunner

CURRENT_FILE_DIR = os.path.dirname(os.path.abspath(__file__))


# Utility functions
def _get_full_file_path(relative_path):
    """Utility function to get the full file path based on a relative path."""
    return os.path.join(CURRENT_FILE_DIR, relative_path)


def _invoke_cli_with_params(runner, input_path, flags=None):
    """Invoke the CLI command with consistent options and specified input_path."""
    command = [
        "--chronon_root=api/py/test/sample",
        f"--input_path={input_path}",
        "--debug",
    ]
    if flags:
        command.extend(flags)
    result = runner.invoke(
        extract_and_convert,
        command,
    )
    return result


def _assert_file_exists(full_file_path, message):
    """Assert that a file exists at the specified path."""
    assert os.path.isfile(full_file_path), message


def _extract_display_output_json_block(output, block_name):
    """Extract a JSON block from the output of a CLI command."""

    field_index = output.find(block_name)
    subset = output[field_index:]
    start_index = subset.find("json.start")
    end_index = subset.find("json.end")
    s = re.sub(r"\x1b\[[0-9;]*m", "", subset[start_index + 10 : end_index].strip())
    return json.loads(s)


@pytest.fixture
def specific_setup():
    # This setup code will only run for tests that request this fixture
    yield  # This is where the testing happens
    # Teardown phase: Cleanup after tests
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    files_to_clean = [
        "sample/production/group_bys/unit_test/event_sample_group_by.v1",
        "sample/production/group_bys/unit_test/entity_sample_group_by.require_backfill",
        "sample/production/group_bys/unit_test/user_inline_group_by",
        "sample/production/joins/unit_test/sample_online_join.v1",
        "sample/production/joins/unit_test/sample_join.v1",
        "sample/production/joins/unit_test/sample_online_join_with_gb_not_online.v1",
        "sample/production/joins/unit_test/user.sample_join_inline_group_by.v1",
    ]

    for relative_path in files_to_clean:
        full_path = os.path.join(current_file_dir, relative_path)
        if os.path.exists(full_path):
            os.remove(full_path)


def test_basic_compile():
    runner = CliRunner()
    result = runner.invoke(
        extract_and_convert, ["--chronon_root=api/py/test/sample", "--input_path=joins/sample_team/"]
    )
    assert result.exit_code == 0
    result = runner.invoke(extract_and_convert, ["--chronon_root=api/py/test/sample", "--input_path=joins/sample_team"])
    assert result.exit_code == 0
    result = runner.invoke(
        extract_and_convert, ["--chronon_root=api/py/test/sample", "--input_path=joins/sample_team/sample_join.py"]
    )
    assert result.exit_code == 0


# Test deprecation warning when a to-be-deprecated group_by is used in an active join
def test_compile_group_by_deprecation():
    runner = CliRunner()
    result = runner.invoke(
        extract_and_convert,
        [
            "--chronon_root=api/py/test/sample",
            "--input_path=group_bys/sample_team/sample_deprecation_group_by.py",
            "--force-overwrite",
        ],
    )
    assert result.exit_code == 0
    warning_message_title = "deprecation warning"
    actual_message = str(result.output).strip().lower()
    assert warning_message_title in actual_message, f"Deprecation warning message not seen in {actual_message}"


# Test deprecation warning when an active join is having a to-be-deprecated join part
def test_compile_join_deprecation():
    runner = CliRunner()
    result = runner.invoke(
        extract_and_convert,
        [
            "--chronon_root=api/py/test/sample",
            "--input_path=joins/sample_team/sample_deprecation_join.py",
            "--force-overwrite",
        ],
    )
    assert result.exit_code == 0
    warning_message_title = "deprecation warning"
    actual_message = str(result.output).strip().lower()
    assert warning_message_title in actual_message, f"Deprecation warning message not seen in {actual_message}"


def test_debug_compile():
    runner = CliRunner()
    result = runner.invoke(
        extract_and_convert, ["--chronon_root=api/py/test/sample", "--input_path=joins/sample_team/", "--debug"]
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
    result = _invoke_cli_with_params(runner, "group_bys/sample_team/sample_group_by_missing_input_column.py")
    assert result.exit_code != 0


def test_failed_compile_when_dependent_join_detected():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(runner, "group_bys/sample_team/event_sample_group_by.py")
    assert result.exit_code != 0
    error_message_expected = "Detected dependencies are as follows: ['sample_team.sample_chaining_join.parent_join', 'sample_team.sample_join_bootstrap.v1', 'sample_team.sample_join_bootstrap.v2', 'sample_team.sample_join_derivation.v1', 'sample_team.sample_join_with_derivations_on_external_parts.v1', 'sample_team.sample_label_join.v1', 'sample_team.sample_label_join_with_agg.v1', 'sample_team.sample_online_join.v1']"
    actual_exception_message = str(result.exception).strip().lower()
    error_message_expected = error_message_expected.strip().lower()
    assert (
        error_message_expected in actual_exception_message
    ), f"Got a different message than expected {actual_exception_message}"
    assert isinstance(
        result.exception, AssertionError
    ), "Expected an AssertionError, but got a different exception or no exception."


def test_detected_dependent_joins_materialized():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(runner, "group_bys/sample_team/event_sample_group_by.py", ["--force-overwrite"])
    assert result.exit_code == 0
    expected_message = "Successfully wrote 8 Join objects to api/py/test/sample/production".strip().lower()
    actual_message = str(result.output).strip().lower()
    assert expected_message in actual_message, f"Got a different message than expected {actual_message}"


def test_failed_compile_when_dependent_groupby_detected():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(runner, "joins/unit_test/sample_parent_join.py")
    assert result.exit_code != 0
    error_message_expected = (
        "Detected dependencies are as follows: ['unit_test.sample_chaining_group_by.chaining_group_by_v1']"
    )
    actual_exception_message = str(result.exception).strip().lower()
    error_message_expected = error_message_expected.strip().lower()
    assert (
        error_message_expected in actual_exception_message
    ), f"Got a different message than expected {actual_exception_message}"
    assert isinstance(
        result.exception, AssertionError
    ), "Expected an AssertionError, but got a different exception or no exception."


def test_detected_dependent_group_bys_materialized():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(runner, "joins/unit_test/sample_parent_join.py", ["--force-overwrite"])
    assert result.exit_code == 0
    expected_message = "Successfully wrote 2 GroupBy objects to api/py/test/sample/production".strip().lower()
    actual_message = str(result.output).strip().lower()
    assert expected_message in actual_message, f"Got a different message than expected {actual_message}"


def test_detected_dependent_nested_joins():
    """
    Should raise errors as we are trying to create aggregations without input column.
    """
    runner = CliRunner()
    result = _invoke_cli_with_params(
        runner, "group_bys/unit_test/user/sample_nested_group_by.py", ["--force-overwrite"]
    )
    assert result.exit_code == 0
    expected_message = "Successfully wrote 1 Join objects to api/py/test/sample/production".strip().lower()
    actual_message = str(result.output).strip().lower()
    assert expected_message in actual_message, f"Got a different message than expected {actual_message}"


def test_compile_table_display():
    """
    Test that compiling an online join correctly materializes all online group_bys.
    """
    runner = CliRunner()
    input_path = f"joins/sample_team/sample_join_with_derivations_on_external_parts.py"
    result = _invoke_cli_with_params(runner, input_path, ["--table-display"])

    assert "Output Join Tables" in result.output
    output_json_dict = _extract_display_output_json_block(result.output, "Output Join Tables")
    expected_json_dict = {
        "backfill": ["chronon_db.sample_team_sample_join_with_derivations_on_external_parts_v1"],
        "stats-summary": ["chronon_db.sample_team_sample_join_with_derivations_on_external_parts_v1_daily_stats"],
        "log-flattener": ["chronon_db.sample_team_sample_join_with_derivations_on_external_parts_v1_logged"],
        "bootstrap": ["chronon_db.sample_team_sample_join_with_derivations_on_external_parts_v1_bootstrap"],
        "join_parts": [
            "sample_team_sample_join_with_derivations_on_external_parts_v1_sample_team_event_sample_group_by_v1",
            "sample_team_sample_join_with_derivations_on_external_parts_v1_sample_team_entity_sample_group_by_from_module_v1",
        ],
    }
    assert json.dumps(output_json_dict, sort_keys=True) == json.dumps(expected_json_dict, sort_keys=True)
    assert result.exit_code == 0


def test_compile_feature_display():
    """
    Test that compiling an online join correctly materializes all online group_bys.
    """
    runner = CliRunner()
    input_path = f"joins/sample_team/sample_join_with_derivations_on_external_parts.py"
    result = _invoke_cli_with_params(runner, input_path, ["--feature-display"])

    expected = map(
        lambda x: x.value.replace("_", " ").title(),
        [
            FeatureDisplayKeys.SOURCE_KEYS,
            FeatureDisplayKeys.INTERNAL_COLUMNS,
            FeatureDisplayKeys.EXTERNAL_COLUMNS,
            FeatureDisplayKeys.DERIVED_COLUMNS,
            FeatureDisplayKeys.OUTPUT_COLUMNS,
        ],
    )
    for key in expected:
        assert key in result.output
    assert result.exit_code == 0


def test_table_display_staging_query():
    """
    Test a staging query compile produces related table
    """
    runner = CliRunner()
    input_path = f"staging_queries/sample_team/sample_staging_query.py"
    result = runner.invoke(
        extract_and_convert,
        [
            "--chronon_root=api/py/test/sample",
            f"--input_path={input_path}",
            "--table-display",
        ],
    )

    assert "Output StagingQuery Tables" in result.output
    output_json_dict = _extract_display_output_json_block(result.output, "Output StagingQuery Tables")
    expected_json_dict = {"backfill": ["sample_namespace.sample_team_sample_staging_query_v1"]}
    assert json.dumps(output_json_dict, sort_keys=True) == json.dumps(expected_json_dict, sort_keys=True)
    assert result.exit_code == 0


def test_compile_dependency_staging_query():
    """
    Test that compiling a staging query does not error out
    """
    runner = CliRunner()
    input_path = f"staging_queries/sample_team/sample_staging_query.py"
    result = runner.invoke(
        extract_and_convert,
        ["--chronon_root=api/py/test/sample", f"--input_path={input_path}"],
    )

    assert result.exit_code == 0


def test_compile_inline_group_by():
    # Test compiling an inline group by.
    runner = CliRunner()
    input_path = f"joins/unit_test/user/sample_join_inline_group_by.py"
    result = runner.invoke(
        extract_and_convert,
        ["--chronon_root=api/py/test/sample", f"--input_path={input_path}"],
    )
    assert result.exit_code == 0

    # Verify the inline group_by's team name is set correctly.
    path = "sample/production/group_bys/unit_test/user_inline_group_by"
    full_file_path = _get_full_file_path(path)
    _assert_file_exists(full_file_path, f"Expected {os.path.basename(path)} to be materialized, but it was not.")
    with open(full_file_path, "r") as file:
        group_by = json2thrift(file.read(), GroupBy)
        assert group_by.metaData.team == "unit_test"

    path = "sample/production/joins/unit_test/user.sample_join_inline_group_by.v1"
    full_file_path = _get_full_file_path(path)
    _assert_file_exists(full_file_path, f"Expected {os.path.basename(path)} to be materialized, but it was not.")
    with open(full_file_path, "r") as file:
        join = json2thrift(file.read(), Join)
        assert len(join.joinParts) == 1
        assert join.joinParts[0].groupBy.metaData.team == "unit_test"
