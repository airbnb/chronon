"""
Basic tests for namespace and breaking changes in run.py
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

import argparse
import json
import os
import time

import pytest
from ai.chronon.repo import run

DEFAULT_ENVIRONMENT = os.environ.copy()


@pytest.fixture
def parser():
    """Basic parser for tests relative to the main arguments of run.py"""
    parser = argparse.ArgumentParser()
    args = [
        "repo",
        "conf",
        "mode",
        "env",
        "app-name",
        "chronon-jar",
        "online-jar",
        "online-class",
        "render-info",
        "sub-help",
    ]
    for arg in args:
        parser.add_argument(f"--{arg}")
    run.set_defaults(parser)
    return parser


@pytest.fixture
def test_conf_location():
    """Sample test conf for tests"""
    return "production/joins/sample_team/sample_online_join.v1"


def reset_env(default_env):
    set_keys = os.environ.keys()
    for key in set_keys:
        os.environ.pop(key)
    for k, v in default_env.items():
        os.environ[k] = v


def test_download_jar(monkeypatch, sleepless):
    def mock_cmd(url, path, skip_download):
        return url

    monkeypatch.setattr(time, "sleep", sleepless)
    monkeypatch.setattr(run, "download_only_once", mock_cmd)
    jar_path = run.download_jar("version", jar_type="uber", release_tag=None, spark_version="2.4.0")
    assert jar_path == "/tmp/spark_uber_2.11-version-assembly.jar"
    jar_path = run.download_jar("version", jar_type="uber", release_tag=None, spark_version="3.1.1")
    assert jar_path == "/tmp/spark_uber_2.12-version-assembly.jar"
    jar_path = run.download_jar("version", jar_type="uber", release_tag=None, spark_version="3.2.1")
    assert jar_path == "/tmp/spark_uber_2.13-version-assembly.jar"
    with pytest.raises(Exception):
        run.download_jar("version", jar_type="uber", release_tag=None, spark_version="2.1.0")


def test_environment(teams_json, repo, parser, test_conf_location):
    default_environment = DEFAULT_ENVIRONMENT.copy()
    # If nothing is passed.
    run.set_runtime_env(parser.parse_args(args=[]))

    # If repo is passed common_env is loaded.
    reset_env(default_environment)
    run.set_runtime_env(parser.parse_args(args=["--repo", repo]))
    assert os.environ["VERSION"] == "latest"

    # For chronon_metadata_export is passed. APP_NAME should be set.
    reset_env(default_environment)
    run.set_runtime_env(parser.parse_args(args=["--mode", "metadata-export"]))
    assert os.environ["APP_NAME"] == "chronon_metadata_export"

    # If APP_NAME is set, should be respected.
    reset_env(default_environment)
    os.environ["APP_NAME"] = "fake-name"
    run.set_runtime_env(parser.parse_args(args=["--mode", "metadata-export"]))
    assert os.environ["APP_NAME"] == "fake-name"

    # If app_name can be passed from cli.
    reset_env(default_environment)
    run.set_runtime_env(parser.parse_args(args=["--mode", "metadata-export", "--app-name", "fake-name"]))
    assert os.environ["APP_NAME"] == "fake-name"

    # Check default backfill for a team sets parameters accordingly.
    reset_env(default_environment)
    run.set_runtime_env(
        parser.parse_args(
            args=[
                "--mode",
                "backfill",
                "--conf",
                test_conf_location,
                "--repo",
                repo,
                "--env",
                "production",
                "--online-jar",
                test_conf_location,
            ]
        )
    )
    # from team env.
    assert os.environ["EXECUTOR_CORES"] == "4"
    # from default env.
    assert os.environ["DRIVER_MEMORY"] == "15G"
    # from common env.
    assert os.environ["VERSION"] == "latest"
    # derived from args.
    assert os.environ["APP_NAME"] == "chronon_joins_backfill_production_sample_team.sample_online_join.v1"
    # from additional_args
    assert os.environ["CHRONON_CONFIG_ADDITIONAL_ARGS"] == "--step-days 14"

    # Check dev backfill for a team sets parameters accordingly.
    reset_env(default_environment)
    run.set_runtime_env(
        parser.parse_args(
            args=[
                "--mode",
                "backfill",
                "--conf",
                test_conf_location,
                "--repo",
                repo,
                "--online-jar",
                test_conf_location,
            ]
        )
    )
    # from team dev env.
    assert os.environ["EXECUTOR_CORES"] == "2"
    # from team dev env.
    assert os.environ["DRIVER_MEMORY"] == "30G"
    # from default dev env.
    assert os.environ["EXECUTOR_MEMORY"] == "8G"

    # Check conf set environment overrides most.
    reset_env(default_environment)
    run.set_runtime_env(
        parser.parse_args(
            args=[
                "--mode",
                "backfill",
                "--conf",
                "production/joins/sample_team/sample_join.v1",
                "--repo",
                repo,
                "--env",
                "production",
            ]
        )
    )
    # from conf env.
    assert os.environ["EXECUTOR_MEMORY"] == "9G"

    # Bad conf location raises error.
    with pytest.raises(Exception):
        reset_env(default_environment)
        run.set_runtime_env(
            parser.parse_args(
                args=[
                    "--mode",
                    "backfill",
                    "--conf",
                    "joins/sample_team/sample_join.v1",
                    "--repo",
                    repo,
                ]
            )
        )

    # Check metadata export run.py
    reset_env(default_environment)
    run.set_runtime_env(
        parser.parse_args(
            args=[
                "--mode",
                "metadata-export",
                "--conf",
                "production/joins//",
                "--repo",
                repo,
            ]
        )
    )
    # without conf still works.
    assert os.environ["APP_NAME"] == "chronon_joins_metadata_export"

    reset_env(default_environment)
    run.set_runtime_env(
        parser.parse_args(
            args=[
                "--mode",
                "metadata-upload",
                "--conf",
                "production/joins//",
                "--repo",
                repo,
            ]
        )
    )
    assert os.environ["APP_NAME"] == "chronon_joins_metadata_upload"
    reset_env(default_environment)


def test_property_default_update(repo, parser, test_conf_location):
    reset_env(DEFAULT_ENVIRONMENT.copy())
    assert "VERSION" not in os.environ
    args, _ = parser.parse_known_args(args=["--mode", "backfill", "--conf", test_conf_location, "--repo", repo])
    assert args.version is None
    run.set_runtime_env(args)
    assert "VERSION" in os.environ
    assert args.version is None
    run.set_defaults(parser)
    reparsed, _ = parser.parse_known_args(args=["--mode", "backfill", "--conf", test_conf_location, "--repo", repo])
    assert reparsed.version is not None


def test_render_info_setting_update(repo, parser, test_conf_location):
    default_environment = DEFAULT_ENVIRONMENT.copy()

    run.set_defaults(parser)
    args, _ = parser.parse_known_args(args=["--mode", "info", "--conf", test_conf_location, "--repo", repo])
    run.set_defaults(parser)
    assert args.render_info == os.path.join(".", run.RENDER_INFO_DEFAULT_SCRIPT)

    reset_env(default_environment)
    run.set_runtime_env(args)
    os.environ["CHRONON_REPO_PATH"] = repo
    run.set_defaults(parser)
    args, _ = parser.parse_known_args(args=["--mode", "info", "--conf", test_conf_location, "--repo", repo])
    assert args.render_info == os.path.join(repo, run.RENDER_INFO_DEFAULT_SCRIPT)

    reset_env(default_environment)
    run.set_defaults(parser)
    somewhere = "/tmp/somewhere/script.py"
    args, _ = parser.parse_known_args(
        args=[
            "--mode",
            "info",
            "--conf",
            test_conf_location,
            "--render-info",
            somewhere,
        ]
    )
    assert args.render_info == somewhere


def test_render_info(repo, parser, test_conf_location, monkeypatch):
    actual_cmd = None

    def mock_check_call(cmd):
        nonlocal actual_cmd
        actual_cmd = cmd
        return cmd

    def mock_exists(_):
        return True

    monkeypatch.setattr(run, "check_call", mock_check_call)
    monkeypatch.setattr(os.path, "exists", mock_exists)
    run.set_defaults(parser)
    args, _ = parser.parse_known_args(args=["--mode", "info", "--conf", test_conf_location, "--repo", repo])

    args.args = _
    runner = run.Runner(args, "some.jar")
    runner.run()

    assert run.RENDER_INFO_DEFAULT_SCRIPT in actual_cmd


def test_split_date_range():
    start_date = "2022-01-01"
    end_date = "2022-01-11"
    parallelism = 5
    expected_result = [
        ("2022-01-01", "2022-01-02"),
        ("2022-01-03", "2022-01-04"),
        ("2022-01-05", "2022-01-06"),
        ("2022-01-07", "2022-01-08"),
        ("2022-01-09", "2022-01-11"),
    ]

    result = run.split_date_range(start_date, end_date, parallelism)
    assert result == expected_result
