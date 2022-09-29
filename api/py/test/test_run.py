"""
Basic tests for namespace and breaking changes in run.py
"""
from ai.chronon.repo import run
import pytest
import time


def test_download_jar(monkeypatch, sleepless):
    def mock_cmd(url, path):
        return url
    monkeypatch.setattr(time, 'sleep', sleepless)
    monkeypatch.setattr(run, 'download_only_once', mock_cmd)
    jar_path = run.download_jar("version", jar_type="uber", release_tag=None, spark_version='2.4.0')
    assert jar_path == "/tmp/spark_uber_2.11-version-assembly.jar"
    jar_path = run.download_jar("version", jar_type="uber", release_tag=None, spark_version='3.1.1')
    assert jar_path == "/tmp/spark_uber_2.12-version-assembly.jar"
    with pytest.raises(Exception):
        run.download_jar("version", jar_type="uber", release_tag=None, spark_version='2.1.0')
