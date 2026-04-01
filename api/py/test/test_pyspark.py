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

"""
PySpark integration tests for Chronon.

These tests verify GroupBy execution through the local testing platform
using the quickstart sample definitions.

Requirements:
    - Compiled Chronon JARs (run `sbt "++ 2.12.12 publishLocal"` first)
    - PySpark with py4j and Hive support
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from ai.chronon.api.ttypes import GroupBy as GroupByThrift
from ai.chronon.repo.extract_objects import import_module_set_name
from ai.chronon.pyspark.local import (
    _get_or_create_session,
    reset_local_session,
    run_local_group_by,
)

# Add test/sample to the path so we can import the quickstart definitions.
_sample_dir = str(Path(__file__).resolve().parent / "sample")
if _sample_dir not in sys.path:
    sys.path.insert(0, _sample_dir)

_purchases_mod = importlib.import_module("group_bys.quickstart.purchases")
import_module_set_name(_purchases_mod, GroupByThrift)

purchases_gb = _purchases_mod.v1


@pytest.fixture(scope="module", autouse=True)
def cleanup():
    """Ensure the local session is reset after all tests in this module."""
    yield
    reset_local_session()


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    """Get the singleton local SparkSession."""
    return _get_or_create_session()


@pytest.fixture(scope="module")
def purchases_df(spark: SparkSession) -> DataFrame:
    """Synthetic purchase events matching data.purchases schema."""
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("purchase_price", DoubleType(), True),
        StructField("ts", LongType(), True),
        StructField("ds", StringType(), True),
    ])
    return spark.createDataFrame(
        [
            ("user1", 10.0, 1640995200000, "20220101"),
            ("user2", 25.0, 1640995300000, "20220101"),
            ("user1", 15.0, 1640995400000, "20220101"),
            ("user1", 30.0, 1641081600000, "20220102"),
            ("user2", 5.0,  1641081700000, "20220102"),
        ],
        schema=schema,
    )


def test_group_by(purchases_df: DataFrame):
    """Test that the quickstart purchases GroupBy runs and produces results."""
    result_df = run_local_group_by(
        group_by=purchases_gb,
        tables={"data.purchases": purchases_df},
        start_date="20220101",
        end_date="20220102",
    )
    assert result_df is not None
    assert result_df.count() > 0
