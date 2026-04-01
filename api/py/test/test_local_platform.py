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
Integration tests for the local testing platform.

These tests use the quickstart sample GroupBy/Join definitions from
test/sample/ with synthetic DataFrames to verify that GroupBy, Join,
and StagingQuery can be executed locally.

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
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from ai.chronon.api.ttypes import GroupBy as GroupByThrift, Join as JoinThrift
from ai.chronon.repo.extract_objects import import_module_set_name
from ai.chronon.pyspark.local import (
    _get_or_create_session,
    register_tables,
    reset_local_session,
    run_local_group_by,
    run_local_join,
)

# Add test/sample to the path so we can import the quickstart definitions
# the same way the sample configs import each other.
_sample_dir = str(Path(__file__).resolve().parent / "sample")
if _sample_dir not in sys.path:
    sys.path.insert(0, _sample_dir)

# Import and set names via the standard Chronon mechanism.
# import_module_set_name derives metaData.name and metaData.team from the
# module's __name__ (e.g. "group_bys.quickstart.purchases" → name
# "quickstart.purchases.v1", team "quickstart").
_purchases_mod = importlib.import_module("group_bys.quickstart.purchases")
import_module_set_name(_purchases_mod, GroupByThrift)

_returns_mod = importlib.import_module("group_bys.quickstart.returns")
import_module_set_name(_returns_mod, GroupByThrift)

_training_set_mod = importlib.import_module("joins.quickstart.training_set")
import_module_set_name(_training_set_mod, JoinThrift)

purchases_gb = _purchases_mod.v1
returns_gb = _returns_mod.v1
training_set_join = _training_set_mod.v1


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
            ("user1", 20.0, 1641168000000, "20220103"),
        ],
        schema=schema,
    )


@pytest.fixture(scope="module")
def returns_df(spark: SparkSession) -> DataFrame:
    """Synthetic return events matching data.returns schema."""
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("refund_amt", DoubleType(), True),
        StructField("ts", LongType(), True),
        StructField("ds", StringType(), True),
    ])
    return spark.createDataFrame(
        [
            ("user1", 10.0, 1641081650000, "20220102"),
            ("user2", 25.0, 1641168100000, "20220103"),
        ],
        schema=schema,
    )


@pytest.fixture(scope="module")
def users_df(spark: SparkSession) -> DataFrame:
    """Synthetic user snapshots matching data.users schema."""
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("account_created_ds", StringType(), True),
        StructField("email_verified", StringType(), True),
        StructField("ds", StringType(), True),
    ])
    return spark.createDataFrame(
        [
            ("user1", "20210601", "true",  "20220101"),
            ("user2", "20211015", "false", "20220101"),
            ("user1", "20210601", "true",  "20220102"),
            ("user2", "20211015", "true",  "20220102"),
            ("user1", "20210601", "true",  "20220103"),
            ("user2", "20211015", "true",  "20220103"),
        ],
        schema=schema,
    )


@pytest.fixture(scope="module")
def checkouts_df(spark: SparkSession) -> DataFrame:
    """Synthetic checkout events matching data.checkouts schema (left side of join)."""
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("ts", LongType(), True),
        StructField("ds", StringType(), True),
    ])
    return spark.createDataFrame(
        [
            ("user1", 1641168100000, "20220103"),
            ("user2", 1641168100000, "20220103"),
        ],
        schema=schema,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_run_local_group_by(purchases_df: DataFrame):
    """Test GroupBy using the quickstart purchases definition."""
    result_df = run_local_group_by(
        group_by=purchases_gb,
        tables={"data.purchases": purchases_df},
        start_date="20220101",
        end_date="20220103",
    )
    assert result_df is not None
    assert result_df.count() > 0


def test_run_local_join(
    purchases_df: DataFrame,
    returns_df: DataFrame,
    users_df: DataFrame,
    checkouts_df: DataFrame,
):
    """Test Join using the quickstart training_set definition."""
    result_df = run_local_join(
        join=training_set_join,
        tables={
            "data.purchases": purchases_df,
            "data.returns": returns_df,
            "data.users": users_df,
            "data.checkouts": checkouts_df,
        },
        start_date="20220101",
        end_date="20220103",
    )
    assert result_df is not None
    assert result_df.count() > 0


def test_session_reuse():
    """Verify the singleton session is reused across calls."""
    session1 = _get_or_create_session()
    session2 = _get_or_create_session()
    assert session1 is session2


def test_register_tables_missing_partition_column(spark: SparkSession):
    """Verify that register_tables raises ValueError for missing partition column."""
    bad_df = spark.createDataFrame(
        [("a", 1)],
        StructType([
            StructField("name", StringType(), True),
            StructField("value", LongType(), True),
        ]),
    )
    with pytest.raises(ValueError, match="missing the partition column"):
        register_tables(spark, {"test.bad_table": bad_df})


def test_reset_local_session(spark: SparkSession):
    """Verify that reset clears state and a new session can be created."""
    reset_local_session()
    new_session = _get_or_create_session()
    # After reset, we should get a different session object
    assert new_session is not spark
