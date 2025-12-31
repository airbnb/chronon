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

These tests verify the Python-to-JVM bridge functionality by executing
GroupBy and Source operations through the Py4J interface.

Requirements:
    - Compiled Chronon JARs (run `sbt "++ 2.12.12 publishLocal"` first)
    - PySpark with py4j
"""

import os
import tempfile
from pathlib import Path

import pytest
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType

from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit, Accuracy
from ai.chronon.query import Query, select
from ai.chronon.repo.test_helpers import run_group_by_with_inputs, create_mock_source


def find_jar(target_dir: Path, artifact_name: str) -> Path:
    """Find JAR file by artifact name, handling version variations.

    JARs can be versioned as:
    - artifact_2.12-0.0.110.jar (release)
    - artifact_2.12-0.0.110-SNAPSHOT.jar (snapshot)
    - artifact_2.12-branch-name-0.0.110-SNAPSHOT.jar (branch build)
    """
    # Find all JARs matching the artifact (exclude sources/javadoc)
    pattern = f"{artifact_name}_2.12-*.jar"
    candidates = [
        j for j in target_dir.glob(pattern)
        if not j.name.endswith(("-sources.jar", "-javadoc.jar"))
    ]

    if not candidates:
        raise Exception(
            f"No JAR found for {artifact_name} in {target_dir}. "
            f"Pattern: {pattern}. "
            "Run `sbt '++ 2.12.12 publishLocal'` to build them."
        )

    # Return the first match (there should typically be only one)
    return candidates[0]


@pytest.fixture
def spark() -> SparkSession:
    chronon_root = Path(__file__).parents[3]

    # Define JAR locations
    jar_configs = [
        (chronon_root / "api" / "target" / "scala-2.12", "api"),
        (chronon_root / "spark" / "target" / "scala-2.12", "spark_uber"),
        (chronon_root / "aggregator" / "target" / "scala-2.12", "aggregator"),
        (chronon_root / "online" / "target" / "scala-2.12", "online"),
    ]

    # Find all required JARs
    jars = []
    for target_dir, artifact_name in jar_configs:
        if not target_dir.exists():
            raise Exception(
                f"Chronon JARs not built: {target_dir} does not exist. "
                "Run `sbt '++ 2.12.12 publishLocal'` to build them."
            )
        jars.append(find_jar(target_dir, artifact_name))

    # Create symlinks with unique names for Spark classpath
    temp_jars_dir = tempfile.TemporaryDirectory()
    for n, jar in enumerate(jars, start=1):
        jar_symlink = os.path.join(temp_jars_dir.name, f"{n}.jar")
        os.symlink(jar, jar_symlink)

    spark_conf = (
            SparkConf()
            .set("spark.ui.enabled", "false")
            .set("spark.executor.extraClassPath", temp_jars_dir.name + "/*")
            .set("spark.driver.extraClassPath", temp_jars_dir.name + "/*")
            .set("spark.sql.shuffle.partitions", "10")
            .set("spark.default.parallelism", "10")
            .set("spark.driver.host", "127.0.0.1")
            .setMaster("local[2]")
            .setAppName("chronon-pyspark-tests")
        )

    spark = (
        SparkSession.builder.config(conf=spark_conf)
        .master("local[*]")
        .appName("Unit-tests")
        .getOrCreate()
    )

    return spark


@pytest.fixture
def input_df(spark: SparkSession) -> DataFrame:
    data: list = [
        ("merchant1", "payment", "EUR", "charge-1", 1640995200000, "20220101"),
        ("merchant2", "payment", "GBP", "charge-2", 1640995300000, "20220101"),
        ("merchant1", "payment", "USD", "charge-3", 1640995400000, "20220101"),
    ]

    schema = StructType(
        [
            StructField("merchant", StringType(), True),
            StructField("type", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("charge", StringType(), True),
            StructField("ts", LongType(), True),
            StructField("ds", StringType(), True),
        ]
    )

    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture
def query_df(spark: SparkSession) -> DataFrame:
    data: list = [
        ("merchant1", 1640995500000, "20220101"),
        ("merchant2", 1640995500000, "20220101"),
    ]

    schema = StructType(
        [
            StructField("merchant", StringType(), True),
            StructField("ts", LongType(), True),
            StructField("ds", StringType(), True),
        ]
    )

    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture
def sample_raw_underlying_table(spark: SparkSession) -> DataFrame:
    data: list = [
        ("merchant1", "payment", "EUR", "charge-1", 1640995200, "20220101"),
        ("merchant2", "not a payment", "GBP", "charge-2", 1640995300, "20220101"),
        ("merchant1", "payment", "USD", "charge-3", 1640995400, "20220101"),
    ]

    schema = StructType(
        [
            StructField("merchant", StringType(), True),
            StructField("type", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("charge", StringType(), True),
            StructField("created", LongType(), True),
            StructField("ds", StringType(), True),
        ]
    )

    return spark.createDataFrame(data=data, schema=schema)


@pytest.fixture
def sample_source() -> Source:
    return Source(
        events=EventSource(
            table="events.payments",
            query=Query(
                selects=select(
                    merchant="merchant",
                    action_type="type",
                    currency="currency",
                    charge="charge"
                ),
                wheres=["type='payment'"],
                start_partition="20220101",
                time_column="CAST(created * 1000 as LONG)",
            ),
        )
    )


@pytest.fixture
def sample_group_by(sample_source: Source) -> GroupBy:
    return GroupBy(
        sources=sample_source,
        keys=["merchant"],
        aggregations=[
            Aggregation(
                input_column="charge",
                operation=Operation.COUNT,
                windows=[
                    Window(length=1, timeUnit=TimeUnit.DAYS),
                    Window(length=7, timeUnit=TimeUnit.DAYS),
                    Window(length=30, timeUnit=TimeUnit.DAYS),
                ],
            ),
            Aggregation(
                input_column="currency",
                operation=Operation.LAST,
                windows=[Window(length=1, timeUnit=TimeUnit.DAYS)],
            ),
        ],
        accuracy=Accuracy.TEMPORAL,
        name="sample_group_by",
    )


def test_group_by(
    spark: SparkSession,
    input_df: DataFrame,
    query_df: DataFrame,
    sample_group_by: GroupBy
):
    result_df = run_group_by_with_inputs(sample_group_by, input_df, query_df, spark)
    result_df.show(100, False)

    assert (
        result_df.where("merchant = 'merchant1'").collect()[0]["currency_last_1d"] == "USD"
        and result_df.where("merchant = 'merchant2'").collect()[0]["currency_last_1d"] == "GBP"
    )


def test_source(
    spark: SparkSession,
    sample_source: Source,
    sample_raw_underlying_table: DataFrame
):
    result_df = create_mock_source(
            source=sample_source,
            accuracy=Accuracy.TEMPORAL,
            key_columns=["merchant"],
            mock_underlying_table_df=sample_raw_underlying_table,
            spark=spark,
        )

    assert (
        len(result_df.where("type = 'payment'").collect()) == 2
        and len(result_df.where("type != 'payment'").collect()) == 0
    )
