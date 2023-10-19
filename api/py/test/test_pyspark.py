
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType

from ai.chronon.repo.test_helpers import run_group_by_with_inputs, create_mock_source
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit, Accuracy
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select

import pytest
import tempfile
import os
import re

@pytest.fixture
def spark() -> SparkSession:
    cwd = os.getcwd()
    chronon_root = cwd[0: cwd.find("chronon") + len("chronon")]

    chronon_version = None

    with open("{}/version.sbt".format(chronon_root), 'r') as file:  
        data = file.read().replace(' ', '')  
        version_match = re.search(r'version:="(.*?)"', data)

        if version_match:  
            chronon_version = version_match.group(1)
        else: 
            raise Exception("Unable to find version in {}/version.sbt".format(chronon_root))
            
    temp_jars_dir = tempfile.TemporaryDirectory()
    jars = [
        "{}/api/target/scala-2.12/api_2.12-{}.jar".format(chronon_root, chronon_version),
        "{}/spark/target/scala-2.12/spark_uber_2.12-{}.jar".format(chronon_root, chronon_version),
        "{}/aggregator/target/scala-2.12/aggregator_2.12-{}.jar".format(chronon_root, chronon_version),
        "{}/online/target/scala-2.12/online_2.12-{}.jar".format(chronon_root, chronon_version),
    ]

    # We run a counter here to ensure that the jars have unique symlink names.
    n = 1
    for jar in jars:
        split_name = os.path.splitext(os.path.basename(jar))
        if not os.path.isfile(jar):
            raise Exception("Unable to locate jar: {}.\nTry running `sbt \"++ 2.12.12 publishLocal\" first.".format(jar))

        jar_symlink = os.path.join(temp_jars_dir.name, f"{n}{split_name[1]}")
        os.symlink(os.path.abspath(jar), jar_symlink)
        n += 1
    
    spark_conf = (
            SparkConf()
            .set("spark.stripe.testMode.enabled", "true")
            .set("spark.ui.enabled", "false")
            .set("spark.executor.extraClassPath", temp_jars_dir.name + "/*")
            .set("spark.driver.extraClassPath", temp_jars_dir.name + "/*")
            .set("spark.sql.shuffle.partitions", "10")
            .set("spark.default.parallelism", "10")
            .set("spark.hadoop.stripe.testMode.enabled", "true")
            .set("spark.driver.host", "127.0.0.1")
            .setMaster("local[2]")
            .setAppName("tests")
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
                    merchant="merchant", action_type="type", currency="currency", charge="charge"
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
        accuracy=Accuracy.TEMPORAL
    )

def test_group_by(spark: SparkSession, input_df: DataFrame, query_df: DataFrame, sample_group_by: GroupBy):

    result_df = run_group_by_with_inputs(sample_group_by, input_df, query_df, spark)
    result_df.show(100, False)

    assert (
        result_df.where("merchant = 'merchant1'").collect()[0]["currency_last_1d"] == "USD"
        and result_df.where("merchant = 'merchant2'").collect()[0]["currency_last_1d"] == "GBP"
    )

def test_source(spark: SparkSession, sample_source: Source, sample_raw_underlying_table: DataFrame):
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

