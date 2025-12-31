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
Test helpers for PySpark integration testing.

Provides utility functions to bridge Python GroupBy/Source definitions
to their Scala counterparts via Py4J for testing purposes.
"""

from __future__ import annotations
from typing import List

from ai.chronon.group_by import Accuracy, GroupBy
from ai.chronon.api.ttypes import Source
from ai.chronon.repo.serializer import thrift_simple_json
from py4j.java_gateway import JavaClass, JavaGateway, JVMView
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def create_mock_group_by(gb: GroupBy, input_df: DataFrame, spark: SparkSession) -> JavaClass:
    """
    Creates the scala spark GroupBy equivalent of the passed in python GroupBy.
    The resulting scala spark GroupBy can be used to run a group by.
    Note that this only creates a group by with the same aggregations and key columns as the python GroupBy,
    and does not include other properties or things such as derivations.

    :param gb: The python GroupBy that we will mock the aggregations and key columns from
    :param input_df: The input dataframe that will represent our event source
    :param spark: The spark session that we will use to run the group by
    """
    java_gateway: JavaGateway = spark._sc._gateway
    jvm: JVMView = java_gateway.jvm

    # In order to create a Java ai.chronon.spark.GroupBy, we need Java representations of our python group-by's aggregations.
    # To do this we transform a python ai.chronon.group_by GroupBy to a Java ai.chronon.api GroupBy
    # we need to convert the py obj to a json thrift obj, which in return can be parsed to a Java ai.chronon.api GroupBy
    group_by_json: str = thrift_simple_json(gb)
    java_thrift_group_by: JavaClass = jvm.ai.chronon.spark.PySparkUtils.parseGroupBy(
        group_by_json
    )

    # We do not support mutation dataframes and currently only support group bys that use event sources and temporal accuracy
    java_spark_group_by: JavaClass = jvm.ai.chronon.spark.GroupBy.usingArrayList(
        java_thrift_group_by.getAggregations(),
        java_thrift_group_by.getKeyColumns(),
        input_df._jdf,
        None,
        java_gateway.jvm.ai.chronon.spark.PySparkUtils.getStringOptional(None),
        True,
    )

    return java_spark_group_by


def run_group_by_with_inputs(gb: GroupBy, input_df: DataFrame, query_df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Generates a group by result by converting the input python GroupBy to a scala spark GroupBy
    and then aggregating on it using the provided input and query dataframes.
    Note that this only processes a group by with the same aggregations and key columns as the python GroupBy,
    and does not include other properties or things such as derivations.

    :param gb: The python GroupBy that we will mock the aggregations and key columns from
    :param input_df: The input dataframe that will represent our event source
    :param query_df: The query dataframe that will represent our queries
    :param spark: The spark session that we will use to run the group by


    Example usage:

    gb = GroupBy(<a group by keyed by merchant that finds the last currency, count and sum of charges over a few different time windows>)
    input_df =
        +---------+-------+--------+--------+-------------+--------+
        | merchant|   type|currency|  charge|           ts|      ds|
        +---------+-------+--------+--------+-------------+--------+
        |merchant1|payment|     EUR|charge-1|1640995200000|20220101|
        |merchant2|payment|     GBP|charge-2|1640995300000|20220101|
        |merchant1|payment|     USD|charge-3|1640995400000|20220101|
        +---------+-------+--------+--------+-------------+--------+
    query_df =
        +---------+-------------+--------+
        | merchant|           ts|      ds|
        +---------+-------------+--------+
        |merchant1|1640995500000|20220101|
        |merchant2|1640995500000|20220101|
        +---------+-------------+--------+

    results ->
        +---------+-------------+--------+---------------+---------------+----------------+----------------+
        | merchant|           ts|      ds|charge_count_1d|charge_count_7d|charge_count_30d|currency_last_1d|
        +---------+-------------+--------+---------------+---------------+----------------+----------------+
        |merchant2|1640995500000|20220101|              1|              1|               1|             GBP|
        |merchant1|1640995500000|20220101|              2|              2|               2|             USD|
        +---------+-------------+--------+---------------+---------------+----------------+----------------+
    """
    java_gateway: JavaGateway = spark._sc._gateway
    jvm: JVMView = java_gateway.jvm

    java_spark_group_by: JavaClass = create_mock_group_by(gb, input_df, spark)

    if gb.accuracy == Accuracy.TEMPORAL:
        temporal_events_result_jdf: JavaClass = java_spark_group_by.temporalEvents(
            query_df._jdf,
            jvm.ai.chronon.spark.PySparkUtils.getTimeRangeOptional(None),
            jvm.ai.chronon.spark.PySparkUtils.getFiveMinuteResolution(),
        )

        result_df = DataFrame(temporal_events_result_jdf.toDF(), spark)

        return result_df

    else:
        raise NotImplementedError(
            "Only group bys with accuracy of ai.chronon.group_by.Accuracy.Temporal are supported currently."
        )


def create_mock_source(source: Source, accuracy: Accuracy, key_columns: List[str], mock_underlying_table_df: DataFrame, spark: SparkSession):
    """
    Generates the query and applies it to a mock underlying table so that users can create tests to make sure sources are working as expected.

    :param source: The source that we will mock the query from
    :param accuracy: Accuracy.TEMPORAL or Accuracy.SNAPSHOT
    :param key_columns: The query dataframe that will represent our queries
    :param mock_underlying_table_df: Dataframe that represents the underlying table that we will apply the query to. The schema should be equal to the schema of the source's events or entities snapshot table.
    :param spark: The spark session that we can use to access the JVM
    """

    java_gateway = spark._sc._gateway
    jvm = java_gateway.jvm

    source_json: str = thrift_simple_json(source)
    java_thrift_source = jvm.ai.chronon.spark.PySparkUtils.parseSource(source_json)

    source_query: str = jvm.ai.chronon.spark.GroupBy.renderUnpartitionedDataSourceQueryWithArrayList(
        java_thrift_source,
        key_columns,
        jvm.ai.chronon.spark.PySparkUtils.getAccuracy(True if accuracy == Accuracy.TEMPORAL else False),
    )

    uncleansed_base_table_name = source.events.table if source.events else source.entities.snapshotTable
    cleansed_base_table_name = uncleansed_base_table_name.replace(".", "_")

    mock_underlying_table_df.createOrReplaceTempView(cleansed_base_table_name)

    cleansed_source_query = source_query.replace(
        uncleansed_base_table_name, cleansed_base_table_name
    )
    print(f"Creating mock source with accuracy {accuracy} (0 for TEMPORAL, 1 for SNAPSHOT) and rendering source query:\n {cleansed_source_query}")

    result_df = spark.sql(cleansed_source_query)

    return result_df
