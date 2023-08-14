from __future__ import annotations


from ai.chronon.group_by import Accuracy, GroupBy
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
    group_by_json = thrift_simple_json(gb)
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


def run_with_inputs(gb: GroupBy, input_df: DataFrame, query_df: DataFrame, spark: SparkSession) -> DataFrame:
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

        result_df = DataFrame(temporal_events_result_jdf.toDF(), spark._wrapped)

        return result_df

    else:
        raise NotImplementedError(
            "Only group bys with accuracy of ai.chronon.group_by.Accuracy.Temporal are supported currently."
        )