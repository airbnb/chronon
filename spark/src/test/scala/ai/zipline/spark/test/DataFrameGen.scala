package ai.zipline.spark.test

import ai.zipline.aggregator.base._
import ai.zipline.aggregator.test.CStream.gen
import ai.zipline.aggregator.test.{Column, RowStreamWithSchema}
import ai.zipline.api.Constants
import ai.zipline.spark.Conversions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.JavaConverters._

// This class generates dataframes given certain dataTypes, cardinalities and rowCounts of data
// Nulls are injected for all types
// String types are nulled at row level and also at the set level (some strings are always absent)
object DataFrameGen {
  //  The main api: that generates dataframes given certain properties of data
  def genRows(spark: SparkSession, columns: Seq[Column], count: Int): DataFrame = {
    val RowStreamWithSchema(rows, schema) = gen(columns, count)
    val data: RDD[Row] = spark.sparkContext.parallelize(rows.map { row => new GenericRow(row.fields.asScala.toArray) })
    spark.createDataFrame(data, Conversions.fromZiplineSchema(schema.toArray))
  }

  //  The main api: that generates dataframes given certain properties of data
  def events(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int): DataFrame = {
    val generated = genRows(spark, columns :+ Column(Constants.TimeColumn, LongType, partitions), count)
    generated.withColumn(
      Constants.PartitionColumn,
      from_unixtime((generated.col(Constants.TimeColumn) / 1000) + 86400, Constants.Partition.format))
  }

  //  Generates Entity data
  def entities(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int): DataFrame = {
    genRows(spark, columns :+ Column(Constants.PartitionColumn, StringType, partitions), count)
  }
}
