package ai.zipline.spark.test

import ai.zipline.aggregator.base._
import ai.zipline.aggregator.test.{CStream, Column, RowsWithSchema}
import ai.zipline.api.{Constants, LongType, StringType}
import ai.zipline.spark.Conversions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ai.zipline.spark.Extensions._

import scala.collection.JavaConverters._

// This class generates dataframes given certain dataTypes, cardinalities and rowCounts of data
// Nulls are injected for all types
// String types are nulled at row level and also at the set level (some strings are always absent)
object DataFrameGen {
  //  The main api: that generates dataframes given certain properties of data
  def gen(spark: SparkSession, columns: Seq[Column], count: Int): DataFrame = {
    val RowsWithSchema(rows, schema) = CStream.gen(columns, count)
    val genericRows = rows.map { row => new GenericRow(row.fieldsSeq.toArray) }.toArray
    val data: RDD[Row] = spark.sparkContext.parallelize(genericRows)
    val sparkSchema = Conversions.fromZiplineSchema(schema)
    spark.createDataFrame(data, Conversions.fromZiplineSchema(schema))
  }

  //  The main api: that generates dataframes given certain properties of data
  def events(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int): DataFrame = {
    val generated = gen(spark, columns :+ Column(Constants.TimeColumn, LongType, partitions), count)
    generated.withColumn(
      Constants.PartitionColumn,
      from_unixtime((generated.col(Constants.TimeColumn) / 1000) + 86400, Constants.Partition.format))
  }

  //  Generates Entity data
  def entities(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int): DataFrame = {
    gen(spark, columns :+ Column(Constants.PartitionColumn, StringType, partitions), count)
  }
}
