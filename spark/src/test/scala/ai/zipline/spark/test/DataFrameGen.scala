package ai.zipline.spark.test

import ai.zipline.aggregator.base._
import ai.zipline.aggregator.test.{CStream, Column, RowsWithSchema}
import ai.zipline.api.{Constants, LongType, StringType}
import ai.zipline.spark.Conversions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.{col, from_unixtime, lit, udf, unix_timestamp}
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
    spark.createDataFrame(data, sparkSchema)
  }

  //  The main api: that generates dataframes given certain properties of data
  def events(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int): DataFrame = {
    val generated = gen(spark, columns :+ Column(Constants.TimeColumn, LongType, partitions), count)
    generated.withColumn(Constants.PartitionColumn,
                         from_unixtime(generated.col(Constants.TimeColumn) / 1000, Constants.Partition.format))
  }

  //  Generates Entity data
  def entities(spark: SparkSession, columns: Seq[Column], count: Int, partitions: Int): DataFrame = {
    gen(spark, columns :+ Column(Constants.PartitionColumn, StringType, partitions), count)
  }

  /**
    * Mutations and snapshots generation.
    * To generate data for mutations we first generate random entities events.
    * We set these as insert mutations.
    * Then we take a sample of rows and mutate them with an is_before and after at a mutation_ts after
    * the mutation and before the end of the day.
    * Finally process each snapshot with a group by to generate the snapshot data.
    * This ensures mutations end state and snapshot data is consistent.
    * @return (SnapshotDf, MutationDf)
    */
  def mutations(spark: SparkSession,
                columns: Seq[Column],
                count: Int,
                partitions: Int,
                mutationProbability: Double,
                mutationColumnIdx: Int,
                keyColumnName: String = "listing_id"): (DataFrame, DataFrame) = {
    val mutationColumn = columns(mutationColumnIdx)
    // Randomly generated some entity data, store them as inserts w/ mutation_ts = ts and partition = dsOf[ts].
    val generated = gen(spark, columns :+ Column(Constants.TimeColumn, LongType, partitions), count)
      .withColumn("created_at", col(Constants.TimeColumn))
      .withColumn("updated_at", col(Constants.TimeColumn))
    val withInserts = generated
      .withColumn(Constants.ReversalColumn, lit(false))
      .withColumn(Constants.MutationTimeColumn, col(Constants.TimeColumn))
      .withColumn(Constants.PartitionColumn,
                  from_unixtime((generated.col(Constants.TimeColumn) / 1000), Constants.Partition.format))
      .drop()

    // Sample some of the inserted data and add a mutation time.
    val randomLerp = udf((t: Long, upper: Long) => (t + math.random * (upper - t)).toLong)
    val mutatedFromDf = withInserts
      .sample(mutationProbability)
      .withColumn(Constants.ReversalColumn, lit(true))
      .withColumn(
        Constants.MutationTimeColumn,
        randomLerp(col(Constants.MutationTimeColumn),
                   unix_timestamp(col(Constants.PartitionColumn), Constants.Partition.format) * 1000 + 86400 * 1000)
      )
    val realizedData = spark.sparkContext.parallelize(mutatedFromDf.rdd.collect())
    val realizedFrom = spark.createDataFrame(realizedData, mutatedFromDf.schema)

    // Generate mutation values then store to fix the random value.
    val randomReplace = udf(() => (math.random * mutationColumn.cardinality).round.toDouble)
    val mutatedToDf = realizedFrom
      .withColumn(Constants.ReversalColumn, lit(false))
      .withColumn(mutationColumn.name, randomReplace())
    val realizedTo =
      spark.createDataFrame(spark.sparkContext.parallelize(mutatedToDf.rdd.collect()), mutatedToDf.schema)

    val mutationsDf = withInserts
      .union(realizedFrom)
      .union(realizedTo)

    // Generate snapshot data from mutations by doing a self join
    val seedDf = mutationsDf
      .filter(s"${Constants.ReversalColumn} = false")
      .toDF(mutationsDf.columns.map(_ + "_s"): _*)
    val expandedEventsDf = seedDf
      .as("sd")
      .join(
        mutationsDf.filter(s"${Constants.ReversalColumn} = false").as("mt"),
        col(s"${Constants.PartitionColumn}") <= col(s"${Constants.PartitionColumn}_s")
      )
      .select("mt.*", s"sd.${Constants.PartitionColumn}_s")
      .drop(Constants.PartitionColumn, Constants.ReversalColumn)
      .withColumnRenamed(s"${Constants.PartitionColumn}_s", Constants.PartitionColumn)
      .dropDuplicates()

    // Given expanded events aggregate data such that the snapshot data is consistent with the mutations from the day.
    val aggregator = new SnapshotAggregator(expandedEventsDf.schema, mutationColumn.name, keyColumnName)
    val snapshotRdd = expandedEventsDf.rdd
      .keyBy(aggregator.aggregatorKey(_))
      .aggregateByKey(aggregator.init)(aggregator.update, aggregator.merge)
      .mapValues(aggregator.finalize(_))
      .map {
        case (key, value) => aggregator.toRow(key, value)
      }

    val snapshotDf = spark.createDataFrame(snapshotRdd, aggregator.outputSchema)
    (snapshotDf, mutationsDf)
  }
}
