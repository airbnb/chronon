package ai.zipline.spark.test

import ai.zipline.aggregator.base._
import ai.zipline.api.Extensions._
import ai.zipline.api.{Constants, TimeUnit, Window}
import ai.zipline.spark.Conversions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.ClassTag
import scala.util.Random

// This class generates dataframes given certain dataTypes, cardinalities and rowCounts of data
// Nulls are injected for all types
// String types are nulled at row level and also at the set level (some strings are always absent)
object DataGen {
  case class Column(name: String, `type`: DataType, cardinality: Int) {
    protected[DataGen] def gen: CStream[Any] =
      `type` match {
        case StringType =>
          name match {
            case Constants.PartitionColumn => new PartitionStream(cardinality)
            case _                         => new StringStream(cardinality, name)
          }
        case IntType    => new IntStream(cardinality)
        case DoubleType => new DoubleStream(cardinality)
        case LongType =>
          name match {
            case Constants.TimeColumn => new TimeStream(new Window(cardinality, TimeUnit.DAYS))
            case _                    => new LongStream(cardinality)
          }
        case otherType => throw new UnsupportedOperationException(s"Can't generate random data for $otherType yet.")
      }

    protected[DataGen] def schema: (String, DataType) = name -> `type`
  }

  //  The main api: that generates dataframes given certain properties of data
  def gen(spark: SparkSession, columns: Seq[Column], count: Int): DataFrame = {
    val schema = columns.map(_.schema)
    val generators = columns.map(_.gen)
    val sparkSchema = Conversions.fromZiplineSchema(schema.toArray)
    val zippedStream = new ZippedStream(generators: _*)
    val data: RDD[Row] = spark.sparkContext.parallelize((0 until count).map { _ =>
      zippedStream.next()
    })
    spark.createDataFrame(data, sparkSchema)
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

  def genPartitions(count: Int): Array[String] = {
    val today = Constants.Partition.at(System.currentTimeMillis())
    Stream
      .iterate(today) { Constants.Partition.before }
      .take(count)
      .toArray
  }

  private type JLong = java.lang.Long
  private type JDouble = java.lang.Double

  // utility classes to generate random data
  private abstract class CStream[+T: ClassTag] {
    def next(): T

    // roll a dice that gives max to min uniformly, with nulls interspersed as per null rate
    protected def rollDouble(max: JDouble, min: JDouble = 0, nullRate: Double = 0.1): JDouble = {
      val dice: Double = math.random
      if (dice < nullRate) null
      else min + ((max - min) * math.random)
    }

    // roll a dice that gives max to min uniformly, with nulls interspersed as per null rate
    protected def roll(max: JLong, min: JLong = 0, nullRate: Double = 0.1): JLong = {
      val roll = rollDouble(max.toDouble, min.toDouble, nullRate)
      if (roll == null) null else roll.toLong
    }
  }

  private class PartitionStream(count: Int) extends CStream[String] {
    val keys: Array[String] = genPartitions(count)
    override def next(): String = Option(roll(keys.length, nullRate = 0)).map(dice => keys(dice.toInt)).get
  }

  private class StringStream(count: Int, prefix: String, absenceRatio: Double = 0.2) extends CStream[String] {
    val keyCount: Int = (count * (1 - absenceRatio)).toInt
    val keys: Array[String] = {
      val fullKeySet = (1 until (count + 1)).map(i => s"$prefix$i")
      Random.shuffle(fullKeySet).take(keyCount).toArray
    }

    override def next(): String = Option(roll(keyCount)).map(dice => keys(dice.toInt)).orNull
  }

  private class TimeStream(window: Window) extends CStream[Long] {
    private val max = System.currentTimeMillis()
    private val min = max - window.millis

    override def next(): Long = {
      roll(max, min, -1) // timestamps can't be null
    }
  }

  private class IntStream(max: Int = 10000) extends CStream[Integer] {
    override def next(): Integer =
      Option(roll(max, 1)).map(dice => Integer.valueOf(dice.toInt)).orNull
  }

  private class LongStream(max: Int = 10000) extends CStream[JLong] {
    override def next(): JLong =
      Option(roll(max, 1)).map(java.lang.Long.valueOf(_)).orNull
  }

  private class DoubleStream(max: Double = 10000) extends CStream[JDouble] {
    override def next(): JDouble =
      Option(rollDouble(max, 1)).map(java.lang.Double.valueOf(_)).orNull
  }

  private class ZippedStream(streams: CStream[Any]*) extends CStream[GenericRow] {
    override def next(): GenericRow =
      new GenericRow(streams.map(_.next()).toArray)
  }

}
