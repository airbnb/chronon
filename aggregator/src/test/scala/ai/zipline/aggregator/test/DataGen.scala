package ai.zipline.aggregator.test

import ai.zipline.aggregator.base.{DataType, _}
import ai.zipline.aggregator.row.Row
import ai.zipline.aggregator.test.CStream._
import ai.zipline.api.Extensions._
import ai.zipline.api.{Constants, TimeUnit, Window}

import scala.reflect.ClassTag
import scala.util.Random

// utility classes to generate random data
abstract class CStream[+T: ClassTag] {
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

object CStream {
  private type JLong = java.lang.Long
  private type JDouble = java.lang.Double
  def genPartitions(count: Int): Array[String] = {
    val today = Constants.Partition.at(System.currentTimeMillis())
    Stream
      .iterate(today) { Constants.Partition.before }
      .take(count)
      .toArray
  }

  class PartitionStream(count: Int) extends CStream[String] {
    val keys: Array[String] = genPartitions(count)
    override def next(): String = Option(roll(keys.length, nullRate = 0)).map(dice => keys(dice.toInt)).get
  }

  class StringStream(count: Int, prefix: String, absenceRatio: Double = 0.2) extends CStream[String] {
    val keyCount: Int = (count * (1 - absenceRatio)).toInt
    val keys: Array[String] = {
      val fullKeySet = (1 until (count + 1)).map(i => s"$prefix$i")
      Random.shuffle(fullKeySet).take(keyCount).toArray
    }

    override def next(): String = Option(roll(keyCount)).map(dice => keys(dice.toInt)).orNull
  }

  class TimeStream(window: Window) extends CStream[Long] {
    private val max = System.currentTimeMillis()
    private val min = max - window.millis

    override def next(): Long = {
      roll(max, min, -1) // timestamps can't be null
    }
  }

  class IntStream(max: Int = 10000) extends CStream[Integer] {
    override def next(): Integer =
      Option(roll(max, 1)).map(dice => Integer.valueOf(dice.toInt)).orNull
  }

  class LongStream(max: Int = 10000) extends CStream[JLong] {
    override def next(): JLong =
      Option(roll(max, 1)).map(java.lang.Long.valueOf(_)).orNull
  }

  class DoubleStream(max: Double = 10000) extends CStream[JDouble] {
    override def next(): JDouble =
      Option(rollDouble(max, 1)).map(java.lang.Double.valueOf(_)).orNull
  }

  class ZippedStream(streams: CStream[Any]*)(tsIndex: Int) extends CStream[TestRow] {
    override def next(): TestRow =
      new TestRow(streams.map(_.next()).toArray: _*)(tsIndex)
  }

  //  The main api: that generates dataframes given certain properties of data
  def gen(columns: Seq[Column], count: Int): RowStreamWithSchema = {
    val schema = columns.map(_.schema)
    val generators = columns.map(_.gen)
    val zippedStream = new ZippedStream(generators: _*)(schema.indexWhere(_._1 == Constants.TimeColumn))
    RowStreamWithSchema(Seq.fill(count) { zippedStream.next() }, schema)
  }
}

case class Column(name: String, `type`: DataType, cardinality: Int) {
  def gen: CStream[Any] =
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

  def schema: (String, DataType) = name -> `type`
}
case class RowStreamWithSchema(rowStream: Seq[TestRow], schema: Seq[(String, DataType)])
