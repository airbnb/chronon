package ai.chronon.aggregator.test

import ai.chronon.aggregator.test.CStream._
import ai.chronon.api.Extensions.WindowOps
import ai.chronon.api._

import scala.reflect.ClassTag
import scala.util.Random

// utility classes to generate random data
abstract class CStream[+T: ClassTag] {
  def next(): T

  // roll a dice that gives max to min uniformly, with nulls interspersed as per null rate
  protected def rollDouble(max: JDouble, min: JDouble = 0, nullRate: Double): JDouble = {
    val dice: Double = math.random
    if (dice < nullRate) null
    else min + ((max - min) * math.random)
  }

  // roll a dice that gives max to min uniformly, with nulls interspersed as per null rate
  protected def roll(max: JLong, min: JLong = 0, nullRate: Double): JLong = {
    val roll = rollDouble(max.toDouble, min.toDouble, nullRate)
    if (roll == null) null else roll.toLong
  }

  def gen(count: Int): Seq[T] = {
    Stream.fill(count)(next())
  }

  def chunk(minSize: Long = 0, maxSize: Long = 10, nullRate: Double): CStream[Seq[T]] = {
    def innerNext(): T = next()
    new CStream[Seq[T]] {
      override def next(): Seq[T] = {
        val size = roll(minSize, maxSize, nullRate)
        if (size != null) {
          (0 until size.toInt).map { _ => innerNext() }
        } else {
          null
        }
      }
    }
  }
}

object CStream {
  private type JLong = java.lang.Long
  private type JDouble = java.lang.Double

  def genTimestamps(window: Window,
                    count: Int,
                    roundMillis: Int = 1,
                    maxTs: Long = System.currentTimeMillis()): Array[Long] =
    new CStream.TimeStream(window, roundMillis, maxTs).gen(count).toArray.sorted

  def genPartitions(count: Int): Array[String] = {
    val today = Constants.Partition.at(System.currentTimeMillis())
    Stream
      .iterate(today) {
        Constants.Partition.before
      }
      .take(count)
      .toArray
  }

  class PartitionStream(count: Int) extends CStream[String] {
    val keys: Array[String] = genPartitions(count)
    override def next(): String = Option(roll(keys.length, nullRate = 0)).map(dice => keys(dice.toInt)).get
  }

  class StringStream(count: Int, prefix: String, nullRate: Double) extends CStream[String] {
    val keyCount: Int = (count * (1 - nullRate)).ceil.toInt
    val keys: Array[String] = {
      val fullKeySet = (1 until (count + 1)).map(i => s"$prefix$i")
      Random.shuffle(fullKeySet).take(keyCount).toArray
    }

    override def next(): String = Option(roll(keyCount, nullRate = nullRate)).map(dice => keys(dice.toInt)).orNull
  }

  class TimeStream(window: Window, roundMillis: Long = 1, maxTs: Long = System.currentTimeMillis())
      extends CStream[Long] {
    private val minTs = maxTs - window.millis

    def round(v: Long): Long = (v / roundMillis) * roundMillis

    override def next(): Long = {
      round(roll(maxTs, minTs, -1)) // timestamps can't be null
    }
  }

  class IntStream(max: Int = 10000, nullRate: Double = 0.1) extends CStream[Integer] {
    override def next(): Integer =
      Option(roll(max, 1, nullRate)).map(dice => Integer.valueOf(dice.toInt)).orNull
  }

  class LongStream(max: Int = 10000, nullRate: Double = 0.1) extends CStream[JLong] {
    override def next(): JLong =
      Option(roll(max, 1, nullRate)).map(java.lang.Long.valueOf(_)).orNull
  }

  class DoubleStream(max: Double = 10000, nullRate: Double = 0.1) extends CStream[JDouble] {
    override def next(): JDouble =
      Option(rollDouble(max, 1, nullRate)).map(java.lang.Double.valueOf(_)).orNull
  }

  class ZippedStream(streams: CStream[Any]*)(tsIndex: Int) extends CStream[TestRow] {
    override def next(): TestRow =
      new TestRow(streams.map(_.next()).toArray: _*)(tsIndex)
  }

  //  The main api: that generates dataframes given certain properties of data
  def gen(columns: Seq[Column], count: Int): RowsWithSchema = {
    val schema = columns.map(_.schema)
    val generators = columns.map(_.gen)
    val zippedStream = new ZippedStream(generators: _*)(schema.indexWhere(_._1 == Constants.TimeColumn))
    RowsWithSchema(Seq.fill(count) { zippedStream.next() }.toArray, schema)
  }
}

case class Column(name: String, `type`: DataType, cardinality: Int, chunkSize: Int = 10, nullRate: Double = 0.1) {
  def genImpl(dtype: DataType): CStream[Any] =
    dtype match {
      case StringType =>
        name match {
          case Constants.PartitionColumn => new PartitionStream(cardinality)
          case _                         => new StringStream(cardinality, name, nullRate)
        }
      case IntType    => new IntStream(cardinality, nullRate)
      case DoubleType => new DoubleStream(cardinality, nullRate)
      case LongType =>
        name match {
          case Constants.TimeColumn => new TimeStream(new Window(cardinality, TimeUnit.DAYS))
          case _                    => new LongStream(cardinality, nullRate)
        }
      case ListType(elementType) => genImpl(elementType).chunk(chunkSize, nullRate = nullRate)
      case otherType             => throw new UnsupportedOperationException(s"Can't generate random data for $otherType yet.")
    }

  def gen: CStream[Any] = genImpl(`type`)
  def schema: (String, DataType) = name -> `type`
}
case class RowsWithSchema(rows: Array[TestRow], schema: Seq[(String, DataType)])
