package ai.chronon.aggregator.windowing

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.aggregator.windowing.HopsAggregator.{HopIr, IrMapType}
import ai.chronon.api.Extensions.{AggregationOps, AggregationsOps, WindowOps, WindowUtils}
import ai.chronon.api.{Aggregation, DataType, PartitionSpec}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.slf4j.LoggerFactory

import java.util

class HopsAggregatorDF(finalize: Boolean = true,
                       endTimes: Array[Long],
                       shiftedEndTimes: Array[Long],
                       aggregations: Seq[Aggregation],
                       inputSchema: Seq[(String, DataType)],
                       postAggSchema: StructType,
                       partitionSpec: PartitionSpec,
                       resolution: Resolution, tsIndex: Int) extends Aggregator[Row, IrMapType, Row] {


  private val minQueryTs = endTimes.min
  implicit val rowEncoder: Encoder[Row] = Encoders.kryo[Row]
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  private val hopSizes: Array[Long] = resolution.hopSizes
  @transient lazy val rowAggregator =
    new RowAggregator(inputSchema, aggregations.flatMap(_.unWindowed))

  override def zero: IrMapType = {
    Array.fill(hopSizes.length)(new java.util.HashMap[Long, HopIr])
  }

  @transient
  lazy val javaBuildHop: java.util.function.Function[Long, HopIr] = new java.util.function.Function[Long, HopIr] {
    override def apply(ts: Long): HopIr = {
      val v = new Array[Any](rowAggregator.length + 1)
      v.update(rowAggregator.length, ts)
      v
    }
  }

  val leftBoundaries: Array[Option[Long]] = {
    // Nikhil is pretty confident we won't call this when aggregations is empty
    val allWindows = aggregations.allWindowsOpt.get
      .map { window =>
        Option(window).getOrElse(WindowUtils.Unbounded)
      } // agg.windows(i) = Null => one of the windows is "unwindowed"

    // Use the max window for a given tail hop to determine
    // from where(leftBoundary) a particular hops size is relevant
    val hopSizeToMaxWindow =
      allWindows
        .groupBy(resolution.calculateTailHop)
        .mapValues(_.map(_.millis).max)

    val maxHopSize = resolution.calculateTailHop(allWindows.maxBy(_.millis))

    val result: Array[Option[Long]] = resolution.hopSizes.indices.map { hopIndex =>
      val hopSize = resolution.hopSizes(hopIndex)
      // for windows with this hop as the tail hop size
      val windowBasedLeftBoundary = hopSizeToMaxWindow.get(hopSize).map(TsUtils.round(minQueryTs, hopSize) - _)
      // for windows larger with tail hop larger than this hop
      val largerWindowBasedLeftBoundary = if (hopIndex == 0) { // largest window already
        None
      } else { // smaller hop is only used to construct windows' head with larger hopsize.
        val previousHopSize = resolution.hopSizes(hopIndex - 1)
        Some(TsUtils.round(minQueryTs, previousHopSize))
      }
      if (hopSize > maxHopSize) { // this hop size is not relevant
        None
      } else {
        (windowBasedLeftBoundary ++ largerWindowBasedLeftBoundary).reduceOption((a: Long, b: Long) =>
          Ordering[Long].min(a, b))
      }
    }.toArray

    val readableHopSizes = resolution.hopSizes.map(WindowUtils.millisToString)
    val readableLeftBounds = result.map(_.map(TsUtils.toStr).getOrElse("unused"))
    val readableHopsToBoundsMap = readableHopSizes
      .zip(readableLeftBounds)
      .map { case (hop, left) => s"$hop->$left" }
      .mkString(", ")
    logger.info(s"""Left bounds: $readableHopsToBoundsMap
                   |minQueryTs = ${TsUtils.toStr(minQueryTs)}""".stripMargin)
    result
  }

  override def reduce(hopMaps: IrMapType, row: Row): IrMapType = {
    val chrononRow = SparkConversions.toChrononRow(row, tsIndex)
    for (i <- hopSizes.indices) {
      if (leftBoundaries(i).exists(chrononRow.ts >= _)) { // left inclusive
        val hopStart = TsUtils.round(chrononRow.ts, hopSizes(i))
        val hopIr = hopMaps(i).computeIfAbsent(hopStart, javaBuildHop)
        rowAggregator.update(hopIr, chrononRow)
      }
    }
    hopMaps
  }

  override def merge(leftHops: IrMapType, rightHops: IrMapType): IrMapType = {
    if (leftHops == null) return rightHops
    if (rightHops == null) return leftHops
    for (i <- hopSizes.indices) { // left and right will be same size
      val leftMap = leftHops(i)
      val rightIter = rightHops(i).entrySet().iterator()
      while (rightIter.hasNext) {
        val entry = rightIter.next()
        val hopStart = entry.getKey
        val rightIr = entry.getValue
        if (rightIr != null)
          leftMap.put(hopStart, rowAggregator.merge(leftMap.get(hopStart), rightIr))
      }
    }
    leftHops
  }

  // hops have timestamps attached to the end.
  // order by hopStart
  @transient lazy val arrayOrdering: Ordering[HopIr] = new Ordering[HopIr] {
    override def compare(x: HopIr, y: HopIr): Int =
      Ordering[Long]
        .compare(x.last.asInstanceOf[Long], y.last.asInstanceOf[Long])
  }

  override def finish(hopMaps: IrMapType): Row = {
    val sawtoothAggregator = new SawtoothAggregator(aggregations, inputSchema, resolution)
    val hopsmapped = hopMaps.map { m =>
      val resultIt = m.values.iterator()
      val result = new Array[HopIr](m.size())
      for (i <- 0 until m.size()) {
        result.update(i, resultIt.next())
      }
      util.Arrays.sort(result, arrayOrdering)
      result
    }
    // add 1 day to the end times to include data [ds 00:00:00.000, ds + 1 00:00:00.000)
    val irs = sawtoothAggregator.computeWindows(hopsmapped, shiftedEndTimes)
    val allResults: Seq[Array[Any]] = irs.indices.flatMap { i =>
      val result = normalizeOrFinalize(irs(i))
      if (result.forall(_ == null)) None
      else Some(partitionSpec.at(endTimes(i)) +: result)
    }
    val rowArray: Array[Row] = allResults.map(arr => Row.fromSeq(arr.toSeq)).toArray
    Row(rowArray)
  }

  @transient
  lazy val windowAggregator: RowAggregator =
    new RowAggregator(inputSchema, aggregations.flatMap(_.unpack))

  private def normalizeOrFinalize(ir: Array[Any]): Array[Any] =
    if (finalize) {
      windowAggregator.finalize(ir)
    } else {
      windowAggregator.normalize(ir)
    }

  override def bufferEncoder: Encoder[IrMapType] = Encoders.kryo[IrMapType]

  val outputSchema = StructType(Seq(
    StructField("results", ArrayType(postAggSchema), nullable = true)
  ))

  override def outputEncoder: Encoder[Row] = RowEncoder(outputSchema)

}