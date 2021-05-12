package ai.zipline.aggregator.windowing

import java.util.Date
import java.text.SimpleDateFormat
import java.util

import ai.zipline.aggregator.base.DataType
import ai.zipline.aggregator.row.{Row, RowAggregator}
import ai.zipline.aggregator.windowing.HopsAggregator._
import ai.zipline.api.{Aggregation, Window}
import ai.zipline.api.Extensions._
import scala.collection.JavaConverters._

// generate hops per spec, (NOT per window) for the given hop sizes in the resolution
// we use minQueryTs to construct only the relevant hops for a given hop size.
// 180day window , 5hr window (headStart(minTs) - 5hrs, maxTs)
// daily aggregates (headStart(minTs) - 180days, maxTs),
class HopsAggregator(minQueryTs: Long,
                     aggregations: Seq[Aggregation],
                     inputSchema: Seq[(String, DataType)],
                     resolution: Resolution)
    extends Serializable {

  @transient lazy val rowAggregator =
    new RowAggregator(inputSchema, aggregations.map(_.unWindowed))
  val hopSizes: Array[Long] = resolution.hopSizes
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
        (windowBasedLeftBoundary ++ largerWindowBasedLeftBoundary).reduceOption(Ordering[Long].min)
      }
    }.toArray

    val readableHopSizes = resolution.hopSizes.map(WindowUtils.millisToString)
    val readableLeftBounds = result.map(_.map(TsUtils.toStr).getOrElse("unused"))
    val readableHopsToBoundsMap = readableHopSizes
      .zip(readableLeftBounds)
      .map { case (hop, left) => s"$hop->$left" }
      .mkString(", ")
    println(s"""Left bounds: $readableHopsToBoundsMap 
         |minQueryTs = ${TsUtils.toStr(minQueryTs)}""".stripMargin)
    result
  }

  def init(): IrMapType =
    Array.fill(hopSizes.length)(new java.util.HashMap[Long, HopIr])

  @transient
  private lazy val javaBuildHop = new java.util.function.Function[Long, HopIr] {
    override def apply(ts: Long): HopIr = {
      val v = new Array[Any](rowAggregator.length + 1)
      v.update(rowAggregator.length, ts)
      v
    }
  }

  // used to collect hops of various sizes in a single pass of input rows
  def update(hopMaps: IrMapType, row: Row): IrMapType = {
    for (i <- hopSizes.indices) {
      if (leftBoundaries(i).isDefined && row.ts >= leftBoundaries(i).get) { // left inclusive
        val hopStart = TsUtils.round(row.ts, hopSizes(i))
        val hopIr = hopMaps(i).computeIfAbsent(hopStart, javaBuildHop)
        rowAggregator.update(hopIr, row)
      }
    }
    hopMaps
  }

  // Zero-copy merging
  // NOTE: inputs will be mutated in the process, use "clone" if you want re-use references
  def merge(leftHops: IrMapType, rightHops: IrMapType): IrMapType = {
    if (leftHops == null) return rightHops
    if (rightHops == null) return leftHops
    for (i <- hopSizes.indices) { // left and right will be same size
      val leftMap = leftHops(i)
      val rightIter = rightHops(i).entrySet().iterator()
      while (rightIter.hasNext) {
        val entry = rightIter.next()
        val hopStart = entry.getKey
        val rightIr = entry.getValue
        val leftIr = leftMap.get(hopStart)
        if (leftIr != null) { // unfortunate that the option has to be created
          rowAggregator.merge(leftIr, rightIr)
        } else {
          leftMap.put(hopStart, rightIr)
        }
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

  def toTimeSortedArray(hopMaps: IrMapType): OutputArrayType =
    hopMaps.map { m =>
      val resultIt = m.values.iterator()
      val result = new Array[HopIr](m.size())
      for (i <- 0 until m.size()) {
        result.update(i, resultIt.next())
      }
      util.Arrays.sort(result, arrayOrdering)
      result
    }
}

object HopsAggregator {
  // [IR1, IR2, IR3,.... IRN, ts_millis_long]
  // hops have timestamps attached to the end
  type HopIr = Array[Any]
  type OutputArrayType = Array[Array[HopIr]]
  type IrMapType = Array[java.util.HashMap[Long, HopIr]]
}
