package ai.chronon.aggregator.windowing

import scala.collection.Seq

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api.Extensions.AggregationOps
import ai.chronon.api._

class TwoStackLiteHopAggregator(inputSchema: StructType,
                                aggregations: Seq[Aggregation],
                                resolution: Resolution = FiveMinuteResolution,
                                shouldFinalize: Boolean = true,
                                inputSize: Int = 1000) {

  // contains unbounded for unwindowed aggregation
  protected val allParts: Seq[AggregationPart] = aggregations.flatMap(_.unpack)
  private val inputSchemaTuples: Array[(String, DataType)] = inputSchema.fields.map(f => f.name -> f.fieldType)
  private val perWindowAggregators: Array[PerWindowAggregator] = allParts.iterator.zipWithIndex.toArray
    .filter { case (p, _) => p.window != null }
    .groupBy { case (p, _) => p.window }
    .map {
      case (w, ps) =>
        val parts = ps.map(_._1)
        val idxs = ps.map(_._2)
        PerWindowAggregator(w, resolution, new RowAggregator(inputSchemaTuples, parts), idxs)
    }
    .toArray

  private val buffers = perWindowAggregators.map(_.bankersBuffer(inputSize))
  private val currentHops = new Array[BankersEntry[Array[Any]]](perWindowAggregators.length)

  def update(row: Row): Unit = {
    var i = 0

    // remove all unwanted entries before adding new entries - to keep memory low
    while (i < perWindowAggregators.length) {
      evictStaleEntry(i, row.ts)
      val perWindowAggregator = perWindowAggregators(i)
      val buffer = buffers(i)
      val hopStart = perWindowAggregator.hopStart(row.ts)

      if (currentHops(i) == null) {
        // Nothing buffered in the hop yet
        currentHops(i) = BankersEntry(perWindowAggregator.agg.prepare(row), hopStart)
      } else if (hopStart == currentHops(i).ts) {
        // The new row shares the same hop as the current buffered hop
        perWindowAggregator.agg.update(currentHops(i).value, row)
      } else {
        // The new row is not in the current buffered hop, push the current buffer into the two-stack and start a new
        // buffer
        buffer.extend(currentHops(i).value, currentHops(i).ts)
        currentHops(i) = BankersEntry(perWindowAggregator.agg.prepare(row), hopStart)
      }
      i += 1
    }
  }

  def query(queryTs: Long): Array[Any] = {
    // buffer contains only relevant events now - query the buffer and update the result
    val result = new Array[Any](allParts.length)
    var i = 0
    while (i < perWindowAggregators.length) {
      // remove all unwanted entries before adding new entries - to keep memory low
      evictStaleEntry(i, queryTs)

      val perWindowOutput = aggregate(i)

      // arrange the perWindowOutput into the indices expected by final output
      if (perWindowOutput != null) {
        val perWindowAggregator = perWindowAggregators(i)
        val indexMapping = perWindowAggregator.indexMapping
        var j = 0
        while (j < indexMapping.length) {
          result.update(indexMapping(j), perWindowOutput(j))
          j += 1
        }
      }
      i += 1
    }

    result
  }

  private def evictStaleEntry(idx: Int, queryTs: Long): Unit = {
    val perWindowAggregator = perWindowAggregators(idx)
    val buffer = buffers(idx)
    val queryTail = perWindowAggregator.tailTs(queryTs)
    while (buffer.peekBack() != null && buffer.peekBack().ts < queryTail) {
      buffer.pop()
    }

    if (currentHops(idx) != null && queryTail > currentHops(idx).ts) {
      currentHops(idx) = null
    }
  }

  private def aggregate(idx: Int): Array[Any] = {
    val perWindowAggregator = perWindowAggregators(idx)
    val buffer = buffers(idx)
    val bufferIr = buffer.query
    val finalIr =
      if (currentHops(idx) == null)
        bufferIr
      else
        perWindowAggregator.agg.merge(bufferIr, currentHops(idx).value)

    if (finalIr == null) {
      perWindowAggregator.init
    } else if (shouldFinalize) {
      perWindowAggregator.agg.finalize(finalIr)
    } else {
      finalIr
    }
  }
}
