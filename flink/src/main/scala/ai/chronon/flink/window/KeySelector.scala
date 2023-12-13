package ai.chronon.flink.window

import ai.chronon.api.GroupBy

import scala.jdk.CollectionConverters._

// TODO: add comment on what a key selector is
object KeySelector {
  /**
   * Given a GroupBy, create a function to key the output of a SparkExprEval operator by the entities defined in the
   * GroupBy. The function returns a List of size equal to the number of keys in the GroupBy.
   *
   * For example, if our GroupBy is "GroupBy(..., keys=["color", "size"], ...), the function will key the Flink
   * SparkExprEval DataStream by color and size, so all events with the same (color, size) are sent to the same
   * operator.
   */
  def getKeySelectionFunction(groupBy: GroupBy): Map[String, Any] => List[Any] = {
    // List uses MurmurHash.seqHash for its .hashCode(), which gives us hashing based on content.
    // (instead of based on the instance, which is the case for Array).
    // See Scala's "scala/src/library/scala/util/hashing/MurmurHash3.scala"
    val groupByKeys: List[String] = groupBy.keyColumns.asScala.toList
    println(
      f"Creating key selection function for Flink app. groupByKeys=$groupByKeys"
    )
    (sparkEvalOutput: Map[String, Any]) => groupByKeys.collect(sparkEvalOutput)
  }
}
