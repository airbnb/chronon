package ai.chronon.flink.tiling

import ai.chronon.online.GroupByServingInfoParsed

class KeySelector {
  protected[tiling] def getKeySelectionFunction(
                                                 groupByServingInfoParsed: GroupByServingInfoParsed
                                               ): Map[String, Any] => List[Any] = {
    // List uses MurmurHash.seqHash for its .hashCode(), which gives us hashing based on content.
    // (instead of based on the instance, which is the case for Array).
    // See Scala's "scala/src/library/scala/util/hashing/MurmurHash3.scala"
    val groupByKeys: List[String] = groupByServingInfoParsed.groupBy.keyColumns.asScala.toList
    println(
      f"Creating key selection function for Flink app. groupByKeys=$groupByKeys"
    )
    (sparkEvalOutput: Map[String, Any]) => groupByKeys.collect(sparkEvalOutput)
  }
}
