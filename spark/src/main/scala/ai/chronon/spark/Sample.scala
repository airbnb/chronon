package ai.chronon.spark

import scala.jdk.CollectionConverters.asScalaBufferConverter

import ai.chronon.api
import ai.chronon.api.Extensions.GroupByOps

class Sample(tableUtils: TableUtils) {

  def getSourceRanges(groupBy: api.GroupBy, ds: String): Seq[(String, PartitionRange)] = {
    val groupByOps = new GroupByOps(groupBy)
    groupBy.sources.asScala.map{ source =>
      val range: PartitionRange = GroupBy.getIntersectedRange(
        source,
        PartitionRange(ds, ds)(tableUtils),
        tableUtils,
        groupByOps.maxWindow)

      
    }

  }
}
