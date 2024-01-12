package ai.chronon.spark

import ai.chronon.api

class Sample(tableUtils: TableUtils) {

  /*
  def getSourceRanges(groupBy: api.GroupBy): Seq[(String, PartitionRange)] = {
    groupBy.sources.asScala.map{ source =>
      val range: PartitionRange = GroupBy.getIntersectedRange(
        source,
        PartitionRange("", "")(tableUtils),
        tableUtils)
    }


  }
   */
}
