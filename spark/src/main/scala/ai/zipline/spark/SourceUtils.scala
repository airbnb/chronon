package ai.zipline.spark

import ai.zipline.api.{Query, Source}


object SourceUtils {

  def getQuery(source: Source): Query =
    if (source.isSetEntities) source.getEntities.getQuery else source.getEvents.getQuery

  def compareSourcesIgnoringStartEnd(leftSource: Source, rightSource: Source) = {
    // Start and end partitions are often manipulated while testing and backfilling, we don't want these to trigger re-computes
    val leftSourceCopy = leftSource.deepCopy()
    val rightSourceCopy = rightSource.deepCopy()
    val leftSourceQuery = SourceUtils.getQuery(leftSource)
    val rightSourceQuery = SourceUtils.getQuery(rightSource)
    leftSourceQuery.unsetStartPartition()
    leftSourceQuery.unsetEndPartition()
    rightSourceQuery.unsetStartPartition()
    rightSourceQuery.unsetEndPartition()
    leftSourceCopy == rightSourceCopy
  }

}
