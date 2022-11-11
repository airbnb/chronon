package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Builders, Constants, Source, TimeUnit, Window}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class LabelJoin(labelJoinConf: api.LabelJoin, left: Source, tableUtils: TableUtils,
                labelDS: String,
                labelPartitionName: String = Constants.LabelPartitionColumn) {
  assert(Option(labelJoinConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")

  assert(labelJoinConf.leftStartOffset >= labelJoinConf.getLeftEndOffset,
    s"Start time offset ${labelJoinConf.leftStartOffset} must be earlier than end offset " +
      s"${labelJoinConf.leftEndOffset}")

  val leftStart = Constants.Partition.minus(labelDS, new Window(labelJoinConf.leftStartOffset, TimeUnit.DAYS))
  val leftEnd = Constants.Partition.minus(labelDS, new Window(labelJoinConf.leftEndOffset, TimeUnit.DAYS))

  def computeLabelJoin(stepDays: Option[Int] = None): DataFrame = {

    assert(Option(left.dataModel).equals(Option(Events)),
           s"join.left.dataMode needs to be Events for label join ${labelJoinConf.metaData.name}")

    labelJoinConf.labels.asScala.foreach { jp =>
      assert(Option(jp.groupBy.dataModel).equals(Option(Entities)),
             s"groupBy.dataModel must be Entities for label join ${jp.groupBy.metaData.name}")

      assert(Option(jp.groupBy.aggregations).isEmpty,
             s"groupBy.aggregations not yet supported for label join ${jp.groupBy.metaData.name}")
    }

    // update time window
    println(s"Join label window: ${left.query.startPartition} - ${left.query.endPartition}")
    val labelJoinLeft = left.updateQueryPartitions(Option(leftStart), Option(leftEnd))
    println(s"Updated join label window: ${left.query.startPartition} - ${left.query.endPartition}")

    val labelJoin = Builders.Join(
      labelJoinConf.metaData,
      labelJoinLeft,
      labelJoinConf.labels.asScala
    )
    val runner = new Join(labelJoin, leftEnd, tableUtils, true)
    runner.computeJoin(stepDays, Option(labelDS), Option(labelPartitionName))
  }
}
