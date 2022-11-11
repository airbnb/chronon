package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.DataModel.{Entities, Events}
import ai.chronon.api.Extensions._
import ai.chronon.api.{Constants, TimeUnit, Window}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

class LabelJoin(joinConf: api.Join, startOffset: Int, endOffset: Int, tableUtils: TableUtils,
                labelDS: String,
                labelPartitionName: String = Constants.LabelPartitionColumn) {
  assert(Option(joinConf.metaData.outputNamespace).nonEmpty, s"output namespace could not be empty or null")

  assert(startOffset >= endOffset, s"Start time offset ${startOffset} must be earlier than end offset ${endOffset}")

  val leftStart = Constants.Partition.minus(labelDS, new Window(startOffset, TimeUnit.DAYS))
  val leftEnd = Constants.Partition.minus(labelDS, new Window(endOffset, TimeUnit.DAYS))

  def computeLabelJoin(stepDays: Option[Int] = None): DataFrame = {

    assert(Option(joinConf.left.dataModel).equals(Option(Events)),
           s"join.left.dataMode needs to be Events for label join ${joinConf.metaData.name}")

    joinConf.joinParts.asScala.foreach { jp =>
      assert(Option(jp.groupBy.dataModel).equals(Option(Entities)),
             s"groupBy.dataModel must be Entities for label join ${jp.groupBy.metaData.name}")

      assert(Option(jp.groupBy.aggregations).isEmpty,
             s"groupBy.aggregations not yet supported for label join ${jp.groupBy.metaData.name}")
    }

    // update time window
    println(s"Join label window: ${joinConf.left.query.startPartition} - ${joinConf.left.query.endPartition}")
    val labelJoinLeft = joinConf.left.updateQueryPartitions(Option(leftStart), Option(leftEnd))
    println(s"Updated join label window: ${joinConf.left.query.startPartition} - ${joinConf.left.query.endPartition}")

    val labelJoin = joinConf.setLeft(labelJoinLeft)
    val runner = new Join(labelJoin, leftEnd, tableUtils, true)
    runner.computeJoin(stepDays, Option(labelDS), Option(labelPartitionName))
  }
}
