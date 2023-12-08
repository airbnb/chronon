/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.online

import org.slf4j.LoggerFactory
import ai.chronon.api
import ai.chronon.api.{Constants, DataModel}
import ai.chronon.api.DataModel.DataModel
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.Seq
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.{Failure, Success, Try}

case class TopicInfo(name: String, topicType: String, params: Map[String, String])
object TopicInfo {
  // default topic type is kafka
  // kafka://topic_name/schema=my_schema/host=X/port=Y should parse into TopicInfo(topic_name, kafka, {schema: my_schema, host: X, port Y})
  def parse(topic: String): TopicInfo = {
    assert(topic.nonEmpty, s"invalid topic: $topic")
    val (topicType, rest) = if (topic.contains("://")) {
      val tokens = topic.split("://", 2)
      tokens.head -> tokens.last
    } else {
      "kafka" -> topic
    }
    assert(rest.nonEmpty, s"invalid topic: $topic")
    val fields = rest.split("/")
    val topicName = fields.head
    val params = fields.tail.map { f =>
      val kv = f.split("=", 2); kv.head -> kv.last
    }.toMap
    TopicInfo(topicName, topicType, params)
  }
}

case class DataStream(df: DataFrame, partitions: Int, topicInfo: TopicInfo) {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  // apply a query to a given data stream
  def apply(query: api.Query, keys: Seq[String] = null, dataModel: DataModel = DataModel.Events): DataStream = {
    // apply setups
    Option(query.setups).map(_.toScala.map { setup =>
      Try(df.sparkSession.sql(setup)) match {
        case Failure(ex) =>
          logger.error(s"[Failure] Setup command: ($setup) failed with exception: ${ex.toString}")
          ex.printStackTrace(System.out)
        case Success(value) => logger.info(s"[SUCCESS] Setup command: $setup")
      }
    })

    // enrich selects with time columns & keys
    val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
    // In theory timeColumn for entities is only necessary when windows are specified.
    // TODO: Explore whether timeColumn for entities can be dropped in life-time aggregate cases
    val timeSelects: Map[String, String] = Map(Constants.TimeColumn -> timeColumn) ++ (dataModel match {
      // these are derived from Mutation class for streaming case - we ignore what is set in conf
      case DataModel.Entities => Map(Constants.ReversalColumn -> null, Constants.MutationTimeColumn -> null)
      case DataModel.Events   => Map.empty
    })
    val selectsOption: Option[Map[String, String]] = for {
      selectMap <- Option(query.selects).map(_.toScala.toMap)
      keyMap = Option(keys).map(_.map(k => k -> k).toMap).getOrElse(Map.empty)
    } yield (keyMap ++ selectMap ++ timeSelects)
    val selectClauses = selectsOption.map { _.map { case (name, expr) => s"($expr) AS `$name`" }.toSeq }

    logger.info(s"Applying select clauses: $selectClauses")
    val selectedDf = selectClauses.map { selects => df.selectExpr(selects: _*) }.getOrElse(df)

    // enrich where clauses
    val timeIsPresent = dataModel match {
      case api.DataModel.Entities => s"${Constants.MutationTimeColumn} is NOT NULL"
      case api.DataModel.Events   => s"$timeColumn is NOT NULL"
    }

    val atLeastOneKeyIsPresent =
      Option(keys)
        .map(_.map { key => s"${selectsOption.map(_(key)).getOrElse(key)} IS NOT NULL" }
          .mkString(" OR "))
        .map(where => s"($where)")
    val baseWheres = Option(query.wheres).map(_.toScala).getOrElse(Seq.empty[String])
    val whereClauses = baseWheres ++ atLeastOneKeyIsPresent :+ timeIsPresent

    logger.info(s"Applying where clauses: $whereClauses")
    val filteredDf = whereClauses.foldLeft(selectedDf)(_.where(_))
    DataStream(filteredDf, partitions, topicInfo)
  }
}
