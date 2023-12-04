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

package ai.chronon.spark
import ai.chronon.api.{Accuracy, Builders, Constants, Operation}

object LogUtils {
  def buildLogSchemaGroupBy(logTable: String,
                            name: Option[String] = Some("logging_schema.v1"),
                            namespace: Option[String] = None,
                            team: Option[String] = Some("chronon"),
                            backfillStartDate: Option[String] = None): ai.chronon.api.GroupBy = {
    val groupBy = Builders.GroupBy(
      keyColumns = Seq(s"${Constants.SchemaHash}"),
      sources = Seq(
        Builders.Source.events(
          table = logTable,
          query = Builders.Query(
            selects = Builders.Selects.exprs(
              Constants.SchemaHash -> "decode(unbase64(key_base64), 'utf-8')",
              "schema_value" -> "decode(unbase64(value_base64), 'utf-8')"
            ),
            wheres = Seq(s"name='${Constants.SchemaPublishEvent}'"),
            timeColumn = "ts_millis"
          )
        )
      ),
      aggregations = Seq(
        Builders.Aggregation(
          inputColumn = "schema_value",
          operation = Operation.LAST
        )
      ),
      accuracy = Accuracy.SNAPSHOT,
      metaData = Builders.MetaData()
    )

    name.foreach(name => groupBy.metaData.setName(name))
    namespace.foreach(namespace => groupBy.metaData.setOutputNamespace(namespace))
    team.foreach(team => groupBy.metaData.setTeam(team))
    backfillStartDate.foreach(backfillStartDate => groupBy.setBackfillStartDate(backfillStartDate))

    groupBy
  }
}
