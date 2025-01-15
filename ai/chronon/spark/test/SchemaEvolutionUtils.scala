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

package ai.chronon.spark.test

import ai.chronon.spark.{LogUtils, SparkSessionBuilder, TableUtils}

object SchemaEvolutionUtils {
  def runLogSchemaGroupBy(mockApi: MockApi, ds: String, backfillStartDate: String): Unit = {
    val schemaGroupByConf = LogUtils.buildLogSchemaGroupBy(
      mockApi.logTable,
      Some("mock/schema_table"),
      Some(mockApi.namespace),
      Some("chronon"),
      Some(backfillStartDate)
    )
    ai.chronon.spark.GroupBy.computeBackfill(
      schemaGroupByConf,
      ds,
      TableUtils(SparkSessionBuilder.build(s"groupBy_${schemaGroupByConf.metaData.name}_backfill"))
    )
  }
}
