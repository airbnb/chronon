package ai.chronon.spark.test

import ai.chronon.spark.TableUtils

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
