package ai.chronon.spark.stats

import ai.chronon
import ai.chronon.api.Extensions._
import ai.chronon.spark.Extensions._
import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.{Conversions, PartitionRange, TableUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CompareJob {
  /*
  * Navigate the dataframes and compare them and fetch statistics.
  */
  def compare(leftDf: DataFrame, rightDf: DataFrame, keys: Seq[String], mapping: Map[String, String] = Map.empty): DataMetrics = {
    // 1. Check for schema consistency issues
    // TODO: Check for datatypes, number of arguments, mapping to match the cols on each side
    // TODO: Mapping should either be empty or filled out completely no partial mapping is allowed.

    // 2. Build comparison dataframe
    // TODO: Should consider the partition column as a separate thing from keys
    println(s"""Join keys: ${keys.mkString(", ")}
        |Left Schema:
        |${leftDf.schema.pretty}
        |
        |Right Schema:
        |${rightDf.schema.pretty}
        |
        |""".stripMargin)

    // Rename the left data source columns with a suffix (except the keys) to reduce the ambiguity
    val renamedLeftDf = leftDf.schema.fieldNames.foldLeft(leftDf)((df, field) => {
      if (!keys.contains(field)) {
        df.withColumnRenamed(field, s"${field}${CompareMetrics.leftSuffix}")
      } else {
        df
      }
    })

    // 3. Join both the dataframes based on the keys and the partition column
    renamedLeftDf.validateJoinKeys(rightDf, keys)
    val joinedDf = renamedLeftDf.join(rightDf, keys, "full")

    // Rename the right data source columns with a suffix (except the keys) to reduce the ambiguity
    val compareDf = rightDf.schema.fieldNames.foldLeft(joinedDf)((df, field) => {
      if (!keys.contains(field)) {
        df.withColumnRenamed(field, s"${field}${CompareMetrics.rightSuffix}")
      } else {
        df
      }
    })

    val leftChrononSchema = StructType(
      "input",
      Conversions.toChrononSchema(leftDf.schema)
        .filterNot(tup => keys.contains(tup._1))
        .map(tup => StructField(tup._1, tup._2)))

    // 4. Run the consistency check
    val (df, metrics) = CompareMetrics.compute(leftChrononSchema.fields, compareDf)

    // 5. Optionally save the compare results to a table
    // TODO Save the results to a table
    // df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable, tableProperties = tblProperties)

    metrics
  }
}


