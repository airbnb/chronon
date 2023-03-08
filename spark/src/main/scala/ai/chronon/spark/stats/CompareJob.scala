package ai.chronon.spark.stats

import ai.chronon.api._
import ai.chronon.online.{SparkConversions, _}
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

object CompareJob {

  def checkConsistency(
      leftFields: Map[String, DataType],
      rightFields: Map[String, DataType],
      keys: Seq[String],
      mapping: Map[String, String] = Map.empty,
      migrationCheck: Boolean = false
  ): Unit = {
    // Make sure the number of fields are comparable on either side.
    // For migration checks, the left side can have more fields.
    val sizeCheck = if (migrationCheck) leftFields.size >= rightFields.size else leftFields.size == rightFields.size
    assert(sizeCheck,
           s"""Inconsistent number of fields; left side: ${leftFields.size}, right side: ${rightFields.size}
           |Left side fields:
           | - ${leftFields.mkString("\n - ")}
           |
           |Right side fields:
           | - ${rightFields.mkString("\n - ")}
           |""".stripMargin
    )

    // Verify that the mapping and the datatypes match
    val revMapping = mapping.map(_.swap)
    rightFields.foreach { rightField =>
      val leftFieldName = if (revMapping.contains(rightField._1)) revMapping.get(rightField._1).get else rightField._1
      assert(leftFields.contains(leftFieldName),
             s"Mapping column on the left table is not present; column name: ${leftFieldName}")

      val leftFieldType = leftFields.get(leftFieldName).get
      assert(rightField._2 == leftFieldType,
             s"Comparison data types do not match for column '${leftFieldName}';" +
             s" left side: ${leftFieldType}, right side: ${rightField._2}")
    }

    // Make sure the values of the mapping dict doesn't contain any duplicates
    assert(mapping.size == revMapping.size,
           s"Mapping values contain duplicate values. Keys: ${mapping.keys}, Values: ${mapping.values}")

    // Verify the mapping has unique keys and values and they are all present in the left and right data frames.
    assert(mapping.keySet.subsetOf(leftFields.keySet),
           s"Invalid mapping provided missing fields; provided: ${mapping.keySet}," +
           s" expected to be subset of: ${leftFields.keySet}"
    )
    assert(mapping.values.toSet.subsetOf(rightFields.keySet),
           s"Invalid mapping provided missing fields; provided: ${mapping.values.toSet}," +
           s" expected to be subset of: ${rightFields.keySet}"
    )

    // Make sure the key columns are present in both the frames.
    Seq(leftFields, rightFields).foreach { kset =>
      assert(keys.toSet.subsetOf(kset.keySet),
             s"Some of the primary keys are missing in the source dataframe; provided: ${keys}," +
             s" expected to be subset of: ${kset.keySet}")
    }

    // Make sure the passed keys has one of the time elements in it
    assert(keys.intersect(Constants.ReservedColumns).length != 0, "Ensure that one of the key columns is a time column")
  }

  /*
   * Navigate the dataframes and compare them and fetch statistics.
   */
  def compare(
      leftDf: DataFrame,
      rightDf: DataFrame,
      keys: Seq[String],
      mapping: Map[String, String] = Map.empty,
      migrationCheck: Boolean = false
  ): (DataFrame, DataFrame, DataMetrics) = {
    // 1. Check for schema consistency issues
    val leftFields: Map[String, DataType] = leftDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap
    val rightFields: Map[String, DataType] = rightDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap
    checkConsistency(leftFields, rightFields, keys, mapping, migrationCheck)

    // 2. Prune the extra columns that we may have on the left side for migration use cases
    // so that the comparison becomes consistent across both sides.
    val prunedLeftDf = if (migrationCheck) {
      leftDf.schema.fieldNames.foldLeft(leftDf)((df, field) => {
        val rightFieldName = if (mapping.contains(field)) mapping.get(field).get else field
        if (!rightFields.contains(rightFieldName)) {
          df.drop(field)
        } else {
          df
        }
      })
    } else {
      leftDf
    }

    // 3. Build comparison dataframe
    println(s"""Join keys: ${keys.mkString(", ")}
        |Left Schema:
        |${prunedLeftDf.schema.pretty}
        |
        |Right Schema:
        |${rightDf.schema.pretty}
        |
        |""".stripMargin)

    // Rename the left data source columns with a suffix (except the keys) to reduce the ambiguity
    val renamedLeftDf = prunedLeftDf.schema.fieldNames.foldLeft(prunedLeftDf)((df, field) => {
      if (!keys.contains(field)) {
        df.withColumnRenamed(field, s"${field}${CompareMetrics.leftSuffix}")
      } else {
        df
      }
    })

    // 4.. Join both the dataframes based on the keys and the partition column
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

    val leftChrononSchema = StructType("input",
                                       SparkConversions
                                         .toChrononSchema(prunedLeftDf.schema)
                                         .filterNot(tup => keys.contains(tup._1))
                                         .map(tup => StructField(tup._1, tup._2)))

    // 5. Run the consistency check
    val (metricsDf, metrics) = CompareMetrics.compute(leftChrononSchema.fields, compareDf, keys, mapping)

    // Return the data frame of the actual comparison table, metrics table and the metrics itself
    (compareDf, metricsDf, metrics)
  }
}
