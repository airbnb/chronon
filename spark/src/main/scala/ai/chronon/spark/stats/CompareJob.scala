package ai.chronon.spark.stats

import ai.chronon.api._
import ai.chronon.online._
import ai.chronon.spark.Conversions
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

object CompareJob {

  def checkConsistency(
      leftDf: DataFrame,
      rightDf: DataFrame,
      keys: Seq[String],
      mapping: Map[String, String] = Map.empty
  ): Unit = {
    val leftFields: Map[String, DataType] = leftDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap
    val rightFields: Map[String, DataType] = rightDf.schema.fields.map(sb => (sb.name, sb.dataType)).toMap
    // Make sure the number of fields are the same on either side
    assert(leftFields.size == rightFields.size,
      s"Inconsistent number of fields; left side: ${leftFields.size}, right side: ${rightFields.size}")

    // Verify that the mapping and the datatypes match
    leftFields.foreach { leftField =>
      val rightFieldName = if (mapping.contains(leftField._1)) mapping.get(leftField._1).get else leftField._1
      assert(rightFields.contains(rightFieldName),
        s"Mapping column on the right table is not present; column name: ${rightFieldName}")

      // Make sure the data types match for proper comparison
      val rightFieldType = rightFields.get(rightFieldName).get
      assert(leftField._2 == rightFieldType,
        s"Comparison data types do not match; left side: ${leftField._2}, right side: ${rightFieldType}")
    }

    // Verify the mapping has unique keys and values and they are all present in the left and right data frames.
    assert(mapping.keySet.subsetOf(leftFields.keySet),
        s"Invalid mapping provided missing fields; provided: ${mapping.keySet}," +
        s" expected to be subset of: ${leftFields.keySet}")
    assert(mapping.values.toSet.subsetOf(rightFields.keySet),
        s"Invalid mapping provided missing fields; provided: ${mapping.values.toSet}," +
        s" expected to be subset of: ${rightFields.keySet}")

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
      mapping: Map[String, String] = Map.empty
  ): DataMetrics = {
    // 1. Check for schema consistency issues
    checkConsistency(leftDf, rightDf, keys, mapping)

    // 2. Build comparison dataframe
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
    val (df, metrics) = CompareMetrics.compute(leftChrononSchema.fields, compareDf, mapping)

    // 5. Optionally save the compare results to a table
    // TODO Save the results to a table
    // df.withTimeBasedColumn("ds").save(joinConf.metaData.consistencyTable, tableProperties = tblProperties)

    metrics
  }
}


