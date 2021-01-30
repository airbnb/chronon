package ai.zipline.spark

import ai.zipline.api._
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Extensions {

  implicit class StructTypeOps(schema: StructType) {
    def pretty: String = {
      val schemaTuples = schema.fields.map { field =>
        field.dataType.simpleString -> field.name
      }

      // pad the first column so that the second column is aligned vertically
      val padding = schemaTuples.map(_._1.length).max
      schemaTuples
        .map {
          case (typ, name) => s"  ${typ.padTo(padding, ' ')} : $name"
        }
        .mkString("\n")
    }
  }

  implicit class SparkSessionOps(sparkSession: SparkSession) {
    def disableLogSpam(): Unit = {
      sparkSession.sparkContext.setLogLevel("ERROR")
    }
  }

  implicit class DataframeOps(df: DataFrame) {
    def timeRange: TimeRange = {
      val (start, end) = df.range[Long](Constants.TimeColumn)
      TimeRange(start, end)
    }

    def partitionRange: PartitionRange = {
      val (start, end) = df.range[String](Constants.PartitionColumn)
      PartitionRange(start, end)
    }

    def range[T](columnName: String): (T, T) = {
      val viewName = s"${columnName}_range_input"
      df.createOrReplaceTempView(viewName)
      assert(df.schema.names.contains(columnName),
             s"$columnName is not a column of the dataframe. Pick one of [${df.schema.names.mkString(", ")}]")
      val minMaxDf: DataFrame = df.sqlContext
        .sql(s"select min($columnName), max($columnName) from $viewName")
      assert(minMaxDf.count() == 1, "Logic error! There needs to be exactly one row")
      val minMaxRow = minMaxDf.collect()(0)
      df.sparkSession.catalog.dropTempView(viewName)
      val (min, max) = (minMaxRow.getAs[T](0), minMaxRow.getAs[T](1))
      println(s"Computed Range for $columnName - min: $min, max: $max")
      (min, max)
    }

    def typeOf(col: String): DataType = df.schema(df.schema.fieldIndex(col)).dataType

    // partitionRange is a hint to figure out the cardinality if repartitioning to control number of output files
    // use sparingly/in tests.
    def save(tableName: String, tableProperties: Map[String, String] = null): Unit = {
      TableUtils(df.sparkSession).insertPartitions(df,
                                                   Seq(Constants.PartitionColumn),
                                                   tableName,
                                                   df.partitionRange.length,
                                                   tableProperties)
    }

    //  partitionRange is a hint to figure out the cardinality if repartitioning to control number of output files
    def savePartitionCounted(tableName: String,
                             partitionRange: PartitionRange,
                             tableProperties: Map[String, String] = null,
                             filesPerPartition: Int): Unit = {
      TableUtils(df.sparkSession).insertPartitions(df,
                                                   Seq(Constants.PartitionColumn),
                                                   tableName,
                                                   partitionRange.length,
                                                   tableProperties,
                                                   filesPerPartition)
    }

    def prefixColumnNames(prefix: String, columns: Seq[String]): DataFrame = {
      columns.foldLeft(df) { (renamedDf, key) =>
        renamedDf.withColumnRenamed(key, s"${prefix}_$key")
      }
    }

    def validateJoinKeys(right: DataFrame, keys: Seq[String]): Unit = {
      keys.foreach { key =>
        val leftFields = df.schema.fieldNames
        val rightFields = right.schema.fieldNames
        assert(leftFields.contains(key),
               s"left side of the join doesn't contain the key $key, available keys are [${leftFields.mkString(", ")}]")
        assert(
          rightFields.contains(key),
          s"right side of the join doesn't contain the key $key, available columns are [${rightFields.mkString(", ")}]")
        val leftDataType = df.schema(leftFields.indexOf(key)).dataType
        val rightDataType = right.schema(rightFields.indexOf(key)).dataType
        assert(leftDataType == rightDataType,
               s"Join key, '$key', has mismatched data types - left type: $leftDataType vs. right type $rightDataType")
      }
    }

    def nullSafeJoin(right: DataFrame, keys: Seq[String], joinType: String): DataFrame = {
      validateJoinKeys(right, keys)
      val prefixedLeft = df.prefixColumnNames("left", keys)
      val prefixedRight = right.prefixColumnNames("right", keys)
      val joinExpr = keys
        .map(key => prefixedLeft(s"left_$key") <=> prefixedRight(s"right_$key"))
        .reduce((col1, col2) => col1.and(col2))
      val joined = prefixedLeft.join(
        prefixedRight,
        joinExpr,
        joinType = joinType
      )
      keys.foldLeft(joined) { (renamedJoin, key) =>
        renamedJoin.withColumnRenamed(s"left_$key", key).drop(s"right_$key")
      }
    }

    def withTimestampBasedPartition(columnName: String): DataFrame =
      df.withColumn(columnName, from_unixtime(df.col(Constants.TimeColumn) / 1000, Constants.Partition.format))

    def withPartitionBasedTimestamp(colName: String): DataFrame =
      df.withColumn(colName, unix_timestamp(df.col(Constants.PartitionColumn), Constants.Partition.format) * 1000)

    def replaceWithReadableTime(cols: Seq[String], dropOriginal: Boolean): DataFrame = {
      cols.foldLeft(df) { (dfNew, col) =>
        val renamed = dfNew
          .withColumn(s"${col}_str", from_unixtime(df(col) / 1000, "yyyy-MM-dd HH:mm:ss"))
        if (dropOriginal) renamed.drop(col) else renamed
      }
    }
  }

}
