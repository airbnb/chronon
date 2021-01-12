package ai.zipline.spark

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.{Locale, TimeZone}

import ai.zipline.api.Config.Accuracy.{Accuracy, Snapshot, Temporal}
import ai.zipline.api.Config.Operation.{First, FirstK, Last, LastK}
import ai.zipline.api.Config.DataModel.DataModel
import ai.zipline.api.Config.{Aggregation, Constants, PartitionSpec, Window, GroupBy => GroupByConf}
import ai.zipline.api.QueryUtils
import ai.zipline.spark.Comparison.prefixColumnName
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.{DataType, StructType}

object Extensions {
  implicit class PartitionOps(partitionSpec: PartitionSpec) {
    private val partitionFormatter = DateTimeFormatter
      .ofPattern(partitionSpec.format, Locale.US)
      .withZone(ZoneOffset.UTC)
    val sdf = new SimpleDateFormat(partitionSpec.format)
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

    def epochMillis(partition: String): Long = {
      sdf.parse(partition).getTime
    }

    def of(millis: Long): String = {
      // [ASSUMPTION] Event partitions are start inclusive end exclusive
      // For example, for daily partitions of event data:
      //   Event timestamp at midnight of the day before of the event is included
      //   Event timestamp at midnight of the day of partition is excluded
      val roundedMillis =
        math.ceil((millis + 1).toDouble / partitionSpec.spanMillis.toDouble).toLong * partitionSpec.spanMillis
      partitionFormatter.format(Instant.ofEpochMilli(roundedMillis))
    }

    def at(millis: Long): String = partitionFormatter.format(Instant.ofEpochMilli(millis))

    def before(s: String): String = {
      partitionFormatter.format(Instant.ofEpochMilli(epochMillis(s) - partitionSpec.spanMillis))
    }

    def minus(s: String, window: Window): String = of(epochMillis(s) - window.millis)

    def after(s: String): String = {
      partitionFormatter.format(Instant.ofEpochMilli(epochMillis(s) + partitionSpec.spanMillis))
    }

    def before(millis: Long): String = of(millis - partitionSpec.spanMillis)
  }

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
    def disableLogSpam: Unit = {
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
        .sql(s"select min(${columnName}), max(${columnName}) from $viewName")
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
    def save(tableName: String): Unit = {
      TableUtils(df.sparkSession).insertPartitions(df,
                                                   Seq(Constants.PartitionColumn),
                                                   tableName,
                                                   df.partitionRange.length)
    }

    //  partitionRange is a hint to figure out the cardinality if repartitioning to control number of output files
    def save(tableName: String, partitionRange: PartitionRange): Unit = {
      TableUtils(df.sparkSession).insertPartitions(df, Seq(Constants.PartitionColumn), tableName, partitionRange.length)
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

  implicit class AggregationsOps(aggregations: Seq[Aggregation]) {
    def hasWindows = aggregations.exists(_.windows != null)
    def needsTimestamp: Boolean = {
      val hasWindows = aggregations.exists(_.windows != null)
      val hasTimedAggregations = aggregations.exists(_.operation match {
        case LastK | FirstK | Last | First => true
        case _                             => false
      })
      hasWindows || hasTimedAggregations
    }
  }

  implicit class GroupByOps(groupByConf: GroupByConf) {
    def maxWindow: Option[Window] = {
      val aggs = groupByConf.aggregations
      if (aggs == null) None // no-agg
      else if (aggs.exists(_.windows == null)) None // agg without windows
      else if (aggs.flatMap(_.windows).exists(_ == null)) None // one of the windows is null - meaning unwindowed
      else Some(aggs.flatMap(_.windows).maxBy(_.millis)) // no null windows
    }

    def dataModel: DataModel = {
      val models = groupByConf.sources.map(_.dataModel)
      assert(models.forall(_ != null),
             s"dataModel needs to be specified for all sources in the groupBy: ${groupByConf.metadata.name}")
      assert(models.distinct.length == 1,
             s"All source of the groupBy: ${groupByConf.metadata.name} " +
               s"should be of the same type. Either 'Events' or 'Entities'")
      models.head
    }

    def accuracy: Accuracy = {
      val validTopics = groupByConf.sources.map(_.topic).filter(_ != null)
      if (validTopics.length > 0) Temporal else Snapshot
    }
  }
}
