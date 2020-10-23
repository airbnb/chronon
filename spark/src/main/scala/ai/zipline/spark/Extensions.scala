package ai.zipline.spark

import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}
import java.util.{Locale, TimeZone}

import ai.zipline.api.Config.{Constants, PartitionSpec}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.{StructField, StructType}

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

    def after(s: String): String = {
      partitionFormatter.format(Instant.ofEpochMilli(epochMillis(s) + partitionSpec.spanMillis))
    }

    def before(millis: Long): String = {
      of(millis - partitionSpec.spanMillis)
    }
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
          case (typ, name) => s"${typ.padTo(padding, ' ')} : $name"
        }
        .mkString("\n")
    }
  }

  implicit class DataframeOps(df: DataFrame) {
    def timeRange: (Long, Long) = {
      val viewName = s"timerange_input_${(math.random() * 1000).toInt}"
      df.createOrReplaceTempView(viewName)
      val minMaxDf: DataFrame = df.sqlContext
        .sql(s"select min(${Constants.TimeColumn}), max(${Constants.TimeColumn}) from $viewName")
      assert(minMaxDf.count() == 1, "Logic error! There needs to be exactly one row")
      val minMaxRow = minMaxDf.collect()(0)
      (minMaxRow.getLong(0), minMaxRow.getLong(1))
    }

    def typeOf(col: String): StructField = df.schema(df.schema.fieldIndex(col))

    def matchColumns(cols: Seq[String], other: DataFrame): Unit = {
      cols.foreach { col =>
        assert(df.typeOf(col) == other.typeOf(col),
               s"Mismatching column types of $col, ${df.typeOf(col)} vs. ${other.typeOf(col)}")
      }
    }

    def attachPartition: DataFrame =
      df.withColumn(Constants.PartitionColumn,
                    from_unixtime(df.col(Constants.TimeColumn) / 1000, Constants.Partition.format))

    def attachPartitionTimestamp(colName: String): DataFrame =
      df.withColumn(colName, unix_timestamp(df.col(Constants.PartitionColumn), Constants.Partition.format) * 1000)
  }
}
