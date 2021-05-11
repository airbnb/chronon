package ai.zipline.spark

import ai.zipline.api._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{from_unixtime, udf, unix_timestamp}
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.util.sketch.BloomFilter
import java.util

import scala.reflect.ClassTag


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

  implicit class DataframeOps(df: DataFrame) {
    def timeRange: TimeRange = {
      assert(df.schema(Constants.TimeColumn).dataType == LongType, s"Timestamp must be a Long but found ${df.schema(Constants.TimeColumn).dataType}, if you are using a ts string, consider casting it with the UNIX_TIMESTAMP(ts) function.")
      val (start, end) = df.range[Long](Constants.TimeColumn)
      TimeRange(start, end)
    }

    def prunePartition(partitionRange: PartitionRange): DataFrame = {
      val pruneFilter = partitionRange.whereClauses.mkString(" AND ")
      println(s"Pruning using $pruneFilter")
      df.filter(pruneFilter)
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
      TableUtils(df.sparkSession).insertPartitions(df, tableName, tableProperties)
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

    private def mightContain(f: BloomFilter): UserDefinedFunction =
      udf((x: Object) => if (x != null) f.mightContain(x) else true)

    def filterBloom(bloomMap: Map[String, BloomFilter]): DataFrame =
      bloomMap.foldLeft(df) {
        case (dfIter, (col, bloom)) => dfIter.where(mightContain(bloom)(dfIter(col)))
      }

    // math for computing bloom size
    // n = number of keys (hardcoded as 1000000000 = billion distinct keys)
    // e = false positive fraction (hardcoded as 0.1)
    // numBits(m) = - n * log(e) / lg(2) * lg(2) = 2.08*n => 2e9 bits = 256MB
    // hashes = numBits * lg(2)/ n = 1.44~2
    // so each column bloom take 256MB on driver
    def generateBloomFilter(col: String, count: Long): BloomFilter =
      df.filter(df.col(col).isNotNull)
        .stat
        .bloomFilter(col, count, 0.1)

    def removeNulls(cols: Seq[String]): DataFrame = {
      println(s"filtering nulls from columns: [${cols.mkString(", ")}]")
      // do not use != or <> operator with null, it doesn't return false ever!
      df.filter(cols.map(_ + " IS NOT NULL").mkString(" AND "))
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

  implicit class ArrayOps[T: ClassTag](arr: Array[T]) {
    def uniqSort(ordering: Ordering[T]): Array[T] = {
      val tree = new util.TreeSet[T](ordering)
      for (i <- arr.indices) {
        tree.add(arr(i))
      }
      val result = new Array[T](tree.size)
      val it = tree.iterator()
      var idx = 0
      while (idx < tree.size()) {
        result.update(idx, it.next())
        idx += 1
      }
      result
    }
  }
}
