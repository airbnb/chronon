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

import org.slf4j.LoggerFactory
import ai.chronon.api
import ai.chronon.api.Constants
import ai.chronon.online.{AvroCodec, AvroConversions, SparkConversions}
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.util.sketch.BloomFilter

import java.util
import scala.collection.Seq
import scala.reflect.ClassTag
import scala.util.ScalaJavaConversions.IteratorOps

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

    def toChrononSchema(name: String = null): api.StructType =
      api.StructType.from(name, SparkConversions.toChrononSchema(schema))
    def toAvroSchema(name: String = null): Schema = AvroConversions.fromChrononSchema(toChrononSchema(name))
    def toAvroCodec(name: String = null): AvroCodec = new AvroCodec(toAvroSchema(name).toString())
  }

  case class DfStats(count: Long, partitionRange: PartitionRange)
  // helper class to maintain datafram stats that are necessary for downstream operations
  case class DfWithStats(df: DataFrame, partitionCounts: Map[String, Long])(implicit val tableUtils: TableUtils) {
    private val minPartition: String = partitionCounts.keys.min
    private val maxPartition: String = partitionCounts.keys.max
    val partitionRange: PartitionRange = PartitionRange(minPartition, maxPartition)
    val count: Long = partitionCounts.values.sum

    def prunePartitions(range: PartitionRange): Option[DfWithStats] = {
      println(
        s"Pruning down to new range $range, original range: $partitionRange." +
          s"\nOriginal partition counts: $partitionCounts")
      val intersected = partitionRange.intersect(range)
      if (!intersected.wellDefined) return None
      val intersectedCounts = partitionCounts.filter(intersected.partitions contains _._1)
      if (intersectedCounts.isEmpty) return None
      Some(DfWithStats(df.prunePartition(range), intersectedCounts))
    }
    def stats: DfStats = DfStats(count, partitionRange)
  }

  object DfWithStats {
    def apply(dataFrame: DataFrame)(implicit tableUtils: TableUtils): DfWithStats = {
      val partitionCounts = dataFrame
        .groupBy(col(TableUtils(dataFrame.sparkSession).partitionColumn))
        .count()
        .collect()
        .map(row => row.getString(0) -> row.getLong(1))
        .toMap
      DfWithStats(dataFrame, partitionCounts)
    }
  }

  implicit class DataframeOps(df: DataFrame) {
    @transient lazy val logger = LoggerFactory.getLogger(getClass)
    private implicit val tableUtils: TableUtils = TableUtils(df.sparkSession)

    // This is safe to call on dataframes that are un-shuffled from their disk sources -
    // like tables read without shuffling with row level projections or filters.
    def timeRange: TimeRange = {
      assert(
        df.schema(Constants.TimeColumn).dataType == LongType,
        s"Timestamp must be a Long type in milliseconds but found ${df.schema(Constants.TimeColumn).dataType}, if you are using a ts string, consider casting it with the UNIX_TIMESTAMP(ts)*1000 function."
      )
      val (start, end) = df.range[Long](Constants.TimeColumn)
      TimeRange(start, end)
    }

    def prunePartition(partitionRange: PartitionRange): DataFrame = {
      val pruneFilter = partitionRange.whereClauses().mkString(" AND ")
      logger.info(s"Pruning using $pruneFilter")
      df.filter(pruneFilter)
    }

    def partitionRange: PartitionRange = {
      val (start, end) = df.range[String](tableUtils.partitionColumn)
      PartitionRange(start, end)
    }

    def withStats: DfWithStats = DfWithStats(df)

    def range[T](columnName: String): (T, T) = {
      val viewName = s"${columnName}_range_input_${(math.random * 100000).toInt}"
      df.createOrReplaceTempView(viewName)
      assert(df.schema.names.contains(columnName),
             s"$columnName is not a column of the dataframe. Pick one of [${df.schema.names.mkString(", ")}]")
      val minMaxRows = df.sqlContext
        .sql(s"select min($columnName), max($columnName) from $viewName")
        .collect()
      assert(minMaxRows.size == 1, "Logic error! There needs to be exactly one row")
      val minMaxRow = minMaxRows(0)
      df.sparkSession.catalog.dropTempView(viewName)
      val (min, max) = (minMaxRow.getAs[T](0), minMaxRow.getAs[T](1))
      logger.info(s"Computed Range for $columnName - min: $min, max: $max")
      (min, max)
    }

    def typeOf(col: String): DataType = df.schema(df.schema.fieldIndex(col)).dataType

    def save(tableName: String,
             tableProperties: Map[String, String] = null,
             partitionColumns: Seq[String] = Seq(tableUtils.partitionColumn),
             autoExpand: Boolean = false,
             stats: Option[DfStats] = None,
             sortByCols: Seq[String] = Seq.empty): Unit = {
      TableUtils(df.sparkSession).insertPartitions(df,
                                                   tableName,
                                                   tableProperties,
                                                   partitionColumns,
                                                   autoExpand = autoExpand,
                                                   stats = stats,
                                                   sortByCols = sortByCols)
    }

    def saveUnPartitioned(tableName: String, tableProperties: Map[String, String] = null): Unit = {
      TableUtils(df.sparkSession).insertUnPartitioned(df, tableName, tableProperties)
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

    def filterBloom(bloomMap: util.Map[String, BloomFilter]): DataFrame =
      bloomMap.entrySet().iterator().toScala.foldLeft(df) {
        case (dfIter, entry) =>
          val col = entry.getKey
          val bloom = entry.getValue
          dfIter.where(mightContain(bloom)(dfIter(col)))
      }

    // math for computing bloom size
    // n = number of keys (hardcoded as 1000000000 = billion distinct keys)
    // e = false positive fraction (hardcoded as 0.1)
    // numBits(m) = - n * log(e) / lg(2) * lg(2) = 2.08*n => 2e9 bits = 256MB
    // hashes = numBits * lg(2)/ n = 1.44~2
    // so each column bloom take 256MB on driver
    def generateBloomFilter(col: String,
                            totalCount: Long,
                            tableName: String,
                            partitionRange: PartitionRange,
                            fpp: Double = 0.03): BloomFilter = {
      val approxCount =
        df.filter(df.col(col).isNotNull).select(approx_count_distinct(col)).collect()(0).getLong(0)
      if (approxCount == 0) {
        logger.info(
          s"Warning: approxCount for col ${col} from table ${tableName} is 0. Please double check your input data.")
      }
      logger.info(s""" [STARTED] Generating bloom filter on key `$col` for range $partitionRange from $tableName
           | Approximate distinct count of `$col`: $approxCount
           | Total count of rows: $totalCount
           |""".stripMargin)
      val bloomFilter = df
        .filter(df.col(col).isNotNull)
        .stat
        .bloomFilter(col, approxCount + 1, fpp) // expectedNumItems must be positive

      logger.info(s"""
           | [FINISHED] Generating bloom filter on key `$col` for range $partitionRange from $tableName
           | Approximate distinct count of `$col`: $approxCount
           | Total count of rows: $totalCount
           | BloomSize in bits: ${bloomFilter.bitSize()}
           |""".stripMargin)
      bloomFilter
    }

    def removeNulls(cols: Seq[String]): DataFrame = {
      logger.info(s"filtering nulls from columns: [${cols.mkString(", ")}]")
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

    // convert a millisecond timestamp to string with the specified format
    def withTimeBasedColumn(columnName: String,
                            timeColumn: String = Constants.TimeColumn,
                            format: String = tableUtils.partitionSpec.format): DataFrame =
      df.withColumn(columnName, from_unixtime(df.col(timeColumn) / 1000, format))

    def addTimebasedColIfExists(): DataFrame =
      if (df.schema.names.contains(Constants.TimeColumn)) {
        df.withTimeBasedColumn(Constants.TimePartitionColumn)
      } else {
        df
      }

    private def camelToSnake(name: String) = {
      val res = "([a-z]+)([A-Z]\\w+)?".r
        .replaceAllIn(name, { m => m.subgroups.flatMap(g => Option(g).map(_.toLowerCase())).mkString("_") })
      res
    }

    def camelToSnake: DataFrame =
      df.columns.foldLeft(df)((renamed, col) => renamed.withColumnRenamed(col, camelToSnake(col)))

    def withPartitionBasedTimestamp(colName: String, inputColumn: String = tableUtils.partitionColumn): DataFrame =
      df.withColumn(colName, unix_timestamp(df.col(inputColumn), tableUtils.partitionSpec.format) * 1000)

    def withShiftedPartition(colName: String, days: Int = 1): DataFrame =
      df.withColumn(
        colName,
        date_format(date_add(to_date(df.col(tableUtils.partitionColumn), tableUtils.partitionSpec.format), days),
                    tableUtils.partitionSpec.format))

    def replaceWithReadableTime(cols: Seq[String], dropOriginal: Boolean): DataFrame = {
      cols.foldLeft(df) { (dfNew, col) =>
        val renamed = dfNew
          .withColumn(s"${col}_str", from_unixtime(df(col) / 1000, "yyyy-MM-dd HH:mm:ss"))
        if (dropOriginal) renamed.drop(col) else renamed
      }
    }

    def order(keys: Seq[String]): DataFrame = {
      val filterClause = keys.map(key => s"($key IS NOT NULL)").mkString(" AND ")
      val filtered = df.where(filterClause)
      filtered.orderBy(keys.map(desc).toSeq: _*)
    }

    def prettyPrint(timeColumns: Seq[String] = Seq(Constants.TimeColumn, Constants.MutationTimeColumn)): Unit = {
      val availableColumns = timeColumns.filter(df.schema.names.contains)
      logger.info(s"schema: ${df.schema.fieldNames.mkString("Array(", ", ", ")")}")
      df.replaceWithReadableTime(availableColumns, dropOriginal = true).show(truncate = false)
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

  implicit class InternalRowOps(internalRow: InternalRow) {
    def toRow(schema: StructType): Row = {
      new Row() {
        override def length: Int = {
          internalRow.numFields
        }

        override def get(i: Int): Any = {
          internalRow.get(i, schema.fields(i).dataType)
        }

        override def copy(): Row = internalRow.copy().toRow(schema)
      }
    }
  }

  implicit class TupleToJMapOps[K, V](tuples: Iterator[(K, V)]) {
    def toJMap: util.Map[K, V] = {
      val map = new util.HashMap[K, V]()
      tuples.foreach { case (k, v) => map.put(k, v) }
      map
    }
  }
}
