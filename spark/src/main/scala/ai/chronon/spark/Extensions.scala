package ai.chronon.spark

import ai.chronon.aggregator.row.RowAggregator
import ai.chronon.api
import ai.chronon.api.{Constants, Operation}
import ai.chronon.online.{AvroCodec, AvroConversions}
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer.types
import com.yahoo.sketches.frequencies.ItemsSketch
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, types}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, LongType, StringType, StructType}
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

    def toChrononSchema(name: String = null): api.StructType =
      api.StructType.from(name, Conversions.toChrononSchema(schema))
    def toAvroSchema(name: String = null): Schema = AvroConversions.fromChrononSchema(toChrononSchema(name))
    def toAvroCodec(name: String = null): AvroCodec = new AvroCodec(toAvroSchema(name).toString())
  }

  implicit class DataframeOps(df: DataFrame) {
    case class MetricTransformer(colName: String, expr: String, aggs: Seq[Operation]) {}
    // take an set of subsets of columns and produce heavy hitter counts for each subset in a single pass.
    // Seq(Seq("user", "item"), Seq("item")) will produce two maps, "user|item" heavy hitters and "item" heavy hitters.
    def analyze(frequentItemKeys: Seq[Seq[String]], frequentItemMapSize: Int = 1024): Seq[Map[String, Long]] = {
      assert(frequentItemKeys.nonEmpty, "No column arrays specified for frequent items summary")
      val frequencyKeyExprs = frequentItemKeys.map { cols =>
        assert(cols.nonEmpty, "No columns specified for frequent items summary")
        val stringifiedCols = cols.map { col =>
          df.schema.fields.find(_.name == col) match {
            case Some(types.StructField(name, StringType, _, _)) => name
            case Some(types.StructField(name, _, _, _))          => s"CAST($name AS STRING)"
            case None =>
              throw new IllegalArgumentException(s"$col is not present among: [${df.schema.fieldNames.mkString(", ")}]")
          }
        }

        if (stringifiedCols.length > 1) {
          s"CONCAT_WS('|', ${stringifiedCols.mkString(", ")}) AS ${cols.mkString("__")}"
        } else { stringifiedCols.head }
      }

      val rowAggregator = new RowAggregator()
      df.selectExpr(exprs: _*)
        .rdd
        .treeAggregate(new ItemsSketch[String](1024))(seqOp = {
          case (sketch, row) => sketch.update(row.getString(0))
        })
    }
    def timeRange: TimeRange = {
      assert(
        df.schema(Constants.TimeColumn).dataType == LongType,
        s"Timestamp must be a Long type in milliseconds but found ${df.schema(Constants.TimeColumn).dataType}, if you are using a ts string, consider casting it with the UNIX_TIMESTAMP(ts)*1000 function."
      )
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
      val viewName = s"${columnName}_range_input_${(math.random * 100000).toInt}"
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

    def save(tableName: String, tableProperties: Map[String, String] = null): Unit = {
      TableUtils(df.sparkSession).insertPartitions(df, tableName, tableProperties)
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
    def generateBloomFilter(col: String,
                            totalCount: Long,
                            tableName: String,
                            partitionRange: PartitionRange,
                            fpp: Double = 0.03): BloomFilter = {
      val approxCount =
        df.filter(df.col(col).isNotNull).select(approx_count_distinct(col)).collect()(0).getLong(0)
      println(s""" [STARTED] Generating bloom filter on key `$col` for range $partitionRange from $tableName
           | Approximate distinct count of `$col`: $approxCount
           | Total count of rows: $totalCount
           |""".stripMargin)
      val bloomFilter = df
        .filter(df.col(col).isNotNull)
        .stat
        .bloomFilter(col, approxCount, fpp)
      println(s"""
           | [FINISHED] Generating bloom filter on key `$col` for range $partitionRange from $tableName
           | Approximate distinct count of `$col`: $approxCount
           | Total count of rows: $totalCount
           | BloomSize in bits: ${bloomFilter.bitSize()}
           |""".stripMargin)
      bloomFilter
    }

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

    // convert a millisecond timestamp to string with the specified format
    def withTimeBasedColumn(columnName: String,
                            timeColumn: String = Constants.TimeColumn,
                            format: String = Constants.Partition.format): DataFrame =
      df.withColumn(columnName, from_unixtime(df.col(timeColumn) / 1000, format))

    private def camelToSnake(name: String) = {
      val res = "([a-z]+)([A-Z]\\w+)?".r
        .replaceAllIn(name, { m => m.subgroups.flatMap(g => Option(g).map(_.toLowerCase())).mkString("_") })
      res
    }

    def camelToSnake: DataFrame =
      df.columns.foldLeft(df)((renamed, col) => renamed.withColumnRenamed(col, camelToSnake(col)))

    def withPartitionBasedTimestamp(colName: String, inputColumn: String = Constants.PartitionColumn): DataFrame =
      df.withColumn(colName, unix_timestamp(df.col(inputColumn), Constants.Partition.format) * 1000)

    def withShiftedPartition(colName: String, days: Int = 1): DataFrame =
      df.withColumn(colName, date_format(date_add(df.col(Constants.PartitionColumn), days), Constants.Partition.format))

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
      filtered.orderBy(keys.map(desc): _*)
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
