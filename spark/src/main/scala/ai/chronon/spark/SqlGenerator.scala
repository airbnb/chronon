package ai.chronon.spark
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.Seq
import scala.jdk.CollectionConverters.asScalaBufferConverter

import ai.chronon.api
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.{Either, ScalaJavaConversions}
import scala.collection.JavaConversions._
import ai.chronon.api.Extensions._
import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Operation


object SqlGenerator {

  val DS_FORMAT = "yyyy-MM-dd HH:mm:ss"


  def getKeyInputs(source: api.Source, keys: Seq[String]): Seq[String] = {
    val selectMap = source.query.getSelects
    keys.map(selectMap.get(_))
  }
  def generateGroupBySourceQuery(groupBy: api.GroupBy, ds: String, keys: Seq[String], samplingRate: Option[Float], samplingMap: Option[Map[String, String]]): String = {
    val sourceQueries = groupBy.sources.toScala
      .map { source =>
        val wheres = source.query.getWheres.asScala

        if (samplingRate.isDefined) {
          val keyInputs = getKeyInputs(source, keys).map( k => s"CAST($k AS VARCHAR)")
          val keyClause = if (keyInputs.length > 1) {
            s"CONCAT(${keyInputs.mkString(",")}"
          } else {
            keyInputs.head
          }
          wheres.append(s"((abs(from_ieee754_64(xxhash64(cast($keyClause as varbinary)))) % 100) / 100. < ${samplingRate.get})")
        }

        source.query.setWheres(wheres)


        GroupBy.renderDataSourceQuery(source,
          groupBy.getKeyColumns.toScala,
          PartitionRange(ds, ds),
          None,
          groupBy.maxWindow,
          groupBy.inferredAccuracy)
      }
    sourceQueries.mkString(" \n\n UNION ALL \n\n")
  }


  def dsMidnightMillis(ds: String): Long = {
    LocalDateTime.parse(s"$ds 23:59:59", DateTimeFormatter.ofPattern(DS_FORMAT))
      .toEpochSecond(ZoneOffset.UTC) * 1000
  }

  def getAggregationSelectors(groupBy: api.GroupBy, temporalAccuracy: Boolean, subQueryName: String, ds: String): Seq[String] = {
    val dsMidnight = dsMidnightMillis(ds)
    groupBy.aggregations.asScala.flatMap { agg =>
      val fullSourceColumn = if (temporalAccuracy) {
        agg.inputColumn // TODO: Render IF ts logic here
      } else {
        agg.inputColumn
      }
      val innerExpressions: Seq[(String, String)] = if (agg.windows.asScala.isEmpty) {
        if (temporalAccuracy) {
          Seq((s"IF($subQueryName.ts < left.ts, ${agg.inputColumn}, null)", ""))
        } else {
          Seq((agg.inputColumn, ""))
        }
      } else {
        agg.windows.asScala.map { window =>
          if (temporalAccuracy) {
            (s"IF(left.ts - ${window.millis} <= ${subQueryName}.ts < left.ts, ${agg.inputColumn}, null)", window.suffix)
          } else {
            (s"IF(${subQueryName}.ts > $dsMidnight - ${window.millis}, ${agg.inputColumn}, null)", window.suffix)
          }
          //s"${aggregationPart.inputColumn}_$opSuffix${aggregationPart.window
        }
      }
      val operationSuffix = agg.operation.stringified

      innerExpressions.map { case(innerExpression, windowSuffix) =>
        val aggExpression = agg.operation match {
          case Operation.MIN => s"MIN($innerExpression)"
          case Operation.MAX => s"MAX($innerExpression)"
          case Operation.FIRST =>
          case Operation.LAST =>
          case Operation.UNIQUE_COUNT => s"COUNT(distinct $innerExpression)"
          case Operation.APPROX_UNIQUE_COUNT => s"APPROX_DISTINCT($innerExpression)"
          case Operation.COUNT => s"SUM(1)"
          case Operation.SUM => s"SUM($innerExpression)"
          case Operation.AVERAGE => s"AVG($innerExpression)"
          case Operation.VARIANCE =>
          case Operation.SKEW =>
          case Operation.KURTOSIS =>
          case Operation.APPROX_PERCENTILE =>
          case Operation.LAST_K =>
          case Operation.FIRST_K =>
          case Operation.TOP_K =>
          case Operation.BOTTOM_K =>
          case Operation.HISTOGRAM =>
        }
        val outputName = s"${agg.inputColumn}_$operationSuffix$windowSuffix"
        s"$aggExpression AS $outputName"
      }
    }
  }

  def generateJoinSql(join: api.Join) = {
    println("Generating Join SQL")
  }


  def prestoConvert(query: String): String = {
    query.replace("`", "")
      .replace("UNIX_TIMESTAMP(ts)", "TO_UNIXTIME(CAST(ts as timestamp))")
  }

  def runGroupBy(groupBy: api.GroupBy, ds: String, samplingRate: Option[Float], samplingMap: Option[Map[String, String]]): Unit = {
    //GroupBy.from(groupBy, PartitionRange(ds, ds), dummyUtils)
    groupBy.sources.asScala.head.query.getWheres
    //GroupBy.renderDataSourceQuery()
    val sourceQuery = generateGroupBySourceQuery(groupBy, ds, groupBy.keyColumns.asScala, samplingRate, samplingMap)
    val compositeKeyStr = groupBy.getKeyColumns.asScala.mkString(", ")
    val aggSelectorsString =
      getAggregationSelectors(groupBy, false, "source", ds)
        .mkString(",\n")
    val selectorsString = s"$compositeKeyStr, \n $aggSelectorsString"


    val finalSql = s"""
      | SELECT
      |   $selectorsString, '$ds' AS ds
      |   FROM
      |     (
      |       $sourceQuery
      |     ) source
      |   GROUP BY $compositeKeyStr, '$ds'
      |""".stripMargin

    print("SPARK SQL ===============================\n\n\n")
    print(finalSql)

    print("\n\n\n PRESTO SQL ===============================\n\n\n")
    print(prestoConvert(finalSql))
    print("\n\n")
  }

  def runJoin(join: api.Join, ds: String, samplingRate: Option[Float], samplingMap: Option[Map[String, String]]): Unit = {
    println("Generating Join SQL")
    println(ds)
    println(samplingRate)
    println(samplingMap)
  }

}
