package ai.chronon.spark
import scala.jdk.CollectionConverters.asScalaBufferConverter

import ai.chronon.api
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.{Either, ScalaJavaConversions}
import ai.chronon.api.Extensions._

import ai.chronon.api.Extensions.GroupByOps
import ai.chronon.api.Operation


object SqlGenerator {

  def generateGroupBySourceQuery(groupBy: api.GroupBy, ds: String): String = {
    val sourceQueries = groupBy.sources.toScala
      .map { source =>
        val query = GroupBy.renderDataSourceQuery(source,
          groupBy.getKeyColumns.toScala,
          PartitionRange(ds, ds),
          None,
          groupBy.maxWindow,
          groupBy.inferredAccuracy)
      }
    sourceQueries.mkString(" \n\n UNION ALL \n\n")
  }

  def windowToMillis(window: api.Window): Int = {
    window.timeUnit match {
      case api.TimeUnit.HOURS => window.length * 60 * 60 * 1000
      case api.TimeUnit.DAYS => window.length * 24 * 60 * 60 * 1000
    }
  }

  def getAggregationSelectors(groupBy: api.GroupBy, temporalAccuracy: Boolean, subQueryName: String, timeColumn: String): Seq[String] = {
    groupBy.aggregations.asScala.map { agg =>
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
          val windowMillis = windowToMillis(window)
          if (temporalAccuracy) {
            (s"IF(left.ts - $windowMillis <= ${subQueryName}.ts < left.ts, ${agg.inputColumn}, null)", window.suffix)
          } else {
            (s"IF(${subQueryName}.ts > DATE_SUB(ds, window), input_col, null)", window.suffix)
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
          case Operation.AVERAGE => s"AVERAGE($innerExpression)"
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

  def runGroupBy(groupBy: api.GroupBy, ds: String, samplingRate: Option[Float], samplingMap: Option[Map[String, String]]): Unit = {
    println("Generating GroupBy SQL")
    println(ds)
    println(samplingRate)
    println(samplingMap)
    //GroupBy.from(groupBy, PartitionRange(ds, ds), dummyUtils)
    groupBy.sources
    //GroupBy.renderDataSourceQuery()

    val sourceQuery = generateGroupBySourceQuery(groupBy, ds)


    val finalSql = s"""
      | SELECT
      |   {},
      |   FROM
      |     (
      |       $sourceQuery
      |     ) source
      |   {}
      |   {}
      |""".stripMargin

  }

  def runJoin(join: api.Join, ds: String, samplingRate: Option[Float], samplingMap: Option[Map[String, String]]): Unit = {
    println("Generating Join SQL")
    println(ds)
    println(samplingRate)
    println(samplingMap)
  }

}
