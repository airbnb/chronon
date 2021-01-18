package ai.zipline.api

import ai.zipline.api.DataModel._
import ai.zipline.api.Operation._

import scala.collection.JavaConverters._

object Extensions {

  implicit class TimeUnitOps(timeUnit: TimeUnit) {
    def str: String =
      timeUnit match {
        case TimeUnit.HOURS => "h"
        case TimeUnit.DAYS  => "d"
      }

    def millis: Long = timeUnit.getValue
  }

  implicit class WindowOps(window: Window) {
    private def unbounded: Boolean = window.length == Int.MaxValue || window.length < 0

    val str: String =
      if (unbounded) "unbounded" else s"${window.length}${window.timeUnit.str}"

    val suffix: String =
      if (unbounded) "" else s"_${window.length}${window.timeUnit.str}"

    val millis: Long = window.length.toLong * window.timeUnit.millis
  }

  object WindowUtils {
    val Unbounded: Window = new Window(Int.MaxValue, TimeUnit.DAYS)
    val Hour: Window = new Window(1, TimeUnit.HOURS)
    val Day: Window = new Window(1, TimeUnit.DAYS)
    private val SecondMillis: Long = 1000
    private val Minute: Long = 60 * SecondMillis
    val FiveMinutes: Long = 5 * Minute

    def millisToString(millis: Long): String = {
      if (millis % Day.millis == 0) {
        new Window((millis / Day.millis).toInt, TimeUnit.DAYS).str
      } else if (millis % Hour.millis == 0) {
        new Window((millis / Hour.millis).toInt, TimeUnit.HOURS).str
      } else if (millis % Minute == 0) {
        s"${millis / Minute}mins"
      } else if (millis % SecondMillis == 0) {
        s"${millis / SecondMillis}secs"
      } else {
        s"${millis}ms"
      }
    }
  }

  implicit class MetadataOps(metaData: MetaData) {
    val cleanName: String = metaData.name.replaceAll("[^a-zA-Z0-9_]", "_")
  }

  // one per output column - so single window
  // not exposed to users
  implicit class AggregationPartOps(aggregationPart: AggregationPart) {

    def getInt(arg: String): Int = {
      val argOpt = aggregationPart.argMap.asScala.get(arg)
      require(
        argOpt.isDefined,
        s"$arg needs to be specified in the constructor json for ${aggregationPart.operation} type"
      )
      argOpt.get.toInt
    }

    private def opSuffix =
      aggregationPart.operation match {
        case LAST_K   => s"last${getInt("k")}"
        case FIRST_K  => s"first${getInt("k")}"
        case TOP_K    => s"top${getInt("k")}"
        case BOTTOM_K => s"bottom${getInt("k")}"
        case other    => other.toString.toLowerCase
      }

    def outputColumnName = s"${aggregationPart.inputColumn}_$opSuffix${aggregationPart.window.suffix}"
  }

  implicit class AggregationOps(aggregation: Aggregation) {

    def unpack: Seq[AggregationPart] =
      Option(aggregation.windows.asScala)
        .getOrElse(Seq(WindowUtils.Unbounded))
        .map { window =>
          Option(window) match {
            case Some(window) =>
              Builders.AggregationPart(aggregation.operation,
                                       aggregation.inputColumn,
                                       window,
                                       Option(aggregation.argMap).map(_.asScala.toMap).orNull)
            case None =>
              Builders.AggregationPart(aggregation.operation,
                                       aggregation.inputColumn,
                                       WindowUtils.Unbounded,
                                       Option(aggregation.argMap).map(_.asScala.toMap).orNull)
          }
        }

    def unWindowed: AggregationPart =
      Builders.AggregationPart(aggregation.operation,
                               aggregation.inputColumn,
                               WindowUtils.Unbounded,
                               Option(aggregation.argMap).map(_.asScala.toMap).orNull)
  }

  implicit class AggregationsOps(aggregations: Seq[Aggregation]) {
    def hasWindows: Boolean = aggregations.exists(_.windows != null)
    def needsTimestamp: Boolean = {
      val hasWindows = aggregations.exists(_.windows != null)
      val hasTimedAggregations = aggregations.exists(_.operation match {
        case LAST_K | FIRST_K | LAST | FIRST => true
        case _                               => false
      })
      hasWindows || hasTimedAggregations
    }
  }

  implicit class SourceOps(source: Source) {
    def dataModel: DataModel = {
      assert(source.isSetEntities || source.isSetEvents, "Source type is not specified")
      if (source.isSetEntities) Entities else Events
    }

    def query: Query = {
      if (source.isSetEntities) source.getEntities.query else source.getEvents.query
    }

    def table: String = {
      if (source.isSetEntities) source.getEntities.getSnapshotTable else source.getEvents.getTable
    }

    def topic: String = {
      if (source.isSetEntities) source.getEntities.getMutationTopic else source.getEvents.getTopic
    }
  }

  implicit class QueryOps(query: Query) {
    def selectAliased: Seq[String] = {
      Option(query.getSelects)
        .map(_.asScala.map {
          case (alias: String, expression: String) => s"$expression as `$alias`"
        }.toSeq)
        .orNull
    }
  }

  implicit class GroupByOps(groupBy: GroupBy) {
    def maxWindow: Option[Window] = {
      val aggs = groupBy.aggregations.asScala
      if (aggs == null) None // no-agg
      else if (aggs.exists(_.windows == null)) None // agg without windows
      else if (aggs.flatMap(_.windows.asScala).contains(null))
        None // one of the windows is null - meaning unwindowed
      else Some(aggs.flatMap(_.windows.asScala).maxBy(_.millis)) // no null windows
    }

    def dataModel: DataModel = {
      val models = groupBy.sources.asScala.map(_.dataModel)
      assert(models.distinct.length == 1,
             s"All source of the groupBy: ${groupBy.metaData.name} " +
               s"should be of the same type. Either 'Events' or 'Entities'")
      models.head
    }

    def accuracy: Accuracy = {
      val validTopics = groupBy.sources.asScala.map(_.topic).filter(_ != null)
      if (validTopics.nonEmpty) Accuracy.TEMPORAL else Accuracy.SNAPSHOT
    }
  }
}
