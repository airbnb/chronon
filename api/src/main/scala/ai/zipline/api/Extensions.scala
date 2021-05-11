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

    def millis: Long =
      timeUnit match {
        case TimeUnit.HOURS => 3600 * 1000
        case TimeUnit.DAYS  => 24 * 3600 * 1000
      }
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
    val cleanName: String = Option(metaData.name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).getOrElse("missing_name")

    def copyForVersioningComparison: MetaData = {
      // Changing name results in column rename, therefore schema change, other metadata changes don't effect output table
      val newMetaData = new MetaData()
      newMetaData.setName(metaData.name)
      newMetaData
    }
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
      Option(aggregation.windows)
        .map(_.asScala)
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
    def hasTimedAggregations: Boolean =
      aggregations.exists(_.operation match {
        case LAST_K | FIRST_K | LAST | FIRST => true
        case _                               => false
      })

    def hasWindows: Boolean = aggregations.exists(_.windows != null)
    def needsTimestamp: Boolean = hasWindows || hasTimedAggregations
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

    def copyForVersioningComparison: Source = {
      // Makes a copy of the source and unsets date fields, used to compute equality on sources while ignoring these fields
      val newSource = source.deepCopy()
      val query = newSource.query
      query.unsetEndPartition()
      query.unsetStartPartition()
      newSource
    }
  }

  implicit class GroupByOps(groupBy: GroupBy) {
    def maxWindow: Option[Window] = {
      val aggs = Option(groupBy.aggregations).map(_.asScala).orNull
      val windowsFlattened = Option(aggs).map(_.flatMap(_.windows.asScala)).getOrElse(Seq.empty)
      if (aggs == null) None // no-agg
      else if (aggs.exists(_.windows == null)) None // agg without windows
      else if (windowsFlattened.isEmpty || windowsFlattened.contains(null))
        None // at least one of the windows is null - meaning unwindowed
      else Some(windowsFlattened.maxBy(_.millis)) // no null windows
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

    def setups: Seq[String] = {
      val sources = groupBy.sources.asScala
      sources.flatMap(_.query.setupsSeq).distinct
    }

    def copyForVersioningComparison: GroupBy = {
      val newGroupBy = groupBy.deepCopy()
      newGroupBy.setMetaData(newGroupBy.metaData.copyForVersioningComparison)
      newGroupBy
    }
  }

  implicit class JoinPartOps(joinPart: JoinPart) {
    def leftToRight: Map[String, String] = rightToLeft.map { case (key, value) => value -> key }

    def rightToLeft: Map[String, String] = {
      val rightToRight = joinPart.groupBy.keyColumns.asScala.map { key => key -> key }.toMap
      Option(joinPart.keyMapping)
        .map { leftToRight =>
          val rToL = leftToRight.asScala.map {
            case (left, right) => right -> left
          }.toMap
          rightToRight ++ rToL
        }
        .getOrElse(rightToRight)
    }

    def copyForVersioningComparison: JoinPart = {
      val newJoinPart = joinPart.deepCopy()
      newJoinPart.setGroupBy(newJoinPart.groupBy.copyForVersioningComparison)
      newJoinPart
    }
  }

  implicit class JoinOps(join: Join) {
    // all keys on left
    def leftKeyCols: Array[String] = {
      join.joinParts.asScala
        .flatMap { _.rightToLeft.values }
        .toSet
        .toArray
    }

    private def generateSkewFilterSql(key: String, values: Seq[String]): String = {
      val nulls = Seq("null", "Null", "NULL")
      val nonNullFilters = Some(s"$key NOT IN (${values.filterNot(nulls.contains).mkString(", ")})")
      val nullFilters = if (values.exists(nulls.contains)) Some(s"$key IS NOT NULL") else None
      (nonNullFilters ++ nullFilters).mkString(" AND ")
    }

    // TODO: validate that non keys are not specified in - join.skewKeys
    def skewFilter(keys: Option[Seq[String]] = None, joiner: String = " OR "): Option[String] = {
      Option(join.skewKeys).map { jmap =>
        val result = jmap.asScala
          .filterKeys(key => keys.forall { _.contains(key) })
          .map {
            case (leftKey, values) =>
              assert(
                leftKeyCols.contains(leftKey),
                s"specified skew filter for $leftKey is not used as a key in any join part. " +
                  s"Please specify key columns in skew filters: [${leftKeyCols.mkString(", ")}]"
              )
              generateSkewFilterSql(leftKey, values.asScala)
          }
          .filter(_.nonEmpty)
          .mkString(joiner)
        println(s"Generated join left side skew filter:\n    $result")
        result
      }
    }

    def partSkewFilter(joinPart: JoinPart, joiner: String = " OR "): Option[String] = {
      Option(join.skewKeys).map { jmap =>
        val result = jmap.asScala
          .flatMap {
            case (leftKey, values) =>
              val replacedKey = Option(joinPart.keyMapping)
                .map { _.asScala.getOrElse(leftKey, leftKey) }
                .getOrElse(leftKey)
              if (joinPart.groupBy.keyColumns.contains(replacedKey))
                Some(generateSkewFilterSql(replacedKey, values.asScala))
              else None
          }
          .filter(_.nonEmpty)
          .mkString(joiner)
        println(s"Generated join part skew filter for ${joinPart.groupBy.metaData.name}:\n    $result")
        result
      }
    }

    def setups: Seq[String] = (join.left.query.setupsSeq ++ join.joinParts.asScala.flatMap(_.groupBy.setups)).distinct

    def copyForVersioningComparison(): Join = {
      // When we compare previous-run join to current join to detect changes requiring table migration
      // these are the fields that should be checked to not have accidental recomputes
      val newJoin = join.deepCopy()
      newJoin.setLeft(newJoin.left.copyForVersioningComparison)
      newJoin.unsetJoinParts()
      // Opting not to use metaData.copyForVersioningComparison here because if somehow a name change results
      // in a table existing for the new name (with no other metadata change), it is more than likely intentional
      newJoin.unsetMetaData()
      newJoin
    }
  }

  implicit class StringsOps(strs: Iterable[String]) {
    def pretty: String = {
      if (strs.nonEmpty)
        "\n    " + strs.mkString(",\n    ") + "\n"
      else
        ""
    }
  }

  implicit class QueryOps(query: Query) {
    def setupsSeq: Seq[String] = {
      Option(query.setups).map(_.asScala.toSeq).getOrElse(Seq.empty)
    }
  }
}
