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

package ai.chronon.api

import org.slf4j.LoggerFactory
import ai.chronon.api.DataModel._
import ai.chronon.api.Operation._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.expr

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.regex.Pattern
import scala.collection.{Seq, mutable}
import scala.util.ScalaJavaConversions.{IteratorOps, ListOps, MapOps}
import scala.util.{Failure, Success, Try}
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

  implicit class OperationOps(operation: Operation) {
    def isSimple: Boolean =
      operation match {
        case Operation.FIRST | Operation.LAST | Operation.LAST_K | Operation.FIRST_K => false
        case _                                                                       => true
      }

    def stringified: String = operation.toString.toLowerCase

  }

  implicit class WindowOps(window: Window) {
    private def unbounded: Boolean = window.length == Int.MaxValue || window.length <= 0

    def str: String =
      if (unbounded) "unbounded" else s"${window.length}${window.timeUnit.str}"

    def suffix: String =
      if (unbounded) "" else s"_${window.length}${window.timeUnit.str}"

    def millis: Long = window.length.toLong * window.timeUnit.millis
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
        s"${millis / Minute} minutes"
      } else if (millis % SecondMillis == 0) {
        s"${millis / SecondMillis} seconds"
      } else {
        s"${millis}ms"
      }
    }
  }

  implicit class MetadataOps(metaData: MetaData) {
    def cleanName: String = metaData.name.sanitize

    def outputTable = s"${metaData.outputNamespace}.${metaData.cleanName}"
    def outputLabelTable = s"${metaData.outputNamespace}.${metaData.cleanName}_labels"
    def outputFinalView = s"${metaData.outputNamespace}.${metaData.cleanName}_labeled"
    def outputLatestLabelView = s"${metaData.outputNamespace}.${metaData.cleanName}_labeled_latest"
    def loggedTable = s"${outputTable}_logged"

    def bootstrapTable = s"${outputTable}_bootstrap"

    private def comparisonPrefix = "comparison"

    def comparisonConfName = s"${metaData.getName}_$comparisonPrefix"

    def comparisonTable = s"${outputTable}_$comparisonPrefix"

    def consistencyTable = s"${outputTable}_consistency"
    def consistencyUploadTable = s"${consistencyTable}_upload"

    def loggingStatsTable = s"${loggedTable}_daily_stats"
    def uploadTable = s"${outputTable}_upload"
    def dailyStatsOutputTable = s"${outputTable}_daily_stats"

    def toUploadTable(name: String) = s"${name}_upload"

    def copyForVersioningComparison: MetaData = {
      // Changing name results in column rename, therefore schema change, other metadata changes don't effect output table
      val newMetaData = new MetaData()
      newMetaData.setName(metaData.name)
      newMetaData
    }

    def tableProps: Map[String, String] =
      Option(metaData.tableProperties)
        .map(_.toScala.toMap)
        .orNull

    def nameToFilePath: String = metaData.name.replaceFirst("\\.", "/")

    // helper function to extract values from customJson
    def customJsonLookUp(key: String): Any = {
      if (metaData.customJson == null) return null
      val mapper = new ObjectMapper();
      val typeRef = new TypeReference[java.util.HashMap[String, Object]]() {}
      val jMap: java.util.Map[String, Object] = mapper.readValue(metaData.customJson, typeRef)
      jMap.toScala.get(key).orNull
    }

    def owningTeam: String = {
      val teamOverride = Option(Try(customJsonLookUp(Constants.TeamOverride).asInstanceOf[String]).getOrElse(null))
      teamOverride.getOrElse(metaData.team)
    }
  }

  // one per output column - so single window
  // not exposed to users
  implicit class AggregationPartOps(aggregationPart: AggregationPart) {

    def getInt(arg: String, default: Option[Int] = None): Int = {
      val argOpt = Option(aggregationPart.argMap)
        .flatMap(_.toScala.get(arg))
      require(
        argOpt.isDefined || default.isDefined,
        s"$arg needs to be specified in the `argMap` for ${aggregationPart.operation} type"
      )
      argOpt.map(_.toInt).getOrElse(default.get)
    }

    private def opSuffix =
      aggregationPart.operation match {
        case LAST_K   => s"last${getInt("k")}"
        case FIRST_K  => s"first${getInt("k")}"
        case TOP_K    => s"top${getInt("k")}"
        case BOTTOM_K => s"bottom${getInt("k")}"
        case other    => other.stringified
      }

    private def bucketSuffix = Option(aggregationPart.bucket).map("_by_" + _).getOrElse("")

    def outputColumnName =
      s"${aggregationPart.inputColumn}_$opSuffix${aggregationPart.window.suffix}${bucketSuffix}"
  }

  implicit class AggregationOps(aggregation: Aggregation) {

    // one agg part per bucket per window
    // unspecified windows are translated to one unbounded window
    def unpack: Seq[AggregationPart] = {
      val windows = Option(aggregation.windows)
        .map(_.toScala)
        .getOrElse(Seq(WindowUtils.Unbounded))
        .toSeq
      val buckets = Option(aggregation.buckets)
        .map(_.toScala)
        .getOrElse(Seq(null))
        .toSeq
      for (bucket <- buckets; window <- windows) yield {
        Builders.AggregationPart(
          aggregation.operation,
          aggregation.inputColumn,
          window,
          Option(aggregation.argMap)
            .map(
              _.toScala
            )
            .orNull,
          bucket
        )
      }
    }

    // one agg part per bucket
    // ignoring the windowing
    def unWindowed: Seq[AggregationPart] = {
      val buckets = Option(aggregation.buckets)
        .map(_.toScala)
        .getOrElse(Seq(null))
        .toSeq
      for (bucket <- buckets) yield {
        Builders.AggregationPart(
          aggregation.operation,
          aggregation.inputColumn,
          WindowUtils.Unbounded,
          Option(aggregation.argMap)
            .map(
              _.toScala.toMap
            )
            .orNull,
          bucket
        )
      }
    }
  }

  case class WindowMapping(aggregationPart: AggregationPart, baseIrIndex: Int)

  case class UnpackedAggregations(perBucket: Array[AggregationPart], perWindow: Array[WindowMapping])

  object UnpackedAggregations {
    def from(aggregations: Seq[Aggregation]): UnpackedAggregations = {
      var counter = 0
      val perBucket = new mutable.ArrayBuffer[AggregationPart]
      val perWindow = new mutable.ArrayBuffer[WindowMapping]
      aggregations.foreach { agg =>
        val buckets = Option(agg.buckets)
          .map(_.toScala)
          .getOrElse(Seq(null))
        val windows = Option(agg.windows)
          .map(_.toScala)
          .getOrElse(Seq(WindowUtils.Unbounded))
        for (bucket <- buckets) {
          perBucket += Builders.AggregationPart(
            agg.operation,
            agg.inputColumn,
            WindowUtils.Unbounded,
            Option(agg.argMap)
              .map(
                _.toScala.toMap
              )
              .orNull,
            bucket
          )
          for (window <- windows) {
            perWindow += WindowMapping(
              Builders.AggregationPart(agg.operation,
                                       agg.inputColumn,
                                       window,
                                       Option(agg.argMap)
                                         .map(
                                           _.toScala.toMap
                                         )
                                         .orNull,
                                       bucket),
              counter
            )
          }
          counter += 1
        }
      }
      UnpackedAggregations(perBucket = perBucket.toArray, perWindow = perWindow.toArray)
    }
  }

  implicit class AggregationsOps(aggregations: Seq[Aggregation]) {
    def hasTimedAggregations: Boolean =
      aggregations.exists(_.operation match {
        case LAST_K | FIRST_K | LAST | FIRST => true
        case _                               => false
      })

    def hasWindows: Boolean = aggregations.exists(_.windows != null)

    def needsTimestamp: Boolean = hasWindows || hasTimedAggregations

    def allWindowsOpt: Option[Seq[Window]] =
      Option(aggregations).map { aggs =>
        aggs.flatMap { agg =>
          Option(agg.windows)
            .map(
              _.toScala
            )
            .getOrElse(Seq(null))
        }
      }
  }

  implicit class SourceOps(source: Source) {
    def dataModel: DataModel = {
      assert(source.isSetEntities || source.isSetEvents || source.isSetJoinSource, "Source type is not specified")
      if (source.isSetEntities) Entities
      else if (source.isSetEvents) Events
      else source.getJoinSource.getJoin.left.dataModel
    }

    def rootQuery: Query = {
      if (source.isSetEntities) {
        source.getEntities.query
      } else if (source.isSetEvents) {
        source.getEvents.query
      } else {
        source.getJoinSource.getJoin.getLeft.query
      }
    }

    def query: Query = {
      if (source.isSetEntities) {
        source.getEntities.query
      } else if (source.isSetEvents) {
        source.getEvents.query
      } else {
        source.getJoinSource.query
      }
    }

    def partitionColumnOpt: Option[String] = {
      Option(source.query.partitionColumn)
    }

    def tableToPartitionColumn: Map[String, String] = {
      partitionColumnOpt match {
        case Some(col) => Map(table -> col)
        case None      => Map.empty
      }
    }

    lazy val rootTable: String = {
      if (source.isSetEntities) {
        source.getEntities.getSnapshotTable
      } else if (source.isSetEvents) {
        source.getEvents.getTable
      } else {
        source.getJoinSource.getJoin.left.table
      }
    }

    lazy val rawTable: String = {
      if (source.isSetEntities) { source.getEntities.getSnapshotTable }
      else if (source.isSetEvents) { source.getEvents.getTable }
      else { source.getJoinSource.getJoin.metaData.outputTable }
    }

    def overwriteTable(table: String): Unit = {
      if (source.isSetEntities) { source.getEntities.setSnapshotTable(table) }
      else if (source.isSetEvents) { source.getEvents.setTable(table) }
      else {
        val metadata = source.getJoinSource.getJoin.getMetaData
        val Array(namespace, tableName) = table.split(".")
        metadata.setOutputNamespace(namespace)
        metadata.setName(tableName)
      }
    }

    def table: String = rawTable.cleanSpec

    def subPartitionFilters: Map[String, String] = {
      val subPartitionFiltersTry = Try(
        rawTable
          .split("/")
          .tail
          .map { partitionDef =>
            val splitPartitionDef = partitionDef.split("=")
            (splitPartitionDef.head, splitPartitionDef(1))
          }
          .toMap)

      subPartitionFiltersTry match {
        case Success(value) => value
        case Failure(exception) => {
          throw new Exception(s"Table ${rawTable} has mal-formatted sub-partitions", exception)
        }
      }
    }

    def isCumulative: Boolean = {
      if (source.isSetEntities) false else source.getEvents.isCumulative
    }

    def topic: String = {
      if (source.isSetEntities) {
        source.getEntities.getMutationTopic
      } else if (source.isSetEvents) {
        source.getEvents.getTopic
      } else if (source.isSetJoinSource) {
        source.getJoinSource.getJoin.getLeft.topic
      } else {
        null
      }
    }

    /**
      * If the streaming topic has additional args. Parse them to be used by streamingImpl.
      * Example: kafkatopic/schema=deserializationClass/version=2.0/host=host_url/port=9999
      * -> Map(schema -> deserializationClass, version -> 2.0, host -> host_url, port -> 9999)
      */
    def topicTokens: Map[String, String] = {
      source.topic
        .split("/")
        .drop(1)
        .map { tok =>
          val tokens = tok.split("=", 2)
          tokens(0) -> tokens(1)
        }
        .toMap
    }

    /**
      * Topic without kwargs
      */
    def cleanTopic: String = source.topic.cleanSpec

    def copyForVersioningComparison: Source = {
      // Makes a copy of the source and unsets date fields, used to compute equality on sources while ignoring these fields
      val newSource = source.deepCopy()
      val query = newSource.query
      query.unsetEndPartition()
      query.unsetStartPartition()
      newSource
    }
  }

  implicit class GroupByOps(groupBy: GroupBy) extends GroupBy(groupBy) {
    @transient lazy val logger = LoggerFactory.getLogger(getClass)
    def maxWindow: Option[Window] = {
      val allWindowsOpt = Option(groupBy.aggregations)
        .flatMap(_.toScala.toSeq.allWindowsOpt)
      allWindowsOpt.flatMap { windows =>
        if (windows.contains(null)) None
        else Some(windows.maxBy(_.millis))
      }
    }

    // Check if tiling is enabled for a given GroupBy. Defaults to false if the 'enable_tiling' flag isn't set.
    def isTilingEnabled: Boolean =
      groupBy.getMetaData.customJsonLookUp("enable_tiling") match {
        case s: Boolean => s
        case _          => false
      }

    def semanticHash: String = {
      val newGroupBy = groupBy.deepCopy()
      newGroupBy.unsetMetaData()
      logger.info(s"Computing semantic hash for GroupBy object: ${ThriftJsonCodec.toJsonStr(newGroupBy)}")
      ThriftJsonCodec.md5Digest(newGroupBy)
    }

    def dataModel: DataModel = {
      val models = groupBy.sources.toScala
        .map(_.dataModel)
      assert(models.distinct.length == 1,
             s"All source of the groupBy: ${groupBy.metaData.name} " +
               s"should be of the same type. Either 'Events' or 'Entities'")
      models.head
    }

    lazy val inferredAccuracy: Accuracy = inferredAccuracyImpl

    private def inferredAccuracyImpl: Accuracy = {
      // if user specified something - respect it
      if (groupBy.accuracy != null) return groupBy.accuracy
      // if a topic is specified - then treat it as temporally accurate
      val validTopics = groupBy.sources.toScala
        .map(_.topic)
        .filter(_ != null)
      if (validTopics.nonEmpty) Accuracy.TEMPORAL else Accuracy.SNAPSHOT
    }

    def setups: Seq[String] = {
      groupBy.sources
        .iterator()
        .toScala
        .map(_.query.setups)
        .flatMap(setupList => Option(setupList).map(_.iterator().toScala).getOrElse(Iterator.empty))
        .toSeq
        .distinct
    }

    def copyForVersioningComparison: GroupBy = {
      val newGroupBy = groupBy.deepCopy()
      newGroupBy.setMetaData(newGroupBy.metaData.copyForVersioningComparison)
      newGroupBy
    }

    lazy val batchDataset: String = s"${groupBy.metaData.cleanName.toUpperCase()}_BATCH"
    lazy val streamingDataset: String = s"${groupBy.metaData.cleanName.toUpperCase()}_STREAMING"

    def kvTable: String = s"${groupBy.metaData.outputTable}_upload"

    def streamingSource: Option[Source] =
      groupBy.sources.toScala
        .find(_.topic != null)

    // de-duplicate all columns necessary for aggregation in a deterministic order
    // so we use distinct instead of toSet here
    def aggregationInputs: Array[String] =
      groupBy.aggregations
        .iterator()
        .toScala
        .flatMap(agg =>
          Option(agg.buckets)
            .map(_.iterator().toScala.toSeq)
            .getOrElse(Seq.empty) :+ agg.inputColumn)
        .toArray
        .distinct

    def valueColumns: Array[String] =
      Option(groupBy.aggregations)
        .map(
          _.iterator().toScala
            .flatMap(agg => agg.unpack.map(_.outputColumnName))
            .toArray
        )
        .getOrElse(
          // no-agg case
          groupBy.sources
            .get(0)
            .query
            .selects
            .keySet()
            .iterator()
            .toScala
            .filterNot(groupBy.keyColumns.contains)
            .toArray
        )

    def keys(partitionColumn: String): Seq[String] = {
      val baseKeys = if (groupBy.isSetKeyColumns) groupBy.keyColumns.toScala else List()
      val partitionKey = if (baseKeys.contains(partitionColumn)) None else Some(partitionColumn)
      val timeKey =
        if (groupBy.inferredAccuracy == Accuracy.TEMPORAL && !baseKeys.contains(Constants.TimeColumn))
          Some(Constants.TimeColumn)
        else
          None

      baseKeys ++ partitionKey ++ timeKey
    }

    def hasDerivations: Boolean = groupBy.isSetDerivations && !groupBy.derivations.isEmpty
    lazy val derivationsScala: List[Derivation] =
      if (groupBy.hasDerivations) groupBy.derivations.toScala else List.empty
    lazy val derivationsContainStar: Boolean = groupBy.hasDerivations && derivationsScala.iterator.exists(_.name == "*")
    lazy val derivationsWithoutStar: List[Derivation] =
      if (groupBy.hasDerivations) derivationsScala.filterNot(_.name == "*") else List.empty
    lazy val areDerivationsRenameOnly: Boolean =
      groupBy.hasDerivations && groupBy.derivations.toScala.areDerivationsRenameOnly
    lazy val derivationExpressionSet: Set[String] =
      if (groupBy.hasDerivations) derivationsScala.iterator.map(_.expression).toSet else Set.empty

    case class QueryParts(selects: Option[Seq[String]], wheres: Seq[String])

    def buildQueryParts(query: Query): QueryParts = {
      val selects = query.getQuerySelects
      val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)

      val fillIfAbsent = (groupBy.dataModel match {
        case DataModel.Entities =>
          Map(Constants.ReversalColumn -> Constants.ReversalColumn,
              Constants.MutationTimeColumn -> Constants.MutationTimeColumn)
        case DataModel.Events => Map(Constants.TimeColumn -> timeColumn)
      })

      val baseWheres = Option(query.wheres).map(_.toScala).getOrElse(Seq.empty[String])
      val wheres = baseWheres ++ timeWheres(timeColumn)

      val allSelects = Option(selects).map(fillIfAbsent ++ _).map { m =>
        m.map {
          case (name, expr) => s"($expr) AS $name"
        }.toSeq
      }
      QueryParts(allSelects, wheres)
    }

    // build left streaming query for join source runner
    def buildLeftStreamingQuery(query: Query, defaultFieldNames: Seq[String]): String = {
      val queryParts = groupBy.buildQueryParts(query)
      val streamingInputTable =
        Option(groupBy.metaData.name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull + "_stream"
      s"""SELECT
         |  ${queryParts.selects.getOrElse(defaultFieldNames).mkString(",\n  ")}
         |FROM $streamingInputTable
         |WHERE ${queryParts.wheres.map("(" + _ + ")").mkString(" AND ")}
         |""".stripMargin
    }

    def buildStreamingQuery: String = {
      val streamingSource = groupBy.streamingSource.get
      if (!streamingSource.isSetJoinSource) {
        val query = streamingSource.query
        val selects = query.getQuerySelects
        val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
        val fillIfAbsent = groupBy.dataModel match {
          case DataModel.Entities =>
            Map(Constants.TimeColumn -> timeColumn,
                Constants.ReversalColumn -> null,
                Constants.MutationTimeColumn -> null)
          case DataModel.Events => Map(Constants.TimeColumn -> timeColumn)
        }
        val keys = groupBy.getKeyColumns.toScala

        val baseWheres = Option(query.wheres).map(_.toScala).getOrElse(Seq.empty[String])
        val selectMap = Option(selects).getOrElse(Map.empty[String, String])
        val keyWhereOption = keys
          .map { key =>
            s"${selectMap.getOrElse(key, key)} IS NOT NULL"
          }
          .mkString(" OR ")
        val streamingInputTable =
          Option(groupBy.metaData.name).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull + "_stream"
        QueryUtils.build(
          selects,
          streamingInputTable,
          baseWheres ++ timeWheres(timeColumn) :+ s"($keyWhereOption)",
          fillIfAbsent = if (selects == null) null else fillIfAbsent
        )
      } else {
        //todo: this logic is similar in JoinSourceRunner, we can simplify it to a single place
        val query = streamingSource.getJoinSource.join.left.query
        groupBy.buildLeftStreamingQuery(query, groupBy.keyColumns.toScala)
      }
    }

    private def timeWheres(timeColumn: String) = {
      groupBy.dataModel match {
        case DataModel.Entities => Seq(s"${Constants.MutationTimeColumn} is NOT NULL")
        case DataModel.Events   => Seq(s"$timeColumn is NOT NULL")
      }
    }

    def getPartitionColumn: Option[String] = {
      val partitionColumns = groupBy.sources.toScala.flatMap(_.partitionColumnOpt).distinct
      require(
        partitionColumns.length < 2,
        s"Expect all queries from a given group-by to have the same partition column. All sources should have identical schemas and same partition column name. Found distinct partition columns $partitionColumns for group-by ${groupBy.metaData.name}"
      )
      partitionColumns.headOption
    }
  }

  implicit class StringOps(string: String) {
    def sanitize: String = Option(string).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull

    def cleanSpec: String = string.split("/").head

    // derive a feature name key from path to file
    def confPathToKey: String = {
      // capture <conf_type>/<team>/<conf_name> as key e.g joins/team/team.example_join.v1
      string.split("/").takeRight(3).mkString("/")
    }
  }

  implicit class ExternalSourceOps(externalSource: ExternalSource) extends ExternalSource(externalSource) {
    private def schemaNames(schema: TDataType): Array[String] = schemaFields(schema).map(_.name)

    private def schemaFields(schema: TDataType): Array[StructField] =
      schema.params.toScala
        .map(field => StructField(field.name, DataType.fromTDataType(field.dataType)))
        .toArray

    lazy val keyNames: Array[String] = schemaNames(externalSource.keySchema)
    lazy val valueNames: Array[String] = schemaNames(externalSource.valueSchema)
    lazy val keyFields: Array[StructField] = schemaFields(externalSource.keySchema)
    lazy val valueFields: Array[StructField] = schemaFields(externalSource.valueSchema)

    def isContextualSource: Boolean = externalSource.metadata.name == Constants.ContextualSourceName
  }

  object KeyMappingHelper {
    // key mapping is defined as {left_col1: right_col1}, on the right there can be two keys [right_col1, right_col2]
    // Left is implicitly assumed to have right_col2
    // We need to convert a map {left_col1: a, right_col2: b, irrelevant_col: c} into {right_col1: a, right_col2: b}
    // The way to do this efficiently is to "flip" the keymapping into {right_col1: left_col1} and save it.
    // And later "apply" the flipped mapping.
    def flip(leftToRight: java.util.Map[String, String]): Map[String, String] = {
      Option(leftToRight)
        .map(mp =>
          mp.toScala
            .map({ case (key, value) => value -> key }))
        .getOrElse(Map.empty[String, String])
    }
  }

  implicit class ExternalPartOps(externalPart: ExternalPart) extends ExternalPart(externalPart) {
    lazy val fullName: String =
      Constants.ExternalPrefix + "_" +
        Option(externalPart.prefix).map(_ + "_").getOrElse("") +
        externalPart.source.metadata.name.sanitize

    def apply(query: Map[String, Any], flipped: Map[String, String], right_keys: Seq[String]): Map[String, AnyRef] = {
      val rightToLeft = right_keys.map(k => k -> flipped.getOrElse(k, k))
      val missingKeys = rightToLeft.iterator.map(_._2).filterNot(query.contains).toSet

      // for contextual features, we automatically populate null if any of the keys are missing
      // otherwise, an exception is thrown which will be converted to soft-fail in Fetcher code
      if (missingKeys.nonEmpty && !externalPart.source.isContextualSource) {
        throw KeyMissingException(externalPart.source.metadata.name, missingKeys.toSeq, query)
      }
      rightToLeft.map {
        case (rightKey, leftKey) => rightKey -> query.getOrElse(leftKey, null).asInstanceOf[AnyRef]
      }.toMap
    }

    def applyMapping(query: Map[String, Any]): Map[String, AnyRef] =
      apply(query, rightToLeft, keyNames)

    lazy val rightToLeft: Map[String, String] = KeyMappingHelper.flip(externalPart.keyMapping)
    private lazy val keyNames = externalPart.source.keyNames

    def semanticHash: String = {
      val newExternalPart = externalPart.deepCopy()
      newExternalPart.source.unsetMetadata()
      ThriftJsonCodec.md5Digest(newExternalPart)
    }

    lazy val keySchemaFull: Array[StructField] = externalPart.source.keyFields.map(field =>
      StructField(externalPart.rightToLeft.getOrElse(field.name, field.name), field.fieldType))

    lazy val valueSchemaFull: Array[StructField] =
      externalPart.source.valueFields.map(field => StructField(fullName + "_" + field.name, field.fieldType))

    def isContextual: Boolean = externalPart.source.isContextualSource
  }

  implicit class JoinPartOps(joinPart: JoinPart) extends JoinPart(joinPart) {
    lazy val fullPrefix = (Option(prefix) ++ Some(groupBy.getMetaData.cleanName)).mkString("_")
    lazy val leftToRight: Map[String, String] = rightToLeft.map { case (key, value) => value -> key }

    def valueColumns: Seq[String] = joinPart.groupBy.valueColumns.map(fullPrefix + "_" + _)

    def rightToLeft: Map[String, String] = {
      val rightToRight = joinPart.groupBy.keyColumns.toScala.map { key => key -> key }.toMap
      Option(joinPart.keyMapping)
        .map { leftToRight =>
          val rToL = leftToRight.toScala.map {
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

    def constructJoinPartSchema(schemaField: StructField): StructField = {
      StructField(joinPart.fullPrefix + "_" + schemaField.name, schemaField.fieldType)
    }
  }

  implicit class LabelPartOps(val labelPart: LabelPart) extends Serializable {
    def leftKeyCols: Array[String] = {
      labelPart.labels.toScala
        .flatMap {
          _.rightToLeft.values
        }
        .toSet
        .toArray
    }

    def setups: Seq[String] = {
      labelPart.labels.toScala
        .flatMap(_.groupBy.setups)
        .distinct
    }

    // a list of columns which can identify a row on left, use user specified columns by default
    def rowIdentifier(userRowId: util.List[String] = null, partitionColumn: String): Array[String] = {
      if (userRowId != null && !userRowId.isEmpty) {
        if (!userRowId.contains(partitionColumn))
          userRowId.toScala.toArray ++ Array(partitionColumn)
        else
          userRowId.toScala.toArray
      } else
        leftKeyCols ++ Array(partitionColumn)
    }
  }

  implicit class BootstrapPartOps(val bootstrapPart: BootstrapPart) extends Serializable {

    /**
      * Compress the info such that the hash can be stored at record and
      * used to track which records are populated by which bootstrap tables
      */
    def semanticHash: String = {
      val newPart = bootstrapPart.deepCopy()
      bootstrapPart.unsetMetaData()
      ThriftJsonCodec.md5Digest(newPart)
    }

    def keys(join: Join, partitionColumn: String): Seq[String] = {
      val definedKeys = if (bootstrapPart.isSetKeyColumns) {
        bootstrapPart.keyColumns.toScala
      } else if (join.isSetRowIds) {
        join.getRowIds.toScala
      } else {
        throw new Exception(s"Bootstrap's join key for bootstrap is NOT set for join ${join.metaData.name}")
      }
      if (definedKeys.contains(partitionColumn)) {
        definedKeys
      } else {
        definedKeys :+ partitionColumn
      }
    }

    lazy val startPartition: String = Option(bootstrapPart.query).map(_.startPartition).orNull
    lazy val endPartition: String = Option(bootstrapPart.query).map(_.endPartition).orNull
  }

  object JoinOps {
    private val identifierRegex: Pattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*")
    def isIdentifier(s: String): Boolean = identifierRegex.matcher(s).matches()
  }

  implicit class JoinOps(val join: Join) extends Serializable {
    @transient lazy val logger = LoggerFactory.getLogger(getClass)
    // all keys as they should appear in left that are being used on right
    def leftKeyCols: Array[String] = {
      join.joinParts.toScala
        .flatMap {
          _.rightToLeft.values
        }
        .toSet
        .toArray
    }

    def historicalBackfill: Boolean = {
      if (join.metaData.isSetHistoricalBackfill) {
        join.metaData.historicalBackfill
      } else {
        true
      }
    }

    def computedFeatureCols: Array[String] =
      if (Option(join.derivations).isDefined) {
        val baseColumns = joinPartOps.flatMap(_.valueColumns).toArray
        val baseExpressions = if (join.derivationsContainStar) baseColumns.filterNot {
          join.derivationExpressionSet contains _
        }
        else Array.empty[String]
        baseExpressions ++ join.derivationsWithoutStar.map { d =>
          d.name
        }
      } else
        joinPartOps.flatMap(_.valueColumns).toArray

    def partOutputTable(jp: JoinPart): String =
      (Seq(join.metaData.outputTable) ++ Option(jp.prefix) :+ jp.groupBy.metaData.cleanName).mkString("_")

    val leftSourceKey: String = "left_source"
    val derivedKey: String = "derived"

    /*
     * semanticHash contains hashes of left side and each join part, and is used to detect join definition
     * changes and determine whether any intermediate/final tables of the join need to be recomputed.
     */
    private[api] def baseSemanticHash: Map[String, String] = {
      val leftHash = ThriftJsonCodec.md5Digest(join.left)
      logger.info(s"Join Left Object: ${ThriftJsonCodec.toJsonStr(join.left)}")
      val partHashes = join.joinParts.toScala.map { jp => partOutputTable(jp) -> jp.groupBy.semanticHash }.toMap
      val derivedHashMap = Option(join.derivations)
        .map { derivations =>
          val derivedHash =
            derivations.iterator().toScala.map(d => s"${d.expression} as ${d.name}").mkString(", ").hashCode.toHexString
          Map(derivedKey -> derivedHash)
        }
        .getOrElse(Map.empty)
      val bootstrapHash = ThriftJsonCodec.md5Digest(join.bootstrapParts)
      partHashes ++ Map(leftSourceKey -> leftHash, join.metaData.bootstrapTable -> bootstrapHash) ++ derivedHashMap
    }

    /*
     * Unset topic / mutationTopic in everywhere for this join recursively.
     * Input join will be modified in place.
     */
    private def cleanTopic(join: Join): Join = {
      def cleanTopicInSource(source: Source): Unit = {
        if (source.isSetEvents) {
          source.getEvents.unsetTopic()
        } else if (source.isSetEntities) {
          source.getEntities.unsetMutationTopic()
        } else if (source.isSetJoinSource) {
          cleanTopic(source.getJoinSource.getJoin)
        }
      }

      cleanTopicInSource(join.left)
      join.getJoinParts.toScala.foreach(_.groupBy.sources.toScala.foreach(cleanTopicInSource))
      join
    }

    /*
     * Compute variants of semantic_hash with different flags. A flag is stored on Hive metadata and used to
     * indicate which version of semantic_hash logic to use.
     */
    def semanticHash(excludeTopic: Boolean): Map[String, String] = {
      if (excludeTopic) {
        // WARN: deepCopy doesn't guarantee same semantic_hash will be produced due to reordering of map keys
        // but the behavior is deterministic
        val joinCopy = join.deepCopy()
        cleanTopic(joinCopy)
        joinCopy.baseSemanticHash
      } else {
        baseSemanticHash
      }
    }

    /*
    External features computed in online env and logged
    This method will get the external feature column names
     */
    def getExternalFeatureCols: Seq[String] = {
      Option(join.onlineExternalParts)
        .map(_.toScala
          .map { part =>
            {
              val keys = part.source.getKeySchema.params.toScala
                .map(_.name)
              val values = part.source.getValueSchema.params.toScala
                .map(_.name)
              keys ++ values
            }
          }
          .flatMap(_.toSet))
        .getOrElse(Seq.empty)
    }

    def isProduction: Boolean = join.getMetaData.isProduction

    def team: String = join.getMetaData.getTeam

    private def generateSkewFilterSql(key: String, values: Seq[String]): String = {
      val nulls = Seq("null", "Null", "NULL")
      val nonNullFilters = Some(s"$key NOT IN (${values.filterNot(nulls.contains).mkString(", ")})")
      val nullFilters = if (values.exists(nulls.contains)) Some(s"$key IS NOT NULL") else None
      (nonNullFilters ++ nullFilters).mkString(" AND ")
    }

    // TODO: validate that non keys are not specified in - join.skewKeys
    def skewFilter(keys: Option[Seq[String]] = None, joiner: String = " OR "): Option[String] = {
      Option(join.skewKeys).map { jmap =>
        val result = jmap.toScala
          .filterKeys(key =>
            keys.forall {
              _.contains(key)
            })
          .map {
            case (leftKey, values) =>
              assert(
                leftKeyCols.contains(leftKey),
                s"specified skew filter for $leftKey is not used as a key in any join part. " +
                  s"Please specify key columns in skew filters: [${leftKeyCols.mkString(", ")}]"
              )
              generateSkewFilterSql(leftKey, values.toScala.toSeq)
          }
          .filter(_.nonEmpty)
          .mkString(joiner)
        logger.info(s"Generated join left side skew filter:\n    $result")
        result
      }
    }

    def partSkewFilter(joinPart: JoinPart, joiner: String = " OR "): Option[String] = {
      Option(join.skewKeys).flatMap { jmap =>
        val result = jmap.toScala
          .flatMap {
            case (leftKey, values) =>
              Option(joinPart.keyMapping)
                .map(_.toScala.getOrElse(leftKey, leftKey))
                .orElse(Some(leftKey))
                .filter(joinPart.groupBy.keyColumns.contains(_))
                .map(generateSkewFilterSql(_, values.toScala))
          }
          .filter(_.nonEmpty)
          .mkString(joiner)

        if (result.nonEmpty) {
          logger.info(s"Generated join part skew filter for ${joinPart.groupBy.metaData.name}:\n    $result")
          Some(result)
        } else None
      }
    }

    def setups: Seq[String] =
      (join.left.query.setupsSeq ++ join.joinParts.toScala
        .flatMap(_.groupBy.setups)).distinct

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

    lazy val joinPartOps: Seq[JoinPartOps] =
      Option(join.joinParts)
        .getOrElse(new util.ArrayList[JoinPart]())
        .toScala
        .toSeq
        .map(new JoinPartOps(_))

    def logFullValues: Boolean = true // TODO: supports opt-out in the future

    def hasDerivations: Boolean = join.isSetDerivations && !join.derivations.isEmpty
    lazy val derivationsScala: List[Derivation] = if (join.hasDerivations) join.derivations.toScala else List.empty
    lazy val derivationsContainStar: Boolean = join.hasDerivations && derivationsScala.iterator.exists(_.name == "*")
    lazy val derivationsWithoutStar: List[Derivation] =
      if (join.hasDerivations) derivationsScala.filterNot(_.name == "*") else List.empty
    lazy val areDerivationsRenameOnly: Boolean = join.hasDerivations && derivationsScala.areDerivationsRenameOnly
    lazy val derivationExpressionSet: Set[String] =
      if (join.hasDerivations) derivationsScala.iterator.map(_.expression).toSet else Set.empty
  }

  implicit class StringsOps(strs: Iterable[String]) {
    def pretty: String = {
      if (strs.nonEmpty)
        "\n    " + strs.mkString(",\n    ") + "\n"
      else
        ""
    }

    def prettyInline: String = strs.mkString("[", ",", "]")
  }

  implicit class QueryOps(query: Query) {
    def setupsSeq: Seq[String] = {
      Option(query.setups)
        .map(
          _.toScala.toSeq
        )
        .getOrElse(Seq.empty)
    }

    def getQuerySelects: Map[String, String] = Option(query.selects).map(_.toScala.toMap).orNull
  }

  implicit class ThrowableOps(throwable: Throwable) {
    def traceString: String = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      throwable.printStackTrace(pw)
      sw.toString();
    }
  }

  implicit class DerivationOps(derivations: List[Derivation]) {
    lazy val derivationsContainStar: Boolean = derivations.iterator.exists(_.name == "*")
    lazy val derivationsWithoutStar: List[Derivation] = derivations.filterNot(_.name == "*")
    lazy val areDerivationsRenameOnly: Boolean = derivationsWithoutStar.forall(d => JoinOps.isIdentifier(d.expression))
    lazy val derivationExpressionSet: Set[String] = derivations.iterator.map(_.expression).toSet
    lazy val derivationExpressionFlippedMap: Map[String, String] =
      derivationsWithoutStar.map(d => d.expression -> d.name).toMap
    lazy val renameOnlyDerivations: List[Derivation] =
      derivationsWithoutStar.filter(d => JoinOps.isIdentifier(d.expression))

    // Used during offline spark job and this method preserves ordering of derivations
    def derivationProjection(baseColumns: Seq[String]): Seq[(String, String)] = {
      val wildcardDerivations = if (derivationsContainStar) { // select all baseColumns except renamed ones
        val expressions = derivations.iterator.map(_.expression).toSet
        baseColumns.filterNot(expressions)
      } else {
        Seq.empty
      }

      derivations.iterator.flatMap { d =>
        if (d.name == "*") {
          wildcardDerivations.map(c => c -> c)
        } else {
          Seq(d.name -> d.expression)
        }
      }.toSeq
    }

    def finalOutputColumn(baseColumns: Seq[String]): Seq[Column] = {
      val projections = derivationProjection(baseColumns)
      val finalOutputColumns = projections
        .flatMap {
          case (name, expression) => Some(expr(expression).as(name))
        }
      finalOutputColumns.toSeq
    }

    // Used only during online fetching to reduce latency
    def applyRenameOnlyDerivation(keys: Map[String, Any], values: Map[String, Any]): Map[String, Any] = {
      assert(
        areDerivationsRenameOnly,
        s"Derivations contain more complex expressions than simple renames: ${derivations.map(d => (d.name, d.expression))}")
      val wildcardDerivations = if (derivationsContainStar) {
        values.filterNot(derivationExpressionSet contains _._1)
      } else {
        Map.empty[String, Any]
      }
      wildcardDerivations ++ derivationsWithoutStar
        .map(d => d.name -> (keys ++ values).getOrElse(d.expression, null))
        .toMap
    }
  }
}
