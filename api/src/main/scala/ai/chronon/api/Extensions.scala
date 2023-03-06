package ai.chronon.api

import ai.chronon.api.DataModel._
import ai.chronon.api.Operation._
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper

import java.io.{PrintWriter, StringWriter}
import java.util
import java.util.regex.Pattern
import scala.collection.{Seq, mutable}
import scala.util.ScalaJavaConversions.{IteratorOps, ListOps, MapOps}
import scala.util.matching.Regex
import scala.util.{Failure, ScalaVersionSpecificCollectionsConverter, Success, Try}

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
    private def unbounded: Boolean = window.length == Int.MaxValue || window.length < 0

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

    def uploadTable = s"${outputTable}_upload"

    def copyForVersioningComparison: MetaData = {
      // Changing name results in column rename, therefore schema change, other metadata changes don't effect output table
      val newMetaData = new MetaData()
      newMetaData.setName(metaData.name)
      newMetaData
    }

    def tableProps: Map[String, String] =
      Option(metaData.tableProperties)
        .map(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).toMap)
        .orNull

    def nameToFilePath: String = metaData.name.replaceFirst("\\.", "/")

    // helper function to extract values from customJson
    def customJsonLookUp(key: String): Any = {
      if (metaData.customJson == null) return null
      val mapper = new ObjectMapper();
      val typeRef = new TypeReference[java.util.HashMap[String, Object]]() {}
      val jMap: java.util.Map[String, Object] = mapper.readValue(metaData.customJson, typeRef)
      ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(jMap).get(key).orNull
    }

    def owningTeam: String = {
      val teamOverride = Try(customJsonLookUp(Constants.TeamOverride).asInstanceOf[String]).toOption
      teamOverride.getOrElse(metaData.team)
    }
  }

  // one per output column - so single window
  // not exposed to users
  implicit class AggregationPartOps(aggregationPart: AggregationPart) {

    def getInt(arg: String, default: Option[Int] = None): Int = {
      val argOpt = Option(aggregationPart.argMap)
        .flatMap(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).get(arg))
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
        .map(ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(_))
        .getOrElse(Seq(null))
        .toSeq
      for (bucket <- buckets) yield {
        Builders.AggregationPart(
          aggregation.operation,
          aggregation.inputColumn,
          WindowUtils.Unbounded,
          Option(aggregation.argMap)
            .map(
              ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).toMap
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
          .map(ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(_))
          .getOrElse(Seq(null))
        val windows = Option(agg.windows)
          .map(ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(_))
          .getOrElse(Seq(WindowUtils.Unbounded))
        for (bucket <- buckets) {
          perBucket += Builders.AggregationPart(
            agg.operation,
            agg.inputColumn,
            WindowUtils.Unbounded,
            Option(agg.argMap)
              .map(
                ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).toMap
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
                                           ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).toMap
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
              ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(_)
            )
            .getOrElse(Seq(null))
        }
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

    lazy val rawTable: String =
      if (source.isSetEntities) source.getEntities.getSnapshotTable else source.getEvents.getTable

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

    def topic: String = {
      if (source.isSetEntities) source.getEntities.getMutationTopic else source.getEvents.getTopic
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
    def maxWindow: Option[Window] = {
      val allWindowsOpt = Option(groupBy.aggregations)
        .flatMap(ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(_).toSeq.allWindowsOpt)
      allWindowsOpt.flatMap { windows =>
        if (windows.contains(null)) None
        else Some(windows.maxBy(_.millis))
      }
    }

    def semanticHash: String = {
      val newGroupBy = groupBy.deepCopy()
      newGroupBy.unsetMetaData()
      ThriftJsonCodec.md5Digest(newGroupBy)
    }

    def dataModel: DataModel = {
      val models = ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(groupBy.sources)
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
      val validTopics = ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(groupBy.sources)
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
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(groupBy.sources)
        .find(_.topic != null)

    def buildStreamingQuery: String = {
      assert(streamingSource.isDefined,
             s"You should probably define a topic in one of your sources: ${groupBy.metaData.name}")
      val query = streamingSource.get.query
      val selects = Option(query.selects)
        .map(
          ScalaVersionSpecificCollectionsConverter
            .convertJavaMapToScala(_)
            .toMap)
        .orNull
      val timeColumn = Option(query.timeColumn).getOrElse(Constants.TimeColumn)
      val fillIfAbsent = if (selects == null) null else Map(Constants.TimeColumn -> timeColumn)
      val keys = ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(groupBy.getKeyColumns)

      val baseWheres = Option(query.wheres)
        .map(_.toScala)
        .getOrElse(Seq.empty[String])
      val keyWhereOption =
        Option(selects)
          .map { selectsMap =>
            keys
              .map(key => s"(${selectsMap(key)} is NOT NULL)")
              .mkString(" OR ")
          }
      val timeWheres = Seq(s"$timeColumn is NOT NULL")

      QueryUtils.build(
        selects,
        Constants.StreamingInputTable,
        baseWheres.toSeq ++ timeWheres.toSeq ++ keyWhereOption.toSeq,
        fillIfAbsent = fillIfAbsent
      )
    }

    def aggregationInputs: Array[String] =
      groupBy.aggregations
        .iterator()
        .toScala
        .flatMap(agg =>
          Option(agg.buckets)
            .map(_.iterator().toScala.toSeq)
            .getOrElse(Seq.empty) :+ agg.inputColumn)
        .toSet
        .toArray

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
  }

  implicit class StringOps(string: String) {
    def sanitize: String = Option(string).map(_.replaceAll("[^a-zA-Z0-9_]", "_")).orNull

    def cleanSpec: String = string.split("/").head
  }

  implicit class ExternalSourceOps(externalSource: ExternalSource) extends ExternalSource(externalSource) {
    private def schemaNames(schema: TDataType): Array[String] = schemaFields(schema).map(_.name)

    private def schemaFields(schema: TDataType): Array[StructField] =
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(schema.params)
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
          ScalaVersionSpecificCollectionsConverter
            .convertJavaMapToScala(mp)
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
      val rightToRight = ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(joinPart.groupBy.keyColumns)
        .map { key => key -> key }
        .toMap
      Option(joinPart.keyMapping)
        .map { leftToRight =>
          val rToL = ScalaVersionSpecificCollectionsConverter
            .convertJavaMapToScala(leftToRight)
            .map {
              case (left, right) => right -> left
            }
            .toMap
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
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(labelPart.labels)
        .flatMap {
          _.rightToLeft.values
        }
        .toSet
        .toArray
    }

    def setups: Seq[String] = {
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(labelPart.labels)
        .flatMap(_.groupBy.setups)
        .distinct
    }

    // a list of columns which can identify a row on left, use user specified columns by default
    def rowIdentifier(userRowId: util.List[String] = null): Array[String] = {
      if (userRowId != null && !userRowId.isEmpty) {
        if (!userRowId.contains(Constants.PartitionColumn))
          userRowId.toScala.toArray ++ Array(Constants.PartitionColumn)
        else
          userRowId.toScala.toArray
      } else
        leftKeyCols ++ Array(Constants.PartitionColumn)
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

    def keys(join: Join): Seq[String] = {
      val definedKeys = if (bootstrapPart.isSetKeyColumns) {
        ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(bootstrapPart.keyColumns)
      } else if (join.isSetRowIds) {
        ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(join.getRowIds)
      } else {
        throw new Exception(s"Bootstrap's join key for bootstrap is NOT set for join ${join.metaData.name}")
      }
      if (definedKeys.contains(Constants.PartitionColumn)) {
        definedKeys
      } else {
        definedKeys :+ Constants.PartitionColumn
      }
    }

    def isLogBootstrap(join: Join): Boolean = {
      (bootstrapPart.table == join.metaData.loggedTable) && !(bootstrapPart.isSetQuery && bootstrapPart.getQuery.isSetSelects)
    }
  }

  object JoinOps {
    private val identifierRegex: Pattern = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*")
    def isIdentifier(s: String): Boolean = identifierRegex.matcher(s).matches()
  }

  implicit class JoinOps(val join: Join) extends Serializable {
    // all keys on left
    def leftKeyCols: Array[String] = {
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(join.joinParts)
        .flatMap {
          _.rightToLeft.values
        }
        .toSet
        .toArray
    }

    def partOutputTable(jp: JoinPart): String =
      (Seq(join.metaData.outputTable) ++ Option(jp.prefix) :+ jp.groupBy.metaData.cleanName).mkString("_")

    private val leftSourceKey: String = "left_source"
    private val derivedKey: String = "derived"

    /*
     * semanticHash contains hashes of left side and each join part, and is used to detect join definition
     * changes and determine whether any intermediate/final tables of the join need to be recomputed.
     */
    def semanticHash: Map[String, String] = {
      val leftHash = ThriftJsonCodec.md5Digest(join.left)
      val partHashes = ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(join.joinParts)
        .map { jp => partOutputTable(jp) -> jp.groupBy.semanticHash }
        .toMap
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
    External features computed in online env and logged
    This method will get the external feature column names
     */
    def getExternalFeatureCols: Seq[String] = {
      Option(join.onlineExternalParts)
        .map(
          ScalaVersionSpecificCollectionsConverter
            .convertJavaListToScala(_)
            .map { part =>
              {
                val keys = ScalaVersionSpecificCollectionsConverter
                  .convertJavaListToScala(part.source.getKeySchema.params)
                  .map(_.name)
                val values = ScalaVersionSpecificCollectionsConverter
                  .convertJavaListToScala(part.source.getValueSchema.params)
                  .map(_.name)
                keys ++ values
              }
            }
            .flatMap(_.toSet))
        .getOrElse(Seq.empty)
    }

    /*
     * onlineSemanticHash includes everything in semanticHash as well as hashes of each onlineExternalParts (which only
     * affect online serving but not offline table generation).
     * It is used to detect join definition change in online serving and to update ttl-cached conf files.
     */
    def onlineSemanticHash: Map[String, String] = {
      if (join.onlineExternalParts == null) {
        return Map.empty[String, String]
      }

      val externalPartHashes = ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(join.onlineExternalParts)
        .map { part => part.fullName -> part.semanticHash }
        .toMap

      externalPartHashes ++ semanticHash
    }

    def tablesToDrop(oldSemanticHash: Map[String, String]): Seq[String] = {
      val newSemanticHash = semanticHash
      // only right join part hashes for convenience
      def partHashes(semanticHashMap: Map[String, String]): Map[String, String] = {
        semanticHashMap.filter { case (name, _) => name != leftSourceKey && name != derivedKey }
      }

      // drop everything if left source changes
      val partsToDrop = if (oldSemanticHash(leftSourceKey) != newSemanticHash(leftSourceKey)) {
        partHashes(oldSemanticHash).keys.toSeq
      } else {
        val changed = partHashes(newSemanticHash).flatMap {
          case (key, newVal) =>
            oldSemanticHash.get(key).filter(_ != newVal).map(_ => key)
        }
        val deleted = partHashes(oldSemanticHash).keys.filterNot(newSemanticHash.contains)
        (changed ++ deleted).toSeq
      }
      val added = newSemanticHash.keys.filter(!oldSemanticHash.contains(_)).filter {
        // introduce boostrapTable as a semantic_hash but skip dropping to avoid recompute if it is empty
        case key if key == join.metaData.bootstrapTable => join.isSetBootstrapParts && !join.bootstrapParts.isEmpty
        case _                                          => true
      }
      val derivedChanges = oldSemanticHash.get(derivedKey) != newSemanticHash.get(derivedKey)
      // TODO: make this incremental, retain the main table and continue joining, dropping etc
      val mainTable = if (partsToDrop.nonEmpty || added.nonEmpty || derivedChanges) {
        Some(join.metaData.outputTable)
      } else None
      partsToDrop ++ mainTable
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
        val result = ScalaVersionSpecificCollectionsConverter
          .convertJavaMapToScala(jmap)
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
              generateSkewFilterSql(leftKey,
                                    ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(values).toSeq)
          }
          .filter(_.nonEmpty)
          .mkString(joiner)
        println(s"Generated join left side skew filter:\n    $result")
        result
      }
    }

    def partSkewFilter(joinPart: JoinPart, joiner: String = " OR "): Option[String] = {
      Option(join.skewKeys).map { jmap =>
        val result = ScalaVersionSpecificCollectionsConverter
          .convertJavaMapToScala(jmap)
          .flatMap {
            case (leftKey, values) =>
              val replacedKey = Option(joinPart.keyMapping)
                .map {
                  ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).getOrElse(leftKey, leftKey)
                }
                .getOrElse(leftKey)
              if (joinPart.groupBy.keyColumns.contains(replacedKey))
                Some(
                  generateSkewFilterSql(replacedKey,
                                        ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(values).toSeq))
              else None
          }
          .filter(_.nonEmpty)
          .mkString(joiner)
        println(s"Generated join part skew filter for ${joinPart.groupBy.metaData.name}:\n    $result")
        result
      }
    }

    def setups: Seq[String] =
      (join.left.query.setupsSeq ++ ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(join.joinParts)
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
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(Option(join.joinParts).getOrElse(new util.ArrayList[JoinPart]()))
        .toSeq
        .map(new JoinPartOps(_))

    private lazy val derivationsScala = join.derivations.toScala
    lazy val derivationsContainStar: Boolean = derivationsScala.iterator.exists(_.name == "*")
    lazy val derivationsWithoutStar: List[Derivation] = derivationsScala.filterNot(_.name == "*")
    lazy val areDerivationsRenameOnly: Boolean = derivationsWithoutStar.forall(d => JoinOps.isIdentifier(d.expression))
    lazy val derivationExpressionSet: Set[String] = derivationsScala.iterator.map(_.expression).toSet
    lazy val derivationExpressionFlippedMap: Map[String, String] =
      derivationsWithoutStar.map(d => d.expression -> d.name).toMap

    def derivationProjection(baseColumns: Seq[String]): Seq[(String, String)] = {
      (if (derivationsContainStar) { // select all baseColumns except renamed ones
         val expressions = derivationsScala.iterator.map(_.expression).toSet
         baseColumns.filterNot(expressions)
       } else {
         Seq.empty
       }).map(c => c -> c) ++ derivationsWithoutStar.map(d => (d.name, d.expression))
    }

    def applyRenameOnlyDerivation(baseColumns: Map[String, Any]): Map[String, Any] = {
      assert(
        areDerivationsRenameOnly,
        s"Derivations contain more complex expressions than simple renames: ${derivationsScala.map(d => (d.name, d.expression))}")
      if (derivationsContainStar) {
        baseColumns.filterNot(derivationExpressionSet contains _._1)
      } else {
        Map.empty
      } ++ derivationsScala.map(d => d.name -> baseColumns.getOrElse(d.expression, null)).toMap
    }
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
          ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(_).toSeq
        )
        .getOrElse(Seq.empty)
    }
  }

  implicit class ThrowableOps(throwable: Throwable) {
    def traceString: String = {
      val sw = new StringWriter()
      val pw = new PrintWriter(sw)
      throwable.printStackTrace(pw)
      sw.toString();
    }
  }
}
