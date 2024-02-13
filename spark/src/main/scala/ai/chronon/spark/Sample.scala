package ai.chronon.spark

import java.io.{BufferedWriter, File, FileWriter}

import scala.compat.java8.FunctionConverters.enrichAsJavaFunction
import scala.io.Source
import scala.jdk.CollectionConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.spark.Driver.{parseConf}
import ai.chronon.spark.SampleHelper.{getPriorRunManifestMetadata, writeManifestMetadata}
import com.google.gson.{Gson, GsonBuilder}
import com.google.gson.reflect.TypeToken
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class Sample(conf: Any,
             tableUtils: TableUtils,
             startDate: String,
             endDate: String,
             outputDir: String,
             forceResample: Boolean = false,
             numRows: Int = 100) {

  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  val MANIFEST_FILE_NAME = "manifest.json"
  val MANIFEST_SERDE_TYPE_TOKEN = new TypeToken[java.util.Map[String, Integer]](){}.getType

  val jobSemanticsMetadata: Map[String, Int] = {
    Map("numRows" -> numRows, "startDate" -> dsToInt(startDate), "endDate" -> dsToInt(endDate))
  }

  def sampleSource(table: String, dateRange: PartitionRange, includeLimit: Boolean,
      keySetOpt: Option[Seq[Map[String, Array[Any]]]] = None): DataFrame = {

    val outputFile = s"$outputDir/$table"
    val additionalFilterString = keySetOpt.map { keySetList =>

      val keyFilterWheres = keySetList.map { keySet =>
        val filterList = keySet.map { case (keyName: String, values: Array[Any]) =>
          val valueSet = values.map {
            case s: String => s"'$s'" // Add single quotes for string values
            case other => other.toString // Keep other types (like Int) as they are
          }.toSet
          s"$keyName in (${valueSet.mkString(sep = ",")})"
        }
        s"(${filterList.mkString(sep = " AND \n")})" // For multi-key expressions, we AND to
        // match all keys
      }
      s"AND ${keyFilterWheres.mkString(" OR ")}" // For the top-level key filter clause, we OR to
      // get all rows for all GroupBys
    }.getOrElse("")

    val limitString = if (includeLimit) s"LIMIT $numRows" else ""

    val sql =
      s"""
         |SELECT * FROM $table
         |WHERE ds >= "${dateRange.start}" AND
         |ds <= "${dateRange.end}"
         |$additionalFilterString
         |$limitString
         |""".stripMargin

    logger.info(s"Gathering Data with Query: $sql")

    val df = tableUtils.sparkSession.sql(sql)
    if (df.isEmpty) {
      logger.error(s"Could not fetch any data from table $table for the requested dates. Consider" +
        s" changing " +
        s"the date range, or investigate missing upstream data.")
      System.exit(1)
    }
    logger.info(s"Writing to: $outputFile")
    df.write.mode("overwrite").parquet(outputFile)
    df
  }

  def createKeyFilters(keys: Seq[String], df: DataFrame): Map[String, Array[Any]] = {
    keys.map{ key =>
      val values = df.select(key).collect().map(row => row(0))
      key -> values
    }.toMap
  }

  def getTableSemanticHash(source: api.Source, groupBy: api.GroupBy): Int = {
    // For a groupBy source, the relevant semantic fields include the filters, the keys at the GB level,
    // and the job semantic metadata
    val semanticFields: Map[String, Any] = Map(
      source.rootTable -> source.query.getWheres.asScala.hashCode(),
      "keys" -> groupBy.keyColumns.asScala
    ) ++ jobSemanticsMetadata
    semanticFields.hashCode()
  }


  /*
  For a given GroupBy, sample all the sources. Can optionally take a set of keys to use for sampling
  (used by the join sample path).
   */
  def sampleGroupBy(groupBy: api.GroupBy): Unit = {

    val tablesToHashes = groupBy.sources.asScala.map { source =>
      source.rootTable -> getTableSemanticHash(source, groupBy)
    }.toMap

    val sourcesDiff = getTablesWithSemanticDiff(tablesToHashes)
    if (sourcesDiff.isEmpty) {
      logger.error(s"No further sampling required based on metadata at $MANIFEST_FILE_NAME. The intended behavior" +
        s"in the case is to not execute this job.")
    } else {
      logger.info(s"Running sampling for the following sources: ${sourcesDiff.mkString(", ")}")
      val sourcesToSample = groupBy.sources.asScala.filter { source =>
        sourcesDiff.contains(source.rootTable)}

      sourcesToSample.foreach { source =>
        val range: PartitionRange = GroupBy.getIntersectedRange(
          source,
          PartitionRange(startDate, endDate)(tableUtils),
          tableUtils,
          groupBy.maxWindow)

        val keys = groupBy.keyColumns.asScala

        // Run a query to get the distinct key values that we will sample
        val distinctKeysSql = s"""
                                 |SELECT DISTINCT ${keys.mkString(", ")}
                                 |FROM ${source.rootTable}
                                 |WHERE ds >= $startDate AND
                                 |ds <= $endDate
                                 |LIMIT $numRows
                                 |""".stripMargin

        val distinctKeysDf = tableUtils.sparkSession.sql(distinctKeysSql)

        val keyFilters: Map[String, Array[Any]] = createKeyFilters(keys, distinctKeysDf)

        // We don't need to do anything with the output df in this case
        sampleSource(source.rootTable, range, false, Option(Seq(keyFilters)))
      }

      writeManifestMetadata(outputDir, tablesToHashes)
    }
  }


  def dsToInt(ds: String): Int = {
    ds.replace("-", "").toInt
  }

  def getTableSemanticHash(joinParts: List[api.JoinPart], join: api.Join): Int = {
    // Generates the semantic hash of an output table given the various joinParts that are downstream of it, and the join itself
    // first get a map of join_parts to relevant semantic fields
    val joinPartsSemantics = joinParts.map{ joinPart =>
      // For each joinPart, the only relevant sampling metadata for it's sources are keyMapping and keyColumn
      s"${Option(joinPart.prefix).getOrElse("")}${joinPart.groupBy.metaData.getName}" ->
        (joinPart.keyMapping.asScala, joinPart.groupBy.keyColumns.asScala, joinPart.groupBy.maxWindow).hashCode()
    }.toMap

    // The left side hash only depends on the source where clauses
    val leftSideSemantics = Map("left" -> join.left.query.wheres.asScala)

    // Add them together along with job semantics, return the hashcode
    val semantics: Map[String, Any] = joinPartsSemantics ++ leftSideSemantics ++ jobSemanticsMetadata
    semantics.hashCode()
  }

  def getTablesWithSemanticDiff(tableHashes: Map[String, Int]): Seq[String] = {
    val prior = getPriorRunManifestMetadata(outputDir)
    tableHashes.filter {
      case (key, value) => prior.get(key) match {
        case Some(v) => v != value // The semantic hash has changed
        case None => true          // The entity did not exist in the old metadata
      }
    }.keys.toSeq
  }

  def sampleJoin(join: api.Join): Unit = {

    // Create a map of table -> List[joinPart]
    // So that we can generate semantic hashing per table, and construct one query per source table
    val tablesMap: Map[String, List[api.JoinPart]] = join.joinParts.asScala.flatMap { joinPart =>
      joinPart.groupBy.sources.asScala.map { source =>
        (source.rootTable, joinPart)
      }
    }.groupBy(_._1).mapValues(_.map(_._2).toList)

    val tableHashes: Map[String, Int] = tablesMap.map{ case(table, joinParts) =>
      (table, getTableSemanticHash(joinParts, join))
    } ++ Map(join.getLeft.rootTable -> Option(join.left.query.wheres.asScala).getOrElse("").hashCode())

    val tablesToSample: Seq[String] = if (forceResample) {
      tableHashes.keys.toSeq
    } else {
      getTablesWithSemanticDiff(tableHashes)
    }

    if (tablesToSample.isEmpty) {
      logger.error(s"No entities need resampling based off of the manifest at $outputDir/$MANIFEST_FILE_NAME. " +
        s"Exiting without doing anything (this job should have been skipped, possible issue in run.py).")
    } else {
      logger.info(s"Sampling for the following tables: $tablesToSample")
      val queryRange = PartitionRange(startDate, endDate)(tableUtils)
      // First sample the left side
      val leftRoot = join.getLeft.rootTable
      val sampledLeftDf = sampleSource(leftRoot, queryRange, true)

      val filteredTablesMap = tablesMap.filter { case (key, _) =>
        tablesToSample.contains(key) }

      filteredTablesMap.map { case (table, joinParts) =>
        val startDateAndKeyFilters = joinParts.map { joinPart =>
          val groupBy = joinPart.groupBy

          // Get the key cols
          val keys: Seq[String] = if (joinPart.keyMapping != null) {
            joinPart.keyMapping.asScala.keys.toSeq
          } else {
            groupBy.getKeyColumns.asScala
          }

          // Construct the specific key filter for each GroupBy using the sampledLeftDf
          val keyFilters: Map[String, Array[Any]] = createKeyFilters(keys, sampledLeftDf)

          val start = QueryRangeHelper.earliestDate(join.left.dataModel, groupBy, tableUtils, queryRange)
          (start, keyFilters)
        }
        // get the earliest start date
        // If start date is missing (unwindowed events case), just use a 0 hack for query rendering
        // This allows us to use PartitionRange rather than an (Option[String], string)
        val sourceStartDate = startDateAndKeyFilters.minBy(_._1)._1.getOrElse("0000-00-00")

        val keyFilters = startDateAndKeyFilters.map(_._2)

        sampleSource(table, PartitionRange(sourceStartDate, endDate)(tableUtils), false, Option(keyFilters))
      }
    }
    writeManifestMetadata(outputDir, tableHashes)
  }

  def run(): Unit = {
    // Create outputDir if it doesn't exist
    val directory = new File(outputDir)
    if (!directory.exists()) {
      directory.mkdirs()
    }

    conf match {
      case confPath: String =>
        if (confPath.contains("/joins/")) {
          val joinConf = parseConf[api.Join](confPath)
          sampleJoin(joinConf)
        } else if (confPath.contains("/group_bys/")) {
          val groupByConf = parseConf[api.GroupBy](confPath)
          sampleGroupBy(groupByConf)
        }
      case groupByConf: api.GroupBy => sampleGroupBy(groupByConf)
      case joinConf: api.Join       => sampleJoin(joinConf)
    }
  }
}
