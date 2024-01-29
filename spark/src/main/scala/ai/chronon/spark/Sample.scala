package ai.chronon.spark

import java.io.{BufferedWriter, File, FileWriter}

import scala.compat.java8.FunctionConverters.enrichAsJavaFunction
import scala.io.Source
import scala.jdk.CollectionConverters.{asScalaBufferConverter, mapAsScalaMapConverter}

import ai.chronon.api
import ai.chronon.api.Extensions.{GroupByOps, SourceOps}
import ai.chronon.spark.Driver.{logger, parseConf}
import com.google.gson.{Gson, GsonBuilder}
import com.google.gson.reflect.TypeToken
import org.apache.spark.sql.DataFrame

class Sample(conf: Any,
             tableUtils: TableUtils,
             startDate: String,
             endDate: String,
             outputDir: String,
             forceResample: Boolean = false,
             numRows: Int = 100) {

  val MANIFEST_FILE_NAME = "manifest.json"
  val MANIFEST_SERDE_TYPE_TOKEN = new TypeToken[java.util.Map[String, Integer]](){}.getType

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

  /*
  For a given GroupBy, sample all the sources. Can optionally take a set of keys to use for sampling
  (used by the join sample path).
   */
  def sampleGroupBy(groupBy: api.GroupBy): Unit = {
    // Get a list of source table and relevant partition range for the ds being run
    val manifestMetadata = generateManifestMetadata(groupBy)
    val manifestDiff = getManifestDiff(manifestMetadata)
    logger.info(s"Running sampling for the following sources: ${manifestDiff.mkString(", ")}")
    val sourcesToSample = groupBy.sources.asScala.filter(source => manifestDiff.contains(source.rootTable))
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
    writeManifestMetadata(manifestMetadata)
  }

  def writeManifestMetadata(metadata: Map[String, Int]) = {
    val gson = new GsonBuilder().serializeNulls().setPrettyPrinting().create()
    // val javaMap: java.util.Map[String, Integer] = metadata.mapValues(_.asInstanceOf[Integer]).asJava

    // Convert your Scala Map[String, Int] to a Java map
    val javaMap: java.util.Map[String, Integer] = {
      val tempMap = new java.util.HashMap[String, Integer]()
      metadata.foreach { case (key, value) => tempMap.put(key, value.asInstanceOf[Integer]) }
      tempMap
    }

    val jsonString = gson.toJson(javaMap, MANIFEST_SERDE_TYPE_TOKEN)
    println("WRITING =======================")
    println(jsonString)
    val bw = new BufferedWriter(new FileWriter(s"$outputDir/$MANIFEST_FILE_NAME"))
    try {
      bw.write(jsonString)
    } finally {
      bw.close()
    }
  }

  def generateManifestMetadata(join: api.Join): Map[String, Int] = {
    // Computes a semantic hash of all the sampling-related semantics for the left side and the right_parts of a join
    // Iterate over joinParts and add an entry for each one with their semantic hash

    join.joinParts.asScala.map{ joinPart =>
      // For each joinPart, the only relevant sampling metadata for it's sources are keyMapping and keyColumn
      s"${Option(joinPart.prefix).getOrElse("")}${joinPart.groupBy.metaData.getName}" ->
        (joinPart.keyMapping.asScala, joinPart.groupBy.keyColumns.asScala).hashCode()
    }.toMap + ("left" -> // Add the left side hash, which only depends on the source where clauses and the numRows requested in the job
      (join.left.query.wheres.asScala, numRows).hashCode()) ++ getJobManifestMetadata()
  }

  def generateManifestMetadata(groupBy: api.GroupBy): Map[String, Int] = {
    // GroupBy Semantic hashing only depends on the where clause and the number
    groupBy.getSources.asScala.map { source =>
      source.rootTable -> source.query.getWheres.asScala.hashCode()
    }.toMap ++ getJobManifestMetadata()
  }

  def getJobManifestMetadata(): Map[String, Int] = {
    Map("numRows" -> numRows, "startDate" -> dsToInt(startDate), "endDate" -> dsToInt(endDate))
  }

  def dsToInt(ds: String): Int = {
    ds.replace("-", "").toInt
  }

  def getPriorRunManifestMetadata(): Map[String, Int] = {
    val gson = new Gson()
    try {
      val metadata = Source.fromFile(s"$outputDir/$MANIFEST_FILE_NAME").getLines.mkString
      val javaMap: java.util.Map[String, Integer]  = gson.fromJson(metadata, MANIFEST_SERDE_TYPE_TOKEN)
      // Convert to Scala
      javaMap.asScala.mapValues(_.intValue()).toMap
    } catch {
      case err: Throwable =>
        logger.error(s"Manifest Deserialization error:")
        err.printStackTrace()
        Map.empty[String, Int]
    }
  }

  def getManifestDiff(currentRunMetadata: Map[String, Int]): Seq[String] = {
    // Find which entities need resampling based on semantic hash
    val prior = getPriorRunManifestMetadata()
    println("CURRENT ========= \n\n ")
    println(currentRunMetadata)
    println("\n\n PRIOR =========== \n\n")
    println(prior)
    print(" =========== ")
    val numRowsDiff = currentRunMetadata("numRows") != prior.getOrElse("numRows", 0)
    val datesDiff = currentRunMetadata("startDate") < prior.getOrElse("startDate", Int.MaxValue) ||
      currentRunMetadata("endDate") > prior.getOrElse("endDate", Int.MinValue)
    val leftDiff = currentRunMetadata.getOrElse("left", 0) != prior.getOrElse("left", 0)
    if (numRowsDiff || datesDiff || leftDiff || forceResample) {
      // Under these conditions, resample everything
      currentRunMetadata.keys.toSeq
    } else {
      currentRunMetadata.filter {
        case (key, value) => prior.get(key) match {
          case Some(v) => v != value // The semantic hash has changed
          case None => true          // The entity did not exist in the old metadata
        }
      }.keys.toSeq
    }
  }

  def sampleJoin(join: api.Join) = {
    val manifestMetadata = generateManifestMetadata(join)
    val entitiesToSample = getManifestDiff(manifestMetadata)
    if (entitiesToSample.isEmpty) {
      logger.error(s"No entities need resampling based off of the manifest at $outputDir/$MANIFEST_FILE_NAME." +
        s"The intended behavior in this case is to never execute this job")
    } else {
      val queryRange = PartitionRange(startDate, endDate)(tableUtils)
      // First sample the left side
      val leftRoot = join.getLeft.rootTable
      val sampledLeftDf = sampleSource(leftRoot, queryRange, true)

      // Filter down to joinParts that require computation
      val joinPartsToRun = join.joinParts.asScala.filter{ joinPart =>
        entitiesToSample.contains(s"${Option(joinPart.prefix).getOrElse("")}${joinPart.groupBy.metaData.getName}")}

      // Create a map of table -> (earliestStartDate, List[keyFilters]) so that we can construct one query per source table
      joinPartsToRun.flatMap { joinPart =>
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
          println(s"INFERRED START $start")

          groupBy.sources.asScala.map { source =>
            (source.rootTable, start, keyFilters)
          }
        }.groupBy(_._1) // Group by the source table
        .map { case (key, tuple) =>
          val minStartDate = tuple.minBy(_._2)._2
          val keySetList = tuple.map(_._3) // Collect all the distinct keySets to sample
          (key, (minStartDate, keySetList))
        }.foreach{ case(table, tuple) =>
          val sourceEndDate = if (table == leftRoot) {
            // In this case, a GroupBy source table is the same as the left root table.
            // Because we're going to union this with the leftDf, we don't want overlapping dates to avoid duplicate rows
            // So we set the end date to a day before the earliest partition of the
            tableUtils.partitionSpec.shift(startDate, -1)
          } else {
            endDate
          }

        // If start date is missing (unwindowed events case), just use a 0 hack for query rendering (allows us
        // to use PartitionRange rather than an (Option[String], string)
        val sourceStartDate = tuple._1.getOrElse("0000-00-00")

          // it's possible that there's no longer any need to sample this source
          if (startDate <= endDate) {
            // We do not include the limit on the rightParts sampling, because that only applies to the left side
            val effectiveStart =
            sampleSource(table, PartitionRange(sourceStartDate, sourceEndDate)(tableUtils), false, Option(tuple._2))
          }
        }
    }
    writeManifestMetadata(manifestMetadata)
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
