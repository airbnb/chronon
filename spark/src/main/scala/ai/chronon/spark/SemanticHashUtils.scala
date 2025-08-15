package ai.chronon.spark

import ai.chronon.api.Constants
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.spark.JoinUtils.logger
import com.google.gson.Gson

import scala.util.ScalaJavaConversions.MapOps

/*
 * Metadata stored in Hive table properties to track semantic hashes and flags.
 * The flags are used to indicate the versioned logic that was used to compute the semantic hash
 */
case class SemanticHashHiveMetadata(semanticHash: Map[String, String], excludeTopic: Boolean)

case class SemanticHashException(message: String) extends Exception(message)

/*
 * Utilities to handle semantic_hash computation, comparison, migration and table_archiving.
 */
object SemanticHashUtils {

  // Finds all join output tables (join parts and final table) that need recomputing (in monolithic spark job mode)
  def tablesToRecompute(joinConf: ai.chronon.api.Join,
                        outputTable: String,
                        tableUtils: TableUtils,
                        unsetSemanticHash: Boolean): (collection.Seq[String], Boolean) =
    computeDiff(joinConf, outputTable, tableUtils, unsetSemanticHash, tableHashesChanged, Seq.empty)

  // Determines if the saved left table of the join (includes bootstrap) needs to be recomputed due to semantic changes since last run
  def shouldRecomputeLeft(joinConf: ai.chronon.api.Join,
                          outputTable: String,
                          tableUtils: TableUtils,
                          unsetSemanticHash: Boolean): (Boolean, Boolean) = {
    computeDiff(joinConf, outputTable, tableUtils, unsetSemanticHash, isLeftHashChanged, false)
  }

  /*
   * When semantic_hash versions are different, a diff does not automatically mean a semantic change.
   * In those scenarios, we print out the diff tables and differ to users about dropping.
   */
  private def canAutoArchive(semanticHashHiveMetadata: SemanticHashHiveMetadata): Boolean = {
    semanticHashHiveMetadata.excludeTopic
  }

  private def computeDiff[T](joinConf: ai.chronon.api.Join,
                             outputTable: String,
                             tableUtils: TableUtils,
                             unsetSemanticHash: Boolean,
                             computeDiffFunc: (Map[String, String], Map[String, String], ai.chronon.api.Join) => T,
                             emptyFunc: => T): (T, Boolean) = {
    val semanticHashHiveMetadata = if (unsetSemanticHash) {
      None
    } else {
      getSemanticHashFromHive(outputTable, tableUtils)
    }
    def prettyPrintMap(map: Map[String, String]): String = {
      map.toSeq.sorted.map { case (key, value) => s"- $key: $value" }.mkString("\n")
    }
    if (semanticHashHiveMetadata.isDefined) {
      val oldSemanticHash = semanticHashHiveMetadata.get.semanticHash
      val newSemanticHash = joinConf.semanticHash(excludeTopic = semanticHashHiveMetadata.get.excludeTopic)
      logger.info(
        s"""Comparing Hashes:
           |Hive Flag:
           |${Constants.SemanticHashExcludeTopic}: ${semanticHashHiveMetadata.get.excludeTopic}
           |Old Hashes:
           |${prettyPrintMap(oldSemanticHash)}
           |New Hashes:
           |${prettyPrintMap(newSemanticHash)}
           |""".stripMargin
      )
      val diff = computeDiffFunc(oldSemanticHash, newSemanticHash, joinConf)
      val autoArchive = canAutoArchive(semanticHashHiveMetadata.get)
      return (diff, autoArchive)
    }
    if (unsetSemanticHash) {
      logger.info(s"Semantic hash has been unset for table ${outputTable}. Proceed to computation and table creation.")
    } else {
      logger.info(s"No semantic hash found in table ${outputTable}. Proceed to computation and table creation.")
    }
    logger.info(s"""New Hashes:
         |${prettyPrintMap(joinConf.semanticHash(excludeTopic = true))}
         |""".stripMargin)
    (emptyFunc, true)
  }

  private def getSemanticHashFromHive(outputTable: String, tableUtils: TableUtils): Option[SemanticHashHiveMetadata] = {
    val gson = new Gson()
    if (!tableUtils.tableExists(outputTable)) {
      logger.info(s"Getting semantic hash from table $outputTable which does not exist. Proceed to computation.")
    }
    val tablePropsOpt = tableUtils.getTableProperties(outputTable)
    val oldSemanticJsonOpt = tablePropsOpt.flatMap(_.get(Constants.SemanticHashKey))
    val oldSemanticHash =
      oldSemanticJsonOpt.map(json => gson.fromJson(json, classOf[java.util.HashMap[String, String]]).toScala)

    val oldSemanticHashOptions = tablePropsOpt
      .flatMap(_.get(Constants.SemanticHashOptionsKey))
      .map(m => gson.fromJson(m, classOf[java.util.HashMap[String, String]]).toScala)
      .getOrElse(Map.empty)
    val hasSemanticHashExcludeTopicFlag =
      oldSemanticHashOptions.get(Constants.SemanticHashExcludeTopic).contains("true")

    oldSemanticHash.map(hashes => SemanticHashHiveMetadata(hashes, hasSemanticHashExcludeTopicFlag))
  }

  private def isLeftHashChanged(oldSemanticHash: Map[String, String],
                                newSemanticHash: Map[String, String],
                                join: ai.chronon.api.Join): Boolean = {
    // Checks for semantic changes in left or bootstrap, because those are saved together
    val bootstrapExistsAndChanged = oldSemanticHash.contains(join.metaData.bootstrapTable) && oldSemanticHash.get(
      join.metaData.bootstrapTable) != newSemanticHash.get(join.metaData.bootstrapTable)
    logger.info(s"Bootstrap table changed: $bootstrapExistsAndChanged")
    oldSemanticHash.get(join.leftSourceKey) != newSemanticHash.get(join.leftSourceKey) || bootstrapExistsAndChanged
  }

  private[spark] def tableHashesChanged(oldSemanticHash: Map[String, String],
                                        newSemanticHash: Map[String, String],
                                        join: ai.chronon.api.Join): Seq[String] = {
    // only right join part hashes for convenience
    def partHashes(semanticHashMap: Map[String, String]): Map[String, String] = {
      semanticHashMap.filter { case (name, _) => name != join.leftSourceKey && name != join.derivedKey }
    }

    // drop everything if left source changes
    val partsToDrop = if (isLeftHashChanged(oldSemanticHash, newSemanticHash, join)) {
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
    val derivedChanges = oldSemanticHash.get(join.derivedKey) != newSemanticHash.get(join.derivedKey)
    // TODO: make this incremental, retain the main table and continue joining, dropping etc
    val mainTable = if (partsToDrop.nonEmpty || added.nonEmpty || derivedChanges) {
      if (join.hasModelTransforms) {
        Some(join.metaData.preModelTransformsTable)
      } else {
        Some(join.metaData.outputTable)
      }
    } else None
    partsToDrop ++ mainTable
  }
}
