package ai.chronon.spark

import ai.chronon.api.Constants
import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.spark.JoinUtils.logger
import com.google.gson.Gson

import scala.util.ScalaJavaConversions.MapOps

object SemanticHashUtils {

  // Finds all join output tables (join parts and final table) that need recomputing (in monolithic spark job mode)
  def tablesToRecompute(joinConf: ai.chronon.api.Join,
                        outputTable: String,
                        tableUtils: TableUtils): collection.Seq[String] =
    computeDiff(joinConf, outputTable, tableUtils, tableHashesChanged, Seq.empty)

  // Determines if the saved left table of the join (includes bootstrap) needs to be recomputed due to semantic changes since last run
  def shouldRecomputeLeft(joinConf: ai.chronon.api.Join, outputTable: String, tableUtils: TableUtils): Boolean =
    computeDiff(joinConf, outputTable, tableUtils, isLeftHashChanged, false)

  private def computeDiff[T](joinConf: ai.chronon.api.Join,
                             outputTable: String,
                             tableUtils: TableUtils,
                             computeDiffFunc: (Map[String, String], Map[String, String], ai.chronon.api.Join) => T,
                             defaultFunc: => T): T = {
    val oldSemanticHashOpt = getOldSemanticHash(outputTable, tableUtils)
    if (oldSemanticHashOpt.isDefined) {
      val newSemanticHash = getNewSemanticHash(joinConf)
      logger.info(s"Comparing Hashes:\nNew: $newSemanticHash,\nOld: ${oldSemanticHashOpt.get}")
      computeDiffFunc(oldSemanticHashOpt.get, newSemanticHash, joinConf)
    } else {
      defaultFunc
    }
  }

  private def getOldSemanticHash(outputTable: String, tableUtils: TableUtils): Option[Map[String, String]] = {
    val gson = new Gson()
    val tablePropsOpt = tableUtils.getTableProperties(outputTable)
    val oldSemanticJsonOpt = tablePropsOpt.flatMap(_.get(Constants.SemanticHashKey))
    oldSemanticJsonOpt.map(json => gson.fromJson(json, classOf[java.util.HashMap[String, String]]).toScala)
  }

  private def getNewSemanticHash(joinConf: ai.chronon.api.Join): Map[String, String] = {
    joinConf.semanticHash
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
      Some(join.metaData.outputTable)
    } else None
    partsToDrop ++ mainTable
  }
}
