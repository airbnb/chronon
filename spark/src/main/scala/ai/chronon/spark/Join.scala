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

package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.DataModel.Entities
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils._
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.util.sketch.BloomFilter

import java.util
import java.util.concurrent.Executors
import scala.collection.compat._
import scala.collection.{Seq, mutable}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.jdk.CollectionConverters._
import scala.util.ScalaJavaConversions.{ListOps, MapOps}
import scala.util.{Failure, Success, Try}

/*
 * hashes: a list containing bootstrap hashes that represent the list of bootstrap parts that a record has matched
 *         during the bootstrap join
 * rowCount: number of records with this particular combination of hashes. for logging purpose only
 * isCovering: whether this combination of hashes fully covers the required fields of a join_part. the join_part
 *             reference itself is omitted here. but essentially each CoveringSet is pertinent to a specific join_part
 */
case class CoveringSet(hashes: Seq[String], rowCount: Long, isCovering: Boolean)

object CoveringSet {
  def toFilterExpression(coveringSets: Seq[CoveringSet]): String = {
    val coveringSetHashExpression = "(" +
      coveringSets
        .map { coveringSet =>
          val hashes = coveringSet.hashes.map("'" + _.trim + "'").mkString(", ")
          s"array($hashes)"
        }
        .mkString(", ") +
      ")"

    s"( ${Constants.MatchedHashes} IS NULL ) OR ( ${Constants.MatchedHashes} NOT IN $coveringSetHashExpression )"
  }
}

class Join(joinConf: api.Join,
           endPartition: String,
           tableUtils: TableUtils,
           skipFirstHole: Boolean = true,
           showDf: Boolean = false,
           selectedJoinParts: Option[List[String]] = None,
           unsetSemanticHash: Boolean = false)
    extends JoinBase(joinConf, endPartition, tableUtils, skipFirstHole, showDf, selectedJoinParts, unsetSemanticHash) {

  private def padFields(df: DataFrame, structType: sql.types.StructType): DataFrame = {
    structType.foldLeft(df) {
      case (df, field) =>
        if (df.columns.contains(field.name)) {
          df
        } else {
          df.withColumn(field.name, lit(null).cast(field.dataType))
        }
    }
  }

  private def toSparkSchema(fields: Seq[StructField]): sql.types.StructType =
    SparkConversions.fromChrononSchema(StructType("", fields.toArray))

  /*
   * For all external fields that are not already populated during the bootstrap step, fill in NULL.
   * This is so that if any derivations depend on the these external fields, they will still pass and not complain
   * about missing columns. This is necessary when we directly bootstrap a derived column and skip the base columns.
   */
  private def padExternalFields(bootstrapDf: DataFrame, bootstrapInfo: BootstrapInfo): DataFrame = {

    val nonContextualFields = toSparkSchema(
      bootstrapInfo.externalParts
        .filter(!_.externalPart.isContextual)
        .flatMap(part => part.keySchema ++ part.valueSchema))
    val contextualFields = toSparkSchema(
      bootstrapInfo.externalParts.filter(_.externalPart.isContextual).flatMap(_.keySchema))

    def withNonContextualFields(df: DataFrame): DataFrame = padFields(df, nonContextualFields)

    // Ensure keys and values for contextual fields are consistent even if only one of them is explicitly bootstrapped
    def withContextualFields(df: DataFrame): DataFrame =
      contextualFields.foldLeft(df) {
        case (df, field) => {
          var newDf = df
          if (!newDf.columns.contains(field.name)) {
            newDf = newDf.withColumn(field.name, lit(null).cast(field.dataType))
          }
          val prefixedName = s"${Constants.ContextualPrefix}_${field.name}"
          if (!newDf.columns.contains(prefixedName)) {
            newDf = newDf.withColumn(prefixedName, lit(null).cast(field.dataType))
          }
          newDf
            .withColumn(field.name, coalesce(col(field.name), col(prefixedName)))
            .withColumn(prefixedName, coalesce(col(field.name), col(prefixedName)))
        }
      }

    withContextualFields(withNonContextualFields(bootstrapDf))
  }

  /*
   * For all external fields that are not already populated during the group by backfill step, fill in NULL.
   * This is so that if any derivations depend on the these group by fields, they will still pass and not complain
   * about missing columns. This is necessary when we directly bootstrap a derived column and skip the base columns.
   */
  private def padGroupByFields(baseJoinDf: DataFrame, bootstrapInfo: BootstrapInfo): DataFrame = {
    val groupByFields = toSparkSchema(bootstrapInfo.joinParts.flatMap(_.valueSchema))
    padFields(baseJoinDf, groupByFields)
  }

  private def findBootstrapSetCoverings(bootstrapDf: DataFrame,
                                        bootstrapInfo: BootstrapInfo,
                                        leftRange: PartitionRange): Seq[(JoinPartMetadata, Seq[CoveringSet])] = {

    val distinctBootstrapSets: Seq[(Seq[String], Long)] =
      if (!bootstrapDf.columns.contains(Constants.MatchedHashes)) {
        Seq()
      } else {
        val collected = bootstrapDf
          .groupBy(Constants.MatchedHashes)
          .agg(count(lit(1)).as("row_count"))
          .collect()

        collected.map { row =>
          val hashes = if (row.isNullAt(0)) {
            Seq()
          } else {
            row.getAs[mutable.WrappedArray[String]](0).toSeq
          }
          (hashes, row.getAs[Long](1))
        }.toSeq
      }

    val distinctBootstrapSetsWithSchema: Seq[(Seq[String], Long, Set[StructField])] =
      distinctBootstrapSets.map {
        case (hashes, rowCount) =>
          if (hashes.exists(hash => !bootstrapInfo.hashToSchema.contains(hash))) {
            logger.error(s"""Bootstrap table contains out-of-date metadata and should be cleaned up.
                 |This is most likely caused by bootstrap table not properly archived during schema evolution. The semantic_hash may have been manually cleared to skip this.
                 |Please manually archive the old bootstrap table and re-run.
                 |
                 |ALTER TABLE ${bootstrapTable} RENAME TO ${bootstrapTable}_<timestamp_suffix>;
                 |""".stripMargin)
            throw new IllegalStateException("Bootstrap table contains out-of-date metadata and should be cleaned up.")
          }
          val schema = hashes.toSet.flatMap(bootstrapInfo.hashToSchema.apply)
          (hashes, rowCount, schema)
      }

    val partsToCompute: Seq[JoinPartMetadata] = {
      if (selectedJoinParts.isEmpty) {
        bootstrapInfo.joinParts
      } else {
        bootstrapInfo.joinParts.filter(part => selectedJoinParts.get.contains(part.joinPart.fullPrefix))
      }
    }

    if (selectedJoinParts.isDefined && partsToCompute.isEmpty) {
      throw new IllegalArgumentException(
        s"Selected join parts are not found. Available ones are: ${bootstrapInfo.joinParts.map(_.joinPart.fullPrefix).prettyInline}")
    }

    val coveringSetsPerJoinPart: Seq[(JoinPartMetadata, Seq[CoveringSet])] = bootstrapInfo.joinParts
      .filter(part => selectedJoinParts.isEmpty || partsToCompute.contains(part))
      .map { joinPartMetadata =>
        val coveringSets = distinctBootstrapSetsWithSchema.map {
          case (hashes, rowCount, schema) =>
            val isCovering = joinPartMetadata.derivationDependencies
              .map {
                case (derivedField, baseFields) =>
                  schema.contains(derivedField) || baseFields.forall(schema.contains)
              }
              .forall(identity)

            CoveringSet(hashes, rowCount, isCovering)
        }
        (joinPartMetadata, coveringSets)
      }

    logger.info(
      s"\n======= CoveringSet for Join ${joinConf.metaData.name} for PartitionRange(${leftRange.start}, ${leftRange.end}) =======\n")
    coveringSetsPerJoinPart.foreach {
      case (joinPartMetadata, coveringSets) =>
        logger.info(s"Bootstrap sets for join part ${joinPartMetadata.joinPart.groupBy.metaData.name}")
        coveringSets.foreach { coveringSet =>
          logger.info(
            s"CoveringSet(hash=${coveringSet.hashes.prettyInline}, rowCount=${coveringSet.rowCount}, isCovering=${coveringSet.isCovering})")
        }
    }

    coveringSetsPerJoinPart
  }

  private def getRightPartsData(leftRange: PartitionRange): Seq[(JoinPart, DataFrame)] = {
    joinConf.joinParts.asScala.flatMap { joinPart =>
      val partTable = joinConf.partOutputTable(joinPart)
      if (!tableUtils.tableExists(partTable)) {
        // When a JoinPart is fully bootstrapped, its partTable may not exist and we skip it during final join.
        logger.warn(s"Table $partTable does not exist, possibly due to full bootstrap covering, skipping it.")
        None
      } else {
        val effectiveRange =
          if (joinConf.left.dataModel != Entities && joinPart.groupBy.inferredAccuracy == Accuracy.SNAPSHOT) {
            leftRange.shift(-1)
          } else {
            leftRange
          }
        val wheres = Seq(s"ds >= '${effectiveRange.start}'", s"ds <= '${effectiveRange.end}'")
        val sql = QueryUtils.build(null, partTable, wheres)
        logger.info(s"Pulling data from joinPart table with: $sql")
        val df = tableUtils.sparkSession.sql(sql)
        Some((joinPart, df))
      }
    }
  }

  override def computeFinalJoin(leftDf: DataFrame, leftRange: PartitionRange, bootstrapInfo: BootstrapInfo): Unit = {
    val bootstrapDf = leftRange.scanQueryDf(query = null, table = bootstrapTable).addTimebasedColIfExists()
    val rightPartsData = getRightPartsData(leftRange)
    val joinedDfTry =
      try {
        Success(
          rightPartsData
            .foldLeft(bootstrapDf) {
              case (partialDf, (rightPart, rightDf)) => joinWithLeft(partialDf, rightDf, rightPart)
            }
            // drop all processing metadata columns
            .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Failure(e)
      }
    val df = processJoinedDf(joinedDfTry, leftDf, bootstrapInfo, bootstrapDf)
    df.save(outputTable, tableProps, autoExpand = true)
  }

  override def computeRange(leftDf: DataFrame,
                            leftRange: PartitionRange,
                            bootstrapInfo: BootstrapInfo,
                            usingBootstrappedLeft: Boolean = false): Option[DataFrame] = {

    val leftTaggedDf = leftDf.addTimebasedColIfExists()

    // compute bootstrap table - a left outer join between left source and various bootstrap source table
    // this becomes the "new" left for the following GB backfills
    val bootstrapDf = if (usingBootstrappedLeft) {
      leftTaggedDf
    } else {
      computeBootstrapTable(leftTaggedDf, leftRange, bootstrapInfo)
    }

    val bootStrapWithStats = bootstrapDf.withStats

    // for each join part, find the bootstrap sets that can fully "cover" the required fields. Later we will use this
    // info to filter records that need backfills vs can be waived from backfills
    val bootstrapCoveringSets = findBootstrapSetCoverings(bootstrapDf, bootstrapInfo, leftRange)

    val leftTimeRangeOpt = if (leftTaggedDf.schema.fieldNames.contains(Constants.TimePartitionColumn)) {
      val leftTimePartitionMinMax = leftTaggedDf.range[String](Constants.TimePartitionColumn)
      Some(PartitionRange(leftTimePartitionMinMax._1, leftTimePartitionMinMax._2)(tableUtils))
    } else {
      None
    }

    implicit val executionContext: ExecutionContextExecutorService =
      ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(tableUtils.joinPartParallelism))

    val joinedDfTry = tableUtils
      .wrapWithCache("Computing left parts for bootstrap table", bootstrapDf) {
        // parallelize the computation of each of the parts

        Thread.currentThread().setName(s"Join-${leftRange.start}-${leftRange.end}")
        // compute join parts (GB) backfills
        // for each GB, we first find out the unfilled subset of bootstrap table which still requires the backfill.
        // we do this by utilizing the per-record metadata computed during the bootstrap process.
        // then for each GB, we compute a join_part table that contains aggregated feature values for the required key space
        // the required key space is a slight superset of key space of the left, due to the nature of using bloom-filter.

        def genKeyFilter(joinPart: JoinPart, unfilledLeftDf: Option[DfWithStats]) = {
          val leftRowCount: Int = unfilledLeftDf.map(_.count.toInt).getOrElse(0)
          if (tableUtils.smallModelEnabled && leftRowCount > 0 && leftRowCount <= tableUtils.smallModeNumRowsCutoff) {
            logger.info(s"Counted $leftRowCount rows, running join in small mode.")
            // If left DF is small, hardcode the key filter into the joinPart's GroupBy's where clause.
            val filteredJoinPart = injectKeyFilter(unfilledLeftDf.map(_.df).get, joinPart)
            (true, None, filteredJoinPart)
          } else {
            if (leftRowCount > 0 && leftRowCount <= tableUtils.bloomFilterThreshold) {
              logger.info(s"Counted $leftRowCount rows, running join in bloom filter mode.")
              // Generate a Bloom filter for 'joinPart' when the row count to be backfilled falls below a specified threshold.
              // This method anticipates that there will likely be a substantial number of rows on the right side that need to be filtered out.
              val leftBlooms = joinConf.leftKeyCols.iterator.map { key =>
                key -> unfilledLeftDf
                  .map(_.df.generateBloomFilter(key, leftRowCount, joinConf.left.table, leftRange))
                  .getOrElse(null)
              }.toJMap

              val rightBlooms = joinPart.rightToLeft.iterator.map {
                case (rightCol, leftCol) =>
                  rightCol -> leftBlooms.get(leftCol)
              }.toJMap

              val bloomSizes = rightBlooms.asScala.map {
                case (rightCol, bloom) =>
                  s"$rightCol -> ${bloom.bitSize()}"
              }
              logger.info(s"Bloom sizes: ${bloomSizes.mkString(", ")}")

              (false, Some(rightBlooms), joinPart)
            } else {
              logger.info(s"Counted $leftRowCount rows, running join without any key filter.")
              (false, None, joinPart)
            }
          }
        }

        try {
          val rightResultsFuture = bootstrapCoveringSets.map {
            case (partMetadata, coveringSets) =>
              Future {
                val joinPart = partMetadata.joinPart
                val threadName = s"${joinPart.groupBy.metaData.cleanName}-${leftRange.start}-${leftRange.end}"
                tableUtils.sparkSession.sparkContext
                  .setLocalProperty("spark.scheduler.pool", s"${joinPart.groupBy.metaData.cleanName}-part-pool")
                val unfilledLeftDf = findUnfilledRecords(bootStrapWithStats, coveringSets.filter(_.isCovering))
                Thread.currentThread().setName(s"active-$threadName")

                // if the join part contains ChrononRunDs macro, then we need to make sure the join is for a single day
                val selects = Option(joinPart.groupBy.sources.toScala.map(_.query.selects).map(_.toScala))
                if (
                  selects.isDefined && selects.get.nonEmpty && selects.get.exists(selectsMap =>
                    Option(selectsMap).isDefined && selectsMap.values.exists(_.contains(Constants.ChrononRunDs)))
                ) {
                  assert(
                    leftRange.isSingleDay,
                    s"Macro ${Constants.ChrononRunDs} is only supported for single day join, current range is ${leftRange}")
                }

                val (runSmallMode: Boolean,
                     bloomFilterOpt: Option[util.Map[String, BloomFilter]],
                     filteredJoinPart: api.JoinPart) =
                  genKeyFilter(joinPart, unfilledLeftDf)

                val df =
                  computeRightTable(unfilledLeftDf,
                                    filteredJoinPart,
                                    leftRange,
                                    leftTimeRangeOpt,
                                    bloomFilterOpt,
                                    runSmallMode)
                    .map(df => filteredJoinPart -> df)
                Thread.currentThread().setName(s"done-$threadName")
                df
              }
          }
          val rightResults = Await.result(Future.sequence(rightResultsFuture), Duration.Inf).flatten

          // early exit if selectedJoinParts is defined. Otherwise, we combine all join parts
          if (selectedJoinParts.isDefined) return None

          // combine bootstrap table and join part tables
          // sequentially join bootstrap table and each join part table. some column may exist both on left and right because
          // a bootstrap source can cover a partial date range. we combine the columns using coalesce-rule
          Success(
            rightResults
              .foldLeft(bootstrapDf.addTimebasedColIfExists()) {
                case (partialDf, (rightPart, rightDf)) => joinWithLeft(partialDf, rightDf, rightPart)
              }
              // drop all processing metadata columns
              .drop(Constants.MatchedHashes, Constants.TimePartitionColumn))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            Failure(e)
        } finally {
          executionContext.shutdownNow()
        }
      }
      .get

    Some(processJoinedDf(joinedDfTry, leftTaggedDf, bootstrapInfo, bootstrapDf))
  }

  private def processJoinedDf(joinedDfTry: Try[DataFrame],
                              leftDf: DataFrame,
                              bootstrapInfo: BootstrapInfo,
                              bootstrapDf: DataFrame): DataFrame = {
    if (joinedDfTry.isFailure) throw joinedDfTry.failed.get
    val joinedDf = joinedDfTry.get
    val outputColumns = joinedDf.columns.filter(bootstrapInfo.fieldNames ++ bootstrapDf.columns)
    val finalBaseDf = padGroupByFields(joinedDf.selectExpr(outputColumns.map(c => s"`$c`"): _*), bootstrapInfo)
    val finalDf = cleanUpContextualFields(applyDerivation(finalBaseDf, bootstrapInfo, leftDf.columns),
                                          bootstrapInfo,
                                          leftDf.columns)
    finalDf.explain()
    finalDf
  }

  def applyDerivation(baseDf: DataFrame, bootstrapInfo: BootstrapInfo, leftColumns: Seq[String]): DataFrame = {
    if (!joinConf.isSetDerivations || joinConf.derivations.isEmpty) {
      return baseDf
    }

    val projections = joinConf.derivations.toScala.derivationProjection(bootstrapInfo.baseValueNames)
    val projectionsMap = projections.toMap
    val baseOutputColumns = baseDf.columns.toSet

    val finalOutputColumns =
      /*
       * Loop through all columns in the base join output:
       * 1. If it is one of the value columns, then skip it here and it will be handled later as we loop through
       *    derived columns again - derivation is a projection from all value columns to desired derived columns
       * 2.  (see case 2 below) If it is matching one of the projected output columns, then there are 2 sub-cases
       *     a. matching with a left column, then we handle the coalesce here to make sure left columns show on top
       *     b. a bootstrapped derivation case, the skip it here and it will be handled later as
       *        loop through derivations to perform coalescing
       * 3. Else, we keep it in the final output - cases falling here are either (1) key columns, or (2)
       *    arbitrary columns selected from left.
       */
      baseDf.columns.flatMap { c =>
        if (bootstrapInfo.baseValueNames.contains(c)) {
          None
        } else if (projectionsMap.contains(c)) {
          if (leftColumns.contains(c)) {
            Some(coalesce(col(c), expr(projectionsMap(c))).as(c))
          } else {
            None
          }
        } else {
          Some(col(c))
        }
      } ++
        /*
         * Loop through all clauses in derivation projections:
         * 1. (see case 2 above) If it is matching one of the projected output columns, then there are 2 sub-cases
         *     a. matching with a left column, then we skip since it is handled above
         *     b. a bootstrapped derivation case (see case 2 below), then we do the coalescing to achieve the bootstrap
         *        behavior.
         * 2. Else, we do the standard projection.
         */
        projections
          .flatMap {
            case (name, expression) =>
              if (baseOutputColumns.contains(name)) {
                if (leftColumns.contains(name)) {
                  None
                } else {
                  Some(coalesce(col(name), expr(expression)).as(name))
                }
              } else {
                Some(expr(expression).as(name))
              }
          }

    val result = baseDf.select(finalOutputColumns: _*)
    if (showDf) {
      logger.info(s"printing results for join: ${joinConf.metaData.name}")
      result.prettyPrint()
    }
    result
  }

  /*
   * Remove extra contextual keys unless it is a result of derivations or it is a column from left
   */
  def cleanUpContextualFields(finalDf: DataFrame, bootstrapInfo: BootstrapInfo, leftColumns: Seq[String]): DataFrame = {

    val contextualNames =
      bootstrapInfo.externalParts.filter(_.externalPart.isContextual).flatMap(_.keySchema).map(_.name)
    val projections = if (joinConf.isSetDerivations) {
      joinConf.derivations.toScala.derivationProjection(bootstrapInfo.baseValueNames).map(_._1)
    } else {
      Seq()
    }
    contextualNames.foldLeft(finalDf) {
      case (df, name) => {
        if (leftColumns.contains(name) || projections.contains(name)) {
          df
        } else {
          df.drop(name)
        }
      }
    }
  }

  /*
   * The purpose of Bootstrap is to leverage input tables which contain pre-computed values, such that we can
   * skip the computation for these record during the join-part computation step.
   *
   * The main goal here to join together the various bootstrap source to the left table, and in the process maintain
   * relevant metadata such that we can easily tell which record needs computation or not in the following step.
   */
  override def computeBootstrapTable(leftDf: DataFrame,
                                     range: PartitionRange,
                                     bootstrapInfo: BootstrapInfo): DataFrame = {

    def validateReservedColumns(df: DataFrame, table: String, columns: Seq[String]): Unit = {
      val reservedColumnsContained = columns.filter(df.schema.fieldNames.contains)
      assert(
        reservedColumnsContained.isEmpty,
        s"Table $table contains columns ${reservedColumnsContained.prettyInline} which are reserved by Chronon."
      )
    }

    val startMillis = System.currentTimeMillis()

    // verify left table does not have reserved columns
    validateReservedColumns(leftDf, joinConf.left.table, Seq(Constants.BootstrapHash, Constants.MatchedHashes))

    tableUtils
      .unfilledRanges(bootstrapTable, range, skipFirstHole = skipFirstHole)
      .getOrElse(Seq())
      .foreach(unfilledRange => {
        val parts = Option(joinConf.bootstrapParts)
          .map(_.toScala)
          .getOrElse(Seq())

        val initDf = leftDf
          .prunePartition(unfilledRange)
          // initialize an empty matched_hashes column for the purpose of later processing
          .withColumn(Constants.MatchedHashes, typedLit[Array[String]](null))

        val joinedDf = parts.foldLeft(initDf) {
          case (partialDf, part) => {

            logger.info(s"\nProcessing Bootstrap from table ${part.table} for range ${unfilledRange}")

            val bootstrapRange = if (part.isSetQuery) {
              unfilledRange.intersect(PartitionRange(part.startPartition, part.endPartition)(tableUtils))
            } else {
              unfilledRange
            }
            if (!bootstrapRange.valid) {
              logger.info(s"partition range of bootstrap table ${part.table} is beyond unfilled range")
              partialDf
            } else {
              val partitionColumn = tableUtils.getPartitionColumn(part.query)
              var bootstrapDf = bootstrapRange.scanQueryDf(part.query,
                                                           part.table,
                                                           fillIfAbsent = Map(partitionColumn -> null),
                                                           partitionColOpt = Some(partitionColumn),
                                                           renamePartitionCol = true)
              // attach semantic_hash for either log or regular table bootstrap
              validateReservedColumns(bootstrapDf, part.table, Seq(Constants.BootstrapHash, Constants.MatchedHashes))
              if (bootstrapDf.columns.contains(Constants.SchemaHash)) {
                bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, col(Constants.SchemaHash))
              } else {
                bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, lit(part.semanticHash))
              }

              // include only necessary columns. in particular,
              // this excludes columns that are NOT part of Join's output (either from GB or external source)
              val includedColumns = bootstrapDf.columns
                .filter(bootstrapInfo.fieldNames ++ part.keys(joinConf, tableUtils.partitionColumn)
                  ++ Seq(Constants.BootstrapHash, tableUtils.partitionColumn))
                .sorted

              bootstrapDf = bootstrapDf
                .select(includedColumns.map(col): _*)
                // TODO: allow customization of deduplication logic
                .dropDuplicates(part.keys(joinConf, tableUtils.partitionColumn).toArray)

              coalescedJoin(partialDf, bootstrapDf, part.keys(joinConf, tableUtils.partitionColumn).toSeq)
              // as part of the left outer join process, we update and maintain matched_hashes for each record
              // that summarizes whether there is a join-match for each bootstrap source.
              // later on we use this information to decide whether we still need to re-run the backfill logic
                .withColumn(Constants.MatchedHashes,
                            set_add(col(Constants.MatchedHashes), col(Constants.BootstrapHash)))
                .drop(Constants.BootstrapHash)
            }
          }
        }

        // include all external fields if not already bootstrapped
        val enrichedDf = padExternalFields(joinedDf, bootstrapInfo)

        // set autoExpand = true since log table could be a bootstrap part
        enrichedDf.save(bootstrapTable, tableProps, autoExpand = true)
      })

    val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
    logger.info(s"Finished computing bootstrap table ${joinConf.metaData.bootstrapTable} in ${elapsedMins} minutes")

    range.scanQueryDf(query = null, table = bootstrapTable)
  }

  /*
   * We leverage metadata information created from the bootstrap step to tell which record was already joined to a
   * bootstrap source, and therefore had certain columns pre-populated. for these records and these columns, we do not
   * need to run backfill again. this is possible because the hashes in the metadata columns can be mapped back to
   * full schema information.
   */
  private def findUnfilledRecords(bootstrapDfWithStats: DfWithStats,
                                  coveringSets: Seq[CoveringSet]): Option[DfWithStats] = {
    val bootstrapDf = bootstrapDfWithStats.df
    if (coveringSets.isEmpty || !bootstrapDf.columns.contains(Constants.MatchedHashes)) {
      // this happens whether bootstrapParts is NULL for the JOIN and thus no metadata columns were created
      return Some(bootstrapDfWithStats)
    }
    val filterExpr = CoveringSet.toFilterExpression(coveringSets)
    logger.info(s"Using covering set filter: $filterExpr")
    val filteredDf = bootstrapDf.where(filterExpr)
    val filteredCount = filteredDf.count()
    if (bootstrapDfWithStats.count == filteredCount) { // counting is faster than computing stats
      Some(bootstrapDfWithStats)
    } else if (filteredCount == 0) {
      None
    } else {
      Some(DfWithStats(filteredDf)(bootstrapDfWithStats.tableUtils))
    }
  }
}
