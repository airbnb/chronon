package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.ScalaJavaConversions.{IterableOps, ListOps}

class Join(joinConf: api.Join, endPartition: String, tableUtils: TableUtils)
    extends BaseJoin(joinConf, endPartition, tableUtils) {

  private val bootstrapTable = joinConf.metaData.bootstrapTable

  override def computeRange(leftDf: DataFrame, leftRange: PartitionRange): DataFrame = {
    val leftTaggedDf = if (leftDf.schema.names.contains(Constants.TimeColumn)) {
      leftDf.withTimeBasedColumn(Constants.TimePartitionColumn)
    } else {
      leftDf
    }

    // compute bootstrap table - a left outer join between left source and various bootstrap source table
    // this becomes the "new" left for the following GB backfills
    val bootstrapInfo = BootstrapInfo.from(joinConf, leftRange, tableUtils)
    val bootstrapDf = computeBootstrapTable(leftTaggedDf, leftRange, bootstrapInfo)

    // compute join parts (GB) backfills
    // for each GB, we first find out the unfilled subset of bootstrap table which still requires the backfill.
    // we do this by utilizing the per-record metadata computed during the bootstrap process.
    // then for each GB, we compute a join_part table that contains aggregated feature values for the required key space
    // the required key space is a slight superset of key space of the left, due to the nature of using bloom-filter.
    val rightResults = bootstrapInfo.joinParts
      .map(_.joinPart)
      .parallel
      .flatMap { part =>
        val (unfilledLeftDf, validHashes) = findUnfilledRecords(bootstrapDf, part, bootstrapInfo)
        computeRightTable(unfilledLeftDf, part, leftRange, validHashes).map(df => part -> df)
      }

    // combine bootstrap table and join part tables
    // sequentially join bootstrap table and each join part table. some column may exist both on left and right because
    // a bootstrap source can cover a partial date range. we combine the columns using coalesce-rule
    val joinedDf = rightResults
      .foldLeft(bootstrapDf) {
        case (partialDf, (rightPart, rightDf)) => joinWithLeft(partialDf, rightDf, rightPart)
      }
      // drop all processing metadata columns
      .drop(Constants.MatchedHashes, Constants.TimePartitionColumn)

    val outputColumns = joinedDf.columns.filter(bootstrapInfo.fieldNames ++ bootstrapDf.columns)
    val finalDf = joinedDf.selectExpr(outputColumns: _*)
    finalDf.explain()
    finalDf
  }

  /*
   * The purpose of Bootstrap is to leverage input tables which contain pre-computed values, such that we can
   * skip the computation for these record during the join-part computation step.
   *
   * The main goal here to join together the various bootstrap source to the left table, and in the process maintain
   * relevant metadata such that we can easily tell which record needs computation or not in the following step.
   */
  private def computeBootstrapTable(leftDf: DataFrame,
                                    range: PartitionRange,
                                    bootstrapInfo: BootstrapInfo): DataFrame = {

    // For consistency comparison join, we also need to materialize the left table as bootstrap table in order to
    // make random OOC sampling deterministic.
    val isConsistencyJoin =
      joinConf.metaData.isSetTableProperties && joinConf.metaData.tableProperties.containsKey(Constants.ChrononOOCTable)

    if (!joinConf.isSetBootstrapParts && !isConsistencyJoin) {
      return leftDf
    }

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
      .unfilledRanges(bootstrapTable, range)
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

            println(s"\nProcessing Bootstrap from table ${part.table} for range ${unfilledRange}")

            val bootstrapRange = if (part.isSetQuery) {
              unfilledRange.intersect(PartitionRange(part.query.startPartition, part.query.endPartition))
            } else {
              unfilledRange
            }
            if (!bootstrapRange.valid) {
              println(s"partition range of bootstrap table ${part.table} is beyond unfilled range")
              partialDf
            } else {
              var bootstrapDf = tableUtils.sql(
                bootstrapRange.genScanQuery(part.query, part.table, Map(Constants.PartitionColumn -> null))
              )

              // attach semantic_hash for either log or regular table bootstrap
              validateReservedColumns(bootstrapDf, part.table, Seq(Constants.BootstrapHash, Constants.MatchedHashes))
              if (part.isLogTable(joinConf)) {
                bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, col(Constants.SchemaHash))
              } else {
                bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, lit(part.semanticHash))
              }

              // include only necessary columns. in particular,
              // this excludes columns that are NOT part of Join's output (either from GB or external source)
              val includedColumns = bootstrapDf.columns.filter(
                bootstrapInfo.fieldNames ++ part.keys(joinConf) ++ Seq(Constants.BootstrapHash,
                                                                       Constants.PartitionColumn))

              bootstrapDf = bootstrapDf
                .select(includedColumns.map(col): _*)
                // TODO: allow customization of deduplication logic
                .dropDuplicates(part.keys(joinConf))

              coalescedJoin(partialDf, bootstrapDf, part.keys(joinConf) :+ Constants.PartitionColumn)
              // as part of the left outer join process, we update and maintain matched_hashes for each record
              // that summarizes whether there is a join-match for each bootstrap source.
              // later on we use this information to decide whether we still need to re-run the backfill logic
                .withColumn(Constants.MatchedHashes,
                            set_add(col(Constants.MatchedHashes), col(Constants.BootstrapHash)))
                .drop(Constants.BootstrapHash)
            }
          }
        }

        // set autoExpand = true since log table could be a bootstrap part
        joinedDf.save(bootstrapTable, tableProps, autoExpand = true)
      })

    val elapsedMins = (System.currentTimeMillis() - startMillis) / (60 * 1000)
    println(s"Finished computing bootstrap table ${joinConf.metaData.bootstrapTable} in ${elapsedMins} minutes")

    tableUtils.sql(range.genScanQuery(query = null, table = bootstrapTable))
  }

  /*
   * We leverage metadata information created from the bootstrap step to tell which record was already joined to a
   * bootstrap source, and therefore had certain columns pre-populated. for these records and these columns, we do not
   * need to run backfill again. this is possible because the hashes in the metadata columns can be mapped back to
   * full schema information.
   */
  private def findUnfilledRecords(bootstrapDf: DataFrame,
                                  joinPart: JoinPart,
                                  bootstrapInfo: BootstrapInfo): (DataFrame, Seq[String]) = {

    if (!bootstrapDf.columns.contains(Constants.MatchedHashes)) {
      // this happens whether bootstrapParts is NULL for the JOIN and thus no metadata columns were created
      return (bootstrapDf, Seq())
    }

    val requiredFields =
      bootstrapInfo.joinParts.find(_.joinPart.groupBy.metaData.name == joinPart.groupBy.metaData.name).get.valueSchema

    // We mark some hashes to be valid. A valid hash means that if a record was marked with this hash during bootstrap,
    // it already had all the required columns populated, and therefore can be skipped in backfill.
    val validHashes =
      bootstrapInfo.hashToSchema.filter(entry => requiredFields.forall(f => entry._2.contains(f))).keys.toSeq

    println(
      s"""Finding records to backfill for joinPart: ${joinPart.groupBy.metaData.name}
         |by splitting left into filled vs unfilled based on valid_hashes: ${validHashes.prettyInline}
         |""".stripMargin
    )

    // Unfilled records are those that are not marked by any of the valid hashes, and thus require backfill
    val filterCondition = not(contains_any(col(Constants.MatchedHashes), typedLit[Seq[String]](validHashes)))
    (bootstrapDf.where(filterCondition), validHashes)
  }
}
