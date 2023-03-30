package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.SparkConversions
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.collection.Seq
import org.apache.spark.sql

import scala.util.ScalaJavaConversions.{IterableOps, ListOps}

class Join(joinConf: api.Join, endPartition: String, tableUtils: TableUtils, skipFirstHole: Boolean = true)
    extends BaseJoin(joinConf, endPartition, tableUtils, skipFirstHole) {

  private val bootstrapTable = joinConf.metaData.bootstrapTable

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
          newDf = newDf.withColumn(field.name, coalesce(col(field.name), col(prefixedName)))
          newDf = newDf.withColumn(prefixedName, coalesce(col(field.name), col(prefixedName)))
          newDf
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

  override def computeRange(leftDf: DataFrame, leftRange: PartitionRange, bootstrapInfo: BootstrapInfo): DataFrame = {
    val leftTaggedDf = if (leftDf.schema.names.contains(Constants.TimeColumn)) {
      leftDf.withTimeBasedColumn(Constants.TimePartitionColumn)
    } else {
      leftDf
    }

    // compute bootstrap table - a left outer join between left source and various bootstrap source table
    // this becomes the "new" left for the following GB backfills
    val bootstrapDf = computeBootstrapTable(leftTaggedDf, leftRange, bootstrapInfo)

    // compute join parts (GB) backfills
    // for each GB, we first find out the unfilled subset of bootstrap table which still requires the backfill.
    // we do this by utilizing the per-record metadata computed during the bootstrap process.
    // then for each GB, we compute a join_part table that contains aggregated feature values for the required key space
    // the required key space is a slight superset of key space of the left, due to the nature of using bloom-filter.
    val rightResults = bootstrapInfo.joinParts.parallel
      .flatMap { partMetadata =>
        val unfilledLeftDf = findUnfilledRecords(bootstrapDf, partMetadata)
        val joinPart = partMetadata.joinPart
        computeRightTable(unfilledLeftDf, joinPart, leftRange).map(df => joinPart -> df)
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
    val finalBaseDf = padGroupByFields(joinedDf.selectExpr(outputColumns: _*), bootstrapInfo)
    val finalDf = cleanUpContextualFields(applyDerivation(finalBaseDf, bootstrapInfo), bootstrapInfo, leftDf.columns)
    finalDf.explain()
    finalDf
  }

  def applyDerivation(baseDf: DataFrame, bootstrapInfo: BootstrapInfo): DataFrame = {
    if (!joinConf.isSetDerivations || joinConf.derivations.isEmpty) {
      return baseDf
    }

    val projections = joinConf.derivationProjection(bootstrapInfo.baseValueNames)
    val derivationOutputColumns = projections.map(_._1).toSet
    val baseOutputColumns = baseDf.columns.toSet

    val finalOutputColumns =
      /*
       * Loop through all columns in the base join output:
       * 1. If it is one of the value columns, then skip it here and it will be handled later as we loop through
       *    derived columns again - derivation is a projection from all value columns to desired derived columns
       * 2. If it is matching one of the projected output columns, then this is a boostrapped derivation case, we also
       *    skip it here and it will be handled later as loop through derivations to perform coalescing
       * 3. Else, we keep it in the final output - cases falling here are either (1) key columns, or (2)
       *    arbitrary columns selected from left.
       */
      baseDf.columns.flatMap { c =>
        if (bootstrapInfo.baseValueNames.contains(c)) {
          None
        } else if (derivationOutputColumns.contains(c)) {
          None // handled below
        } else {
          Some(col(c))
        }
      } ++
        /*
         * Loop through all clauses in derivation projections:
         * 1. if it has the distinct contextualPrefix and is simply a reselection (where name & expr match), we skip it
         *    as we don't want contextual fields to be doubly selected, since the key version is always selected
         * 2. if the base output already has a column with a matching derivation column name, this is a bootstrapped
         *    derivation case (see case 2 above), then we do the coalescing to achieve the bootstrap behavior.
         * 3. Else, we do the standard projection.
         */
        projections
          .flatMap {
            case (name, expression) =>
              if (name == expression && name.startsWith(Constants.ContextualPrefix)) {
                None
              } else if (baseOutputColumns.contains(name)) {
                Some(coalesce(col(name), expr(expression)).as(name))
              } else {
                Some(expr(expression).as(name))
              }
          }

    baseDf.select(finalOutputColumns: _*)
  }

  /*
   * Remove extra contextual keys unless it is a result of derivations or it is a column from left
   */
  def cleanUpContextualFields(finalDf: DataFrame, bootstrapInfo: BootstrapInfo, leftColumns: Seq[String]): DataFrame = {

    val contextualNames =
      bootstrapInfo.externalParts.filter(_.externalPart.isContextual).flatMap(_.keySchema).map(_.name)
    val projections = if (joinConf.isSetDerivations) {
      joinConf.derivationProjection(bootstrapInfo.baseValueNames).map(_._1)
    } else {
      Seq()
    }
    contextualNames.foldLeft(finalDf) {
      case (df, name) => {
        if (leftColumns.contains(name)) {
          df
        } else if (projections.contains(name)) {
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
  private def computeBootstrapTable(leftDf: DataFrame,
                                    range: PartitionRange,
                                    bootstrapInfo: BootstrapInfo): DataFrame = {

    // For consistency comparison join, we also need to materialize the left table as bootstrap table in order to
    // make random OOC sampling deterministic.
    val isConsistencyJoin =
      joinConf.metaData.isSetTableProperties && joinConf.metaData.tableProperties.containsKey(Constants.ChrononOOCTable)

    if (!joinConf.isSetBootstrapParts && !isConsistencyJoin) {
      return padExternalFields(leftDf, bootstrapInfo)
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
              if (bootstrapDf.columns.contains(Constants.SchemaHash)) {
                bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, col(Constants.SchemaHash))
              } else {
                bootstrapDf = bootstrapDf.withColumn(Constants.BootstrapHash, lit(part.semanticHash))
              }

              // include only necessary columns. in particular,
              // this excludes columns that are NOT part of Join's output (either from GB or external source)
              val includedColumns = bootstrapDf.columns
                .filter(bootstrapInfo.fieldNames ++ part.keys(joinConf) ++ Seq(Constants.BootstrapHash))
                .sorted

              bootstrapDf = bootstrapDf
                .select(includedColumns.map(col): _*)
                // TODO: allow customization of deduplication logic
                .dropDuplicates(part.keys(joinConf).toArray)

              coalescedJoin(partialDf, bootstrapDf, part.keys(joinConf))
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
    println(s"Finished computing bootstrap table ${joinConf.metaData.bootstrapTable} in ${elapsedMins} minutes")

    tableUtils.sql(range.genScanQuery(query = null, table = bootstrapTable))
  }

  /*
   * We leverage metadata information created from the bootstrap step to tell which record was already joined to a
   * bootstrap source, and therefore had certain columns pre-populated. for these records and these columns, we do not
   * need to run backfill again. this is possible because the hashes in the metadata columns can be mapped back to
   * full schema information.
   */
  private def findUnfilledRecords(bootstrapDf: DataFrame, joinPartMetadata: JoinPartMetadata): DataFrame = {

    if (!bootstrapDf.columns.contains(Constants.MatchedHashes)) {
      // this happens whether bootstrapParts is NULL for the JOIN and thus no metadata columns were created
      return bootstrapDf
    }

    // Unfilled records are those that are not marked by any of the valid hashes, and thus require backfill
    val filterCondition = not(
      contains_any(
        col(Constants.MatchedHashes),
        typedLit[Seq[String]](joinPartMetadata.validHashes)
      )
    )
    bootstrapDf.where(filterCondition)
  }
}
