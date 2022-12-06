package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.JoinUtils._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.ScalaVersionSpecificCollectionsConverter

class Join(joinConf: api.Join, endPartition: String, tableUtils: TableUtils)
  extends BaseJoin(joinConf, endPartition, tableUtils) {

  private val bootstrapTable = joinConf.metaData.bootstrapTable

  override def computeRange(leftDf: DataFrame, leftRange: PartitionRange): DataFrame = {
    val leftTaggedDf = if (leftDf.schema.names.contains(Constants.TimeColumn)) {
      leftDf.withTimeBasedColumn(Constants.TimePartitionColumn)
    } else {
      leftDf
    }

    // compute bootstrap table - which handles log & custom hive tables bootstrap
    val joinMetadata = JoinMetadata.from(joinConf, leftRange, tableUtils)
    val bootstrapDf = computeBootstrapTable(leftTaggedDf, leftRange, joinMetadata)

    // compute join parts
    val rightResults = joinMetadata.joinParts
      .map(_.joinPart)
      .par
      .flatMap { part =>
        val unfilledLeftDf = findUnfilledRecords(bootstrapDf, part, joinMetadata)
        computeRightTable(unfilledLeftDf, part, leftRange)
          .map(df => part -> df)
      }

    // join across bootstrap and join parts
    val joinedDf = rightResults
      .foldLeft(bootstrapDf) {
        case (partialDf, (rightPart, rightDf)) => joinWithLeft(partialDf, rightDf, rightPart)
      }
      // drop all processing metadata columns
      .drop(Constants.SchemaHash,
        Constants.SemanticHashKey,
        Constants.LogHashes,
        Constants.TableHashes,
        Constants.TimePartitionColumn)

    val outputColumns = joinedDf.columns.filter((bootstrapDf.columns ++ joinMetadata.fieldNames).contains)
    val finalDf = joinedDf.selectExpr(outputColumns: _*)
    finalDf.explain()
    finalDf
  }

  private def computeBootstrapTable(leftDf: DataFrame, range: PartitionRange, joinMetadata: JoinMetadata): DataFrame = {
    if (!joinConf.isSetBootstrapParts) {
      return leftDf
    }

    def checkReservedColumns(df: DataFrame, table: String, column: String): Unit = {
      assert(
        !df.schema.fieldNames.contains(column),
        s"Table $table contains column $column which is a reserved column in Chronon."
      )
    }

    // verify left table does not have reserved columns
    Seq(Constants.SchemaHash, Constants.SemanticHashKey)
      .foreach(checkReservedColumns(leftDf, joinConf.left.table, _))

    tableUtils
      .unfilledRanges(bootstrapTable, range)
      .getOrElse(Seq())
      .foreach(unfilledRange => {
        val parts = ScalaVersionSpecificCollectionsConverter.convertJavaListToScala(joinConf.bootstrapParts)

        // initialize a few metadata columns in leftDf to make processing simpler
        // log_hashes contains a set of schema_hash from join-matched log rows
        // table_hashes contains a set of semantic_hash of the bootstrap parts from join-matched table rows
        // log vs table requires separate handling because log table allows schema evolution therefore an empty
        // column does not mean that the value has been computed; but we can trust standard tables that an empty
        // column means that the value is computed as NULL.
        val initDf = leftDf
          .prunePartition(unfilledRange)
          .withColumn(Constants.LogHashes, typedLit[Array[String]](null))
          .withColumn(Constants.TableHashes, typedLit[Array[String]](null))

        val joinedDf = parts.foldLeft(initDf) {
          case (partialDf, part) => {
            val bootstrapRange = if (part.isSetQuery) {
              unfilledRange.intersect(PartitionRange(part.query.startPartition, part.query.endPartition))
            } else {
              unfilledRange
            }
            if (!bootstrapRange.valid) {
              println(s"partition range of bootstrap table ${part.table} is beyond unfilled range")
              partialDf
            } else {
              var rightDf = tableUtils.sql(
                bootstrapRange.genScanQuery(part.query, part.table, Map(Constants.PartitionColumn -> null)))

              // attach semantic_hash for either log or regular table bootstrap
              checkReservedColumns(rightDf, part.table, Constants.SemanticHashKey)
              rightDf = rightDf.withColumn(Constants.SemanticHashKey, lit(part.semanticHash))

              // attach schema_hash as NULL for regular table to simplify processing
              if (!part.isLogTable(joinConf)) {
                checkReservedColumns(rightDf, part.table, Constants.SchemaHash)
                rightDf = rightDf.withColumn(Constants.SchemaHash, typedLit[String](null))
              }

              // find all needed columns on right_df
              val includedColumns = rightDf.columns.filter((joinMetadata.fieldNames ++ part.keys(joinConf) ++
                Seq(Constants.SchemaHash, Constants.SemanticHashKey, Constants.PartitionColumn)).contains)

              rightDf = rightDf
                .selectExpr(includedColumns: _*)
                // TODO: allow customization of deduplication logic
                .dropDuplicates(part.keys(joinConf))

              mergeDFs(partialDf, rightDf, part.keys(joinConf) :+ Constants.PartitionColumn)
              // merge and update log_hashes and table_hashes in order to track join matches
                .withColumn(Constants.LogHashes, set_add(col(Constants.LogHashes), col(Constants.SchemaHash)))
                .withColumn(Constants.TableHashes, set_add(col(Constants.TableHashes), col(Constants.SemanticHashKey)))
                .drop(Constants.SchemaHash, Constants.SemanticHashKey)
            }
          }
        }

        // set autoExpand = true since log table could be a bootstrap part
        joinedDf.save(bootstrapTable, tableProps, autoExpand = true)
      })

    tableUtils.sql(range.genScanQuery(query = null, table = bootstrapTable))
  }

  private def findUnfilledRecords(bootstrapDf: DataFrame, joinPart: JoinPart, joinMetadata: JoinMetadata): DataFrame = {

    if (!Seq(Constants.LogHashes, Constants.TableHashes).forall(bootstrapDf.columns.contains)) {
      return bootstrapDf
    }

    val requiredFields =
      joinMetadata.joinParts.find(_.joinPart.groupBy.metaData.name == joinPart.groupBy.metaData.name).get.valueSchema

    def validHashes(hashes: Map[String, Array[StructField]]): Seq[String] = {
      hashes.filter(entry => requiredFields.forall(f => entry._2.contains(f))).keys.toSeq
    }

    val validLogHashes = validHashes(joinMetadata.logHashes)
    val validTableHashes = validHashes(joinMetadata.tableHashes.mapValues(_._1))

    println(
      s"""Splitting left into filled vs unfilled by
         |validLogHashes: ${validLogHashes.prettyInline}
         |validTableHashes: ${validTableHashes.prettyInline}
         |for joinPart: ${joinPart.groupBy.metaData.name}
         |""".stripMargin
    )

    val filterCondition = not(
      contains_any(col(Constants.LogHashes), typedLit[Seq[String]](validLogHashes))
        .or(contains_any(col(Constants.TableHashes), typedLit[Seq[String]](validTableHashes)))
    )

    bootstrapDf.where(filterCondition)
  }
}
