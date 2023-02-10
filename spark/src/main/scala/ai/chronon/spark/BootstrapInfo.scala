package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.{Constants, ExternalPart, JoinPart, StructField}
import ai.chronon.online.{JoinCodec, SparkConversions}

import scala.util.ScalaVersionSpecificCollectionsConverter

case class JoinPartMetadata(
    joinPart: JoinPart,
    keySchema: Array[StructField],
    valueSchema: Array[StructField]
)

case class ExternalPartMetadata(
    externalPart: ExternalPart,
    keySchema: Array[StructField],
    valueSchema: Array[StructField]
)

case class BootstrapInfo(
    joinConf: api.Join,
    // join parts enriched with expected output schema
    joinParts: Seq[JoinPartMetadata],
    // external parts enriched with expected schema
    externalParts: Seq[ExternalPartMetadata],
    // hashToSchema is a mapping from a hash string to an associated list of structField, such that if a record was
    // marked by this hash, it means that all the associated structFields have been pre-populated.
    // to generate the hash:
    // - for chronon flattened log table: we use the schema_hash recorded at logging time generated per-row
    // - for regular hive tables: we use the semantic_hash (a snapshot) of the bootstrap part object
    hashToSchema: Map[String, Array[StructField]]
) {

  lazy val fieldNames: Set[String] = fields.map(_.name).toSet

  private lazy val fields: Seq[StructField] = {
    joinParts.flatMap(_.keySchema) ++ joinParts.flatMap(_.valueSchema) ++
      externalParts.flatMap(_.keySchema) ++ externalParts.flatMap(_.valueSchema)
  }
  private lazy val fieldsMap: Map[String, StructField] = fields.map(f => f.name -> f).toMap
}

object BootstrapInfo {

  // Build metadata for the join that contains schema information for join parts, external parts and bootstrap parts
  def from(joinConf: api.Join, range: PartitionRange, tableUtils: TableUtils): BootstrapInfo = {

    // Enrich each join part with the expected output schema
    println(s"\nCreating BootstrapInfo for GroupBys for Join ${joinConf.metaData.name}")
    val joinParts: Seq[JoinPartMetadata] = if (!joinConf.isSetJoinParts) {
      Seq.empty
    } else {
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(joinConf.joinParts)
        .map(part => {
          val gb = GroupBy.from(part.groupBy, range, tableUtils)
          val keySchema = SparkConversions
            .toChrononSchema(gb.keySchema)
            .map(field => StructField(part.rightToLeft(field._1), field._2))
          val valueSchema = gb.outputSchema.fields.map(part.constructJoinPartSchema)
          JoinPartMetadata(part, keySchema, valueSchema)
        })
    }

    // Enrich each external part with the expected output schema
    println(s"\nCreating BootstrapInfo for ExternalParts for Join ${joinConf.metaData.name}")
    val externalParts: Seq[ExternalPartMetadata] = if (!joinConf.isSetOnlineExternalParts) {
      Seq.empty
    } else {
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(joinConf.onlineExternalParts)
        .map(part => ExternalPartMetadata(part, part.keySchemaFull, part.valueSchemaFull))
    }

    /*
     * Log table requires special handling because unlike regular Hive table, since log table is affected by schema
     * evolution. A NULL column does not simply mean that the correct value was NULL, it could be that for this record,
     * the value was never logged as the column was added more recently. Instead, we must use the schema info encoded
     * in the schema_hash at time of logging to tell whether a column truly was populated or not.
     */
    println(s"\nCreating BootstrapInfo for Log Based Bootstraps for Join ${joinConf.metaData.name}")
    val logHashes = if (!joinConf.isSetBootstrapParts) {
      Map.empty[String, Array[StructField]]
    } else {

      // Verify that join keys are valid columns on the log table
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(joinConf.bootstrapParts)
        .withFilter(_.isLogTable(joinConf))
        .foreach(part => {
          // practically there should only be one logBootstrapPart per Join, but nevertheless we will loop here
          val logTable = joinConf.metaData.loggedTable
          val schema = tableUtils.getSchemaFromTable(logTable)
          val missingKeys = part.keys(joinConf).filterNot(schema.fieldNames.contains)
          assert(
            missingKeys.isEmpty,
            s"Log table $logTable for join ${joinConf.metaData.name} does not contain some specified keys: ${missingKeys.prettyInline}"
          )
        })

      // Retrieve schema_hash mapping info from Hive table properties
      LogFlattenerJob
        .readSchemaTableProperties(tableUtils, joinConf)
        .mapValues(JoinCodec.fromLoggingSchema(_, joinConf).valueFields)
    }

    /*
     * For standard Hive tables however, we can simply trust that a NULL column means that its CORRECT value is NULL,
     * that is, we trust all the data that is given to Chronon by its user and it takes precedence over backfill.
     */
    println(s"\nCreating BootstrapInfo for Table Based Bootstraps for Join ${joinConf.metaData.name}")
    val tableHashes = if (!joinConf.isSetBootstrapParts) {
      Map.empty[String, (Array[StructField], String, String)]
    } else {

      // Verify that join keys are valid columns on the bootstrap source table
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(joinConf.bootstrapParts)
        .withFilter(!_.isLogTable(joinConf))
        .map(part => {
          val range = PartitionRange(part.query.startPartition, part.query.endPartition)
          val bootstrapQuery = range.genScanQuery(part.query, part.table, Map(Constants.PartitionColumn -> null))
          val bootstrapDf = tableUtils.sql(bootstrapQuery)
          val schema = bootstrapDf.schema
          val missingKeys = part.keys(joinConf).filterNot(schema.fieldNames.contains)
          assert(
            missingKeys.isEmpty,
            s"Table ${part.table} does not contain some specified keys: ${missingKeys.prettyInline}"
          )

          val valueFields = SparkConversions
            .toChrononSchema(schema)
            .filterNot {
              case (name, _) => (part.keys(joinConf) :+ Constants.PartitionColumn).contains(name)
            }
            .map(field => StructField(field._1, field._2))

          part.semanticHash -> (valueFields, part.table, bootstrapQuery)
        })
        .toMap
    }

    val hashToSchema = logHashes ++ tableHashes.mapValues(_._1)
    val bootstrapInfo = BootstrapInfo(joinConf, joinParts, externalParts, hashToSchema)

    // validate that all selected fields except keys from (non-log) bootstrap tables match with
    // one of defined fields in join parts or external parts
    for (
      (fields, table, query) <- tableHashes.valuesIterator;
      field <- fields
    ) yield {

      assert(
        bootstrapInfo.fieldsMap.contains(field.name),
        s"""Table $table has column ${field.name} with ${field.fieldType}, but Join ${joinConf.metaData.name} does NOT have this field
           |Bootstrap Query:
           |${query}
           |""".stripMargin
      )
      assert(
        bootstrapInfo.fieldsMap(field.name) == field,
        s"""Table $table has column ${field.name} with ${field.fieldType}, but Join ${joinConf.metaData.name} has the same field with ${bootstrapInfo
          .fieldsMap(field.name)
          .fieldType}
           |Bootstrap Query:
           |${query}
           |""".stripMargin
      )
    }

    bootstrapInfo
  }
}
