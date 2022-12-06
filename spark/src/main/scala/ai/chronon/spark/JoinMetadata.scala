package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.{Constants, ExternalPart, JoinPart, StructField}
import ai.chronon.online.JoinCodec

import scala.util.ScalaVersionSpecificCollectionsConverter

case class JoinPartMetadata(
    joinPart: JoinPart,
    leftKeySchema: Array[StructField],
    valueSchema: Array[StructField]
)

case class ExternalPartMetadata(
    externalPart: ExternalPart,
    leftKeySchema: Array[StructField],
    valueSchema: Array[StructField]
)

case class JoinMetadata(
    joinConf: api.Join,
    joinParts: Seq[JoinPartMetadata],
    externalParts: Seq[ExternalPartMetadata],
    logHashes: Map[String, Array[StructField]], // schema_hash -> value_schema
    tableHashes: Map[String, (Array[StructField], String)] // semantic_hash -> (value_schema, table_name)
) {

  lazy val fieldNames: Seq[String] = fields.map(_.name)
  private lazy val fields: Seq[StructField] = {
    joinParts.flatMap(_.leftKeySchema) ++ joinParts.flatMap(_.valueSchema) ++
      externalParts.flatMap(_.leftKeySchema) ++ externalParts.flatMap(_.valueSchema)
  }
  private lazy val fieldsMap: Map[String, StructField] = fields.map(f => f.name -> f).toMap
}

object JoinMetadata {

  // Build metadata for the join that contains schema information for join parts, external parts and bootstrap parts
  def from(joinConf: api.Join, range: PartitionRange, tableUtils: TableUtils): JoinMetadata = {

    // build join parts schema
    val joinParts: Seq[JoinPartMetadata] = ScalaVersionSpecificCollectionsConverter
      .convertJavaListToScala(joinConf.joinParts)
      .map(part => {
        val gb = GroupBy.from(part.groupBy, range, tableUtils)
        val leftKeySchema =
          Conversions.toChrononSchema(gb.keySchema).map(field => StructField(part.rightToLeft(field._1), field._2))
        val valueSchema = gb.outputSchema.fields.map(part.constructJoinPartSchema)
        JoinPartMetadata(part, leftKeySchema, valueSchema)
      })

    // build external parts schema
    val externalParts: Seq[ExternalPartMetadata] = if (!joinConf.isSetOnlineExternalParts) {
      Seq.empty
    } else {
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(joinConf.onlineExternalParts)
        .map(part => {
          val leftKeySchema = part.source.keyFields.map(field =>
            StructField(part.rightToLeft.getOrElse(field.name, field.name), field.fieldType))
          val valueSchema = part.source.valueFields.map(part.constructExternalPartSchema)
          ExternalPartMetadata(part, leftKeySchema, valueSchema)
        })
    }

    // build log_hashes mapping
    val logHashes = if (!joinConf.isSetBootstrapParts) {
      Map.empty[String, Array[StructField]]
    } else {
      // validations
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
            s"Log table ${logTable} for join ${joinConf.metaData.name} does not contain some specified keys: ${missingKeys.prettyInline}"
          )
        })

      LogFlattenerJob
        .readSchemaTableProperties(tableUtils, joinConf)
        .mapValues(JoinCodec.fromLoggingSchema(_, joinConf).valueFields)
    }

    // build table_hashes mapping
    val tableHashes = if (!joinConf.isSetBootstrapParts) {
      Map.empty[String, (Array[StructField], String)]
    } else {
      ScalaVersionSpecificCollectionsConverter
        .convertJavaListToScala(joinConf.bootstrapParts)
        .withFilter(!_.isLogTable(joinConf))
        .map(part => {
          val schema = tableUtils.getSchemaFromTable(part.table)
          val missingKeys = part.keys(joinConf).filterNot(schema.fieldNames.contains)
          assert(
            missingKeys.isEmpty,
            s"Table ${part.table} does not contain some specified keys: ${missingKeys.prettyInline}"
          )

          val valueFields = Conversions
            .toChrononSchema(schema)
            .filterNot((part.keys(joinConf) :+ Constants.PartitionColumn).contains)
            .map(field => StructField(field._1, field._2))

          part.semanticHash -> (valueFields, part.table)
        })
        .toMap
    }

    val joinMetadata = JoinMetadata(joinConf, joinParts, externalParts, logHashes, tableHashes)

    // validate that all selected fields except keys from (non-log) bootstrap tables match with
    // one of defined fields in join parts or external parts
    for (
      (fields, table) <- tableHashes.valuesIterator;
      field <- fields
    ) yield {
      assert(
        !joinMetadata.fieldsMap.contains(field.name) || joinMetadata.fieldsMap(field.name) != field,
        s"Table $table has column ${field.name} with ${field.fieldType}, but Join ${joinConf.metaData.name} has the same field with ${joinMetadata.fieldsMap(field.name).fieldType}"
      )
    }

    joinMetadata
  }
}
