package ai.chronon.online

import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api.{DataType, HashUtils, LongType, StringType, StructField, StructType, Constants}
import ai.chronon.aggregator.row.StatsGenerator
import com.google.gson.Gson

import scala.collection.Seq
import scala.util.ScalaJavaConversions.JMapOps

case class JoinCodec(conf: JoinOps,
                     keySchema: StructType,
                     entityKeySchema: StructType,
                     externalKeySchema: StructType,
                     baseValueSchema: StructType,
                     keyCodec: AvroCodec,
                     baseValueCodec: AvroCodec)
    extends Serializable {
  type DerivationFunc = (Map[String, Any], Map[String, Any]) => Map[String, Any]
  case class SchemaAndDeriveFunc(valueSchema: StructType,
                                 derivationFunc: (Map[String, Any], Map[String, Any]) => Map[String, Any])

  // We want the same branch logic to construct both schema and derivation
  // Conveniently, this also removes branching logic from hot path of derivation
  @transient lazy private val valueSchemaAndDeriveFunc: SchemaAndDeriveFunc =
    if (conf.join == null || conf.join.derivations == null || baseValueSchema.fields.isEmpty) {
      SchemaAndDeriveFunc(baseValueSchema, { case (_: Map[String, Any], values: Map[String, Any]) => values })
    } else {
      def build(fields: Seq[StructField], deriveFunc: DerivationFunc) =
        SchemaAndDeriveFunc(StructType(s"join_derived_${conf.join.metaData.cleanName}", fields.toArray), deriveFunc)
      // if spark catalyst is not necessary, and all the derivations are just renames, we don't invoke catalyst
      if (conf.areDerivationsRenameOnly) {
        val baseExpressions = if (conf.derivationsContainStar) {
          baseValueSchema.filterNot { conf.derivationExpressionSet contains _.name }
        } else {
          Seq.empty
        }
        val expressions = baseExpressions ++ conf.derivationsWithoutStar.map { d =>
          StructField(d.name, baseValueSchema.typeOf(d.expression).get)
        }
        build(
          expressions,
          {
            case (_: Map[String, Any], values: Map[String, Any]) =>
              JoinCodec.adjustExceptions(conf.applyRenameOnlyDerivation(values), values)
          }
        )
      } else {
        val baseExpressions = if (conf.derivationsContainStar) {
          baseValueSchema
            .filterNot { conf.derivationExpressionSet contains _.name }
            .map(sf => sf.name -> sf.name)
        } else { Seq.empty }
        val expressions = baseExpressions ++ conf.derivationsWithoutStar.map { d => d.name -> d.expression }
        val catalystUtil =
          new PooledCatalystUtil(expressions, StructType("all", (keySchema ++ baseValueSchema).toArray))
        build(
          catalystUtil.outputChrononSchema.map(tup => StructField(tup._1, tup._2)),
          {
            case (keys: Map[String, Any], values: Map[String, Any]) =>
              JoinCodec.adjustExceptions(catalystUtil.performSql(keys ++ values).orNull, values)
          }
        )
      }
    }

  @transient lazy val deriveFunc: (Map[String, Any], Map[String, Any]) => Map[String, Any] =
    valueSchemaAndDeriveFunc.derivationFunc

  @transient lazy val valueSchema: StructType = {
    val derivedSchema = valueSchemaAndDeriveFunc.valueSchema
    if (conf.logFullValues) {
      def toMap(schema: StructType): Map[String, DataType] = schema.map(field => (field.name, field.fieldType)).toMap
      val (baseMap, derivedMap) = (toMap(baseValueSchema), toMap(derivedSchema))
      StructType(
        s"join_combined_${conf.join.metaData.cleanName}",
        // derived values take precedence in case of collision
        (baseMap ++ derivedMap).map {
          case (name, dataTye) => StructField(name, dataTye)
        }.toArray
      )
    } else {
      derivedSchema
    }
  }
  @transient lazy val valueCodec: AvroCodec = AvroCodec.of(AvroConversions.fromChrononSchema(valueSchema).toString)

  /*
   * Get the serialized string repr. of the logging schema.
   * key_schema and value_schema are first converted to strings and then serialized as part of Map[String, String] => String conversion.
   *
   * Example:
   * {"join_name":"unit_test/test_join","key_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_key\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"listing\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}","value_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_value\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"unit_test_listing_views_v1_m_guests_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"},{\"name\":\"unit_test_listing_views_v1_m_views_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}"}
   */
  lazy val loggingSchema: String = JoinCodec.buildLoggingSchema(conf.join.metaData.name, keyCodec, valueCodec)
  lazy val loggingSchemaHash: String = HashUtils.md5Base64(loggingSchema)

  val keys: Array[String] = keySchema.fields.iterator.map(_.name).toArray
  val values: Array[String] = valueSchema.fields.iterator.map(_.name).toArray

  // contains a list of all key inputs to the join, an aggregation of entityKeyFields and externalKeyFields
  val keyFields: Array[StructField] = keySchema.fields
  // contains a list of all entity key inputs to the join, derived from the underlying GroupBys
  val entityKeyFields: Array[StructField] = entityKeySchema.fields
  // contains a list of all external key inputs to the join, derived from external parts
  val externalKeyFields: Array[StructField] = externalKeySchema.fields
  val valueFields: Array[StructField] = valueSchema.fields
  lazy val keyIndices: Map[StructField, Int] = keySchema.zipWithIndex.toMap
  lazy val valueIndices: Map[StructField, Int] = valueSchema.zipWithIndex.toMap
  @transient lazy val statsKeyCodec: AvroCodec =
    AvroCodec.of(AvroConversions.fromChrononSchema(Constants.StatsKeySchema).toString)
  val statsInputSchema: StructType = StatsGenerator.statsInputSchema(valueSchema)
  val statsIrSchema: StructType = StatsGenerator.statsIrSchema(valueSchema)
  val statsIrCodec: AvroCodec = AvroCodec.of(AvroConversions.fromChrononSchema(statsIrSchema).toString)

}

object JoinCodec {

  val timeFields: Array[StructField] = Array(
    StructField(Constants.TimeColumn, LongType),
    StructField(Constants.PartitionColumn, StringType)
  )

  // remove value fields of groupBys that have failed with exceptions
  private[online] def adjustExceptions(derived: Map[String, Any], preDerivation: Map[String, Any]): Map[String, Any] = {
    val exceptions: Map[String, Any] = preDerivation.iterator.filter(_._1.endsWith("_exception")).toMap
    if (exceptions.isEmpty) {
      return derived
    }
    val exceptionParts: Array[String] = exceptions.keys.map(_.dropRight("_exception".length)).toArray
    derived.filterKeys(key => !exceptionParts.exists(key.startsWith)).toMap ++ exceptions
  }

  def buildLoggingSchema(joinName: String, keyCodec: AvroCodec, valueCodec: AvroCodec): String = {
    val schemaMap = Map(
      "join_name" -> joinName,
      "key_schema" -> keyCodec.schemaStr,
      "value_schema" -> valueCodec.schemaStr
    )
    new Gson().toJson(schemaMap.toJava)
  }
}
