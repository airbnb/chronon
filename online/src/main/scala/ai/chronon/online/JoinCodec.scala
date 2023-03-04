package ai.chronon.online

import ai.chronon.api.Extensions.{JoinOps, MetadataOps}
import ai.chronon.api.{HashUtils, Join, LongType, StringType, StructField, StructType}
import com.google.gson.Gson

import scala.collection.Seq
import scala.util.ScalaJavaConversions.MapOps
import scala.util.ScalaVersionSpecificCollectionsConverter

case class JoinCodec(conf: JoinOps,
                     keySchema: StructType,
                     baseValueSchema: StructType,
                     keyCodec: AvroCodec,
                     baseValueCodec: AvroCodec)
    extends Serializable {
  type DerivationFunc = (Map[String, AnyRef], Map[String, AnyRef]) => Map[String, AnyRef]
  case class SchemaAndDeriveFunc(valueSchema: StructType,
                                 derivationFunc: (Map[String, AnyRef], Map[String, AnyRef]) => Map[String, AnyRef])

  // We want the same branch logic to construct both schema and derivation
  // Conveniently, this also removes branching logic from hot path of derivation
  @transient lazy private val valueSchemaAndDeriveFunc: SchemaAndDeriveFunc =
    if (conf.join.derivations == null) {
      SchemaAndDeriveFunc(baseValueSchema, { case (_: Map[String, AnyRef], values: Map[String, AnyRef]) => values })
    } else {
      def build(fields: Seq[StructField], deriveFunc: DerivationFunc) =
        SchemaAndDeriveFunc(StructType(s"join_derived_${conf.join.metaData.cleanName}", fields.toArray), deriveFunc)
      // if spark catalyst is not necessary, and all the derivations are just renames, we don't invoke catalyst
      if (conf.areDerivationsRenameOnly) {
        build(
          if (conf.derivationsContainStar) {
            baseValueSchema.filterNot { conf.derivationExpressionSet contains _.name }
          } else { Seq.empty } ++ conf.derivationsWithoutStar.map { d =>
            StructField(d.name, baseValueSchema.typeOf(d.expression).get)
          },
          { case (_: Map[String, AnyRef], values: Map[String, AnyRef]) => conf.applyRenameOnlyDerivation(values) }
        )
      } else {
        val expressions = if (conf.derivationsContainStar) {
          baseValueSchema.filterNot { conf.derivationExpressionSet contains _.name }.map(sf => sf.name -> sf.name)
        } else { Seq.empty } ++ conf.derivationsWithoutStar.map { d => d.name -> d.expression }
        val catalystUtil = new CatalystUtil(expressions, StructType("all", (keySchema ++ baseValueSchema).toArray))
        build(
          catalystUtil.outputChrononSchema.map(tup => StructField(tup._1, tup._2)),
          {
            case (keys: Map[String, AnyRef], values: Map[String, AnyRef]) =>
              conf.applyRenameOnlyDerivation(keys ++ values)
          }
        )
      }
    }
  @transient lazy val valueSchema: StructType = valueSchemaAndDeriveFunc.valueSchema
  @transient lazy val deriveFunc: (Map[String, AnyRef], Map[String, AnyRef]) => Map[String, AnyRef] =
    valueSchemaAndDeriveFunc.derivationFunc
  @transient lazy val valueCodec: AvroCodec = AvroCodec.of(AvroConversions.fromChrononSchema(valueSchema).toString)

  /*
   * Get the serialized string repr. of the logging schema.
   * key_schema and value_schema are first converted to strings and then serialized as part of Map[String, String] => String conversion.
   *
   * Example:
   * {"join_name":"unit_test/test_join","key_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_key\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"listing\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}","value_schema":"{\"type\":\"record\",\"name\":\"unit_test_test_join_value\",\"namespace\":\"ai.chronon.data\",\"doc\":\"\",\"fields\":[{\"name\":\"unit_test_listing_views_v1_m_guests_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"},{\"name\":\"unit_test_listing_views_v1_m_views_sum\",\"type\":[\"null\",\"long\"],\"doc\":\"\"}]}"}
   */
  lazy val loggingSchema: String = {
    val schemaMap = Map(
      "join_name" -> conf.join.metaData.name,
      "key_schema" -> keyCodec.schemaStr,
      "value_schema" -> valueCodec.schemaStr
    )
    new Gson().toJson(ScalaVersionSpecificCollectionsConverter.convertScalaMapToJava(schemaMap))
  }
  lazy val loggingSchemaHash: String = HashUtils.md5Base64(loggingSchema)

  val keys: Array[String] = keySchema.fields.iterator.map(_.name).toArray
  val values: Array[String] = valueSchema.fields.iterator.map(_.name).toArray

  val keyFields: Array[StructField] = keySchema.fields
  val valueFields: Array[StructField] = valueSchema.fields
  lazy val keyIndices: Map[StructField, Int] = keySchema.zipWithIndex.toMap
  lazy val valueIndices: Map[StructField, Int] = valueSchema.zipWithIndex.toMap

}

object JoinCodec {

  val timeFields: Array[StructField] = Array(
    StructField("ts", LongType),
    StructField("ds", StringType)
  )

  def fromLoggingSchema(loggingSchema: String, joinConf: Join): JoinCodec = {
    val schemaMap = new Gson()
      .fromJson(
        loggingSchema,
        classOf[java.util.Map[java.lang.String, java.lang.String]]
      )
      .toScala

    val keyCodec = new AvroCodec(schemaMap("key_schema"))
    val valueCodec = new AvroCodec(schemaMap("value_schema"))

    JoinCodec(
      joinConf,
      keyCodec.chrononSchema.asInstanceOf[StructType],
      valueCodec.chrononSchema.asInstanceOf[StructType],
      keyCodec,
      valueCodec
    )
  }
}
