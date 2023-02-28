package ai.chronon.online

import ai.chronon.api
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

object Extensions {

  implicit class StructTypeOps(schema: StructType) {
    def pretty: String = {
      val schemaTuples = schema.fields.map { field =>
        field.dataType.simpleString -> field.name
      }

      // pad the first column so that the second column is aligned vertically
      val padding = schemaTuples.map(_._1.length).max
      schemaTuples
        .map {
          case (typ, name) => s"  ${typ.padTo(padding, ' ')} : $name"
        }
        .mkString("\n")
    }

    def toChrononSchema(name: String = null): api.StructType =
      api.StructType.from(name, SparkConversions.toChrononSchema(schema))

    def toAvroSchema(name: String = null): Schema = AvroConversions.fromChrononSchema(toChrononSchema(name))

    def toAvroCodec(name: String = null): AvroCodec = new AvroCodec(toAvroSchema(name).toString())
  }
}
