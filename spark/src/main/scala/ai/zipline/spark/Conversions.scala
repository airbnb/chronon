package ai.zipline.spark

import ai.zipline.aggregator.row.{Row => MRow}
import ai.zipline.api
import ai.zipline.api.{IntType, ListType, UnknownType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// wrapper class of spark ai.zipline.aggregator.row that the RowAggregator can work with
// no copies are happening here, but we wrap the ai.zipline.aggregator.row with an additional class
class RowWrapper(val row: Row, val tsIndex: Int) extends MRow {

  override def get(index: Int): Any = row.get(index)

  override val length: Int = row.size

  override def ts: Long = {
    require(
      tsIndex > -1,
      "Requested timestamp from a ai.zipline.aggregator.row with missing `ts` column"
    )
    getAs[Long](tsIndex)
  }
}

object Conversions {

  def toZiplineRow(row: Row, tsIndex: Int): RowWrapper = new RowWrapper(row, tsIndex)

  def toZiplineType(name: String, dataType: DataType): api.DataType = {
    val typeName = name.capitalize
    dataType match {
      case IntegerType               => IntType
      case LongType                  => api.LongType
      case ShortType                 => api.ShortType
      case ByteType                  => api.ByteType
      case FloatType                 => api.FloatType
      case DoubleType                => api.DoubleType
      case StringType                => api.StringType
      case BinaryType                => api.BinaryType
      case BooleanType               => api.BooleanType
      case ArrayType(elementType, _) => ListType(toZiplineType(s"${typeName}Element", elementType))
      case MapType(keyType, valueType, _) =>
        api.MapType(toZiplineType(s"${typeName}Key", keyType), toZiplineType(s"${typeName}Value", valueType))
      case StructType(fields) =>
        api.StructType(
          s"${typeName}Struct",
          fields.map { field =>
            api.StructField(field.name, toZiplineType(field.name, field.dataType))
          }
        )
      case other => UnknownType(other)
    }
  }

  def fromZiplineType(zType: api.DataType): DataType =
    zType match {
      case IntType               => IntegerType
      case api.LongType          => LongType
      case api.ShortType         => ShortType
      case api.ByteType          => ByteType
      case api.FloatType         => FloatType
      case api.DoubleType        => DoubleType
      case api.StringType        => StringType
      case api.BinaryType        => BinaryType
      case api.BooleanType       => BooleanType
      case ListType(elementType) => ArrayType(fromZiplineType(elementType))
      case api.MapType(keyType, valueType) =>
        MapType(fromZiplineType(keyType), fromZiplineType(valueType))
      case api.StructType(_, fields) =>
        StructType(fields.map { field =>
          StructField(field.name, fromZiplineType(field.fieldType))
        })
      case UnknownType(other) => other.asInstanceOf[DataType]
    }

  def toZiplineSchema(schema: StructType): Array[(String, api.DataType)] =
    schema.fields.map { field =>
      (field.name, toZiplineType(field.name, field.dataType))
    }

  def fromZiplineSchema(schema: Seq[(String, api.DataType)]): StructType =
    StructType(schema.map {
      case (name, zType) =>
        StructField(name, fromZiplineType(zType))
    })
}
