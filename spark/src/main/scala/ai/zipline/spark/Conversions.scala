package ai.zipline.spark

import java.util

import ai.zipline.aggregator.base.{
  IntType,
  ListType,
  UnknownType,
  BinaryType => ZBinaryType,
  BooleanType => ZBooleanType,
  ByteType => ZByteType,
  DataType => ZDataType,
  DoubleType => ZDoubleType,
  FloatType => ZFloatType,
  LongType => ZLongType,
  MapType => ZMapType,
  ShortType => ZShortType,
  StringType => ZStringType,
  StructField => ZStructField,
  StructType => ZStructType
}
import ai.zipline.aggregator.row.{Row => MRow}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

// wrapper class of spark ai.zipline.aggregator.row that the RowAggregator can work with
// no copies are happening here, but we wrap the ai.zipline.aggregator.row with an additional class
class ArrayRow(val row: Row, val tsIndex: Int) extends MRow {

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
  def fromZiplineRow(
      values: Array[Any],
      ziplineSchema: Array[(String, ZDataType)]
  ): Array[Any] = {
    if (values == null) return null
    val result = new Array[Any](values.length)
    for (i <- values.indices) {
      val columnType = ziplineSchema(i)._2
      val convertedColumn = fromZiplineColumn(values(i), columnType)
      result.update(i, convertedColumn)
    }
    result
  }

  // won't handle arbitrary nesting
  // lists are util.ArrayList in both spark and zipline
  // structs in zipline are Array[Any], but in spark they are Row/GenericRow types
  def fromZiplineColumn(value: Any, dataType: ZDataType): Any = {
    if (value == null) return null
    dataType match {
      case ListType(ZStructType(_, _)) => //Irs of LastK, FirstK
        val list = value.asInstanceOf[util.ArrayList[Array[Any]]]
        val result = new util.ArrayList[GenericRow](list.size())
        for (i <- 0 until list.size()) {
          result.set(i, new GenericRow(list.get(i)))
        }
        result
      case ZStructType(_, _) =>
        new GenericRow(
          value.asInstanceOf[Array[Any]]
        ) // Irs of avg, last, first
      case _ => value
    }
  }

  def toZiplineRow(row: Row, tsIndex: Int): ArrayRow = new ArrayRow(row, tsIndex)

  def capitalize(str: String): String = s"${str.head.toUpper}${str.tail}"

  def toZiplineType(name: String, dataType: DataType): ZDataType = {
    val typeName = capitalize(name)
    dataType match {
      case IntegerType               => IntType
      case LongType                  => ZLongType
      case ShortType                 => ZShortType
      case ByteType                  => ZByteType
      case FloatType                 => ZFloatType
      case DoubleType                => ZDoubleType
      case StringType                => ZStringType
      case BinaryType                => ZBinaryType
      case BooleanType               => ZBooleanType
      case ArrayType(elementType, _) => ListType(toZiplineType(s"${typeName}Element", elementType))
      case MapType(keyType, valueType, _) =>
        ZMapType(toZiplineType(s"${typeName}Key", keyType), toZiplineType(s"${typeName}Value", valueType))
      case StructType(fields) =>
        ZStructType(
          s"${typeName}Struct",
          fields.map { field =>
            ZStructField(field.name, toZiplineType(field.name, field.dataType))
          }
        )
      case other => UnknownType(other)
    }
  }

  def fromZiplineType(zType: ZDataType): DataType =
    zType match {
      case IntType               => IntegerType
      case ZLongType             => LongType
      case ZShortType            => ShortType
      case ZByteType             => ByteType
      case ZFloatType            => FloatType
      case ZDoubleType           => DoubleType
      case ZStringType           => StringType
      case ZBinaryType           => BinaryType
      case ZBooleanType          => BooleanType
      case ListType(elementType) => ArrayType(fromZiplineType(elementType))
      case ZMapType(keyType, valueType) =>
        MapType(fromZiplineType(keyType), fromZiplineType(valueType))
      case ZStructType(_, fields) =>
        StructType(fields.map { field =>
          StructField(field.name, fromZiplineType(field.fieldType))
        })
      case UnknownType(other) => other.asInstanceOf[DataType]
    }

  def toZiplineSchema(schema: StructType): Array[(String, ZDataType)] =
    schema.fields.map { field =>
      (field.name, toZiplineType(field.name, field.dataType))
    }

  def fromZiplineSchema(schema: Seq[(String, ZDataType)]): StructType =
    StructType(schema.map {
      case (name, zType) =>
        StructField(name, fromZiplineType(zType))
    })
}
