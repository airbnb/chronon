package ai.zipline.spark

import java.util

import ai.zipline.aggregator.base.{
  IntType,
  ListType,
  UnknownType,
  BinaryType => MBinaryType,
  BooleanType => MBooleanType,
  ByteType => MByteType,
  DataType => MDataType,
  DoubleType => MDoubleType,
  FloatType => MFloatType,
  LongType => MLongType,
  MapType => MMapType,
  ShortType => MShortType,
  StringType => MStringType,
  StructField => MStructField,
  StructType => MStructType
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
  def fromMooliRow(
      values: Array[Any],
      ziplineSchema: Array[(String, MDataType)]
  ): Array[Any] = {
    if (values == null) return null
    val result = new Array[Any](values.length)
    for (i <- values.indices) {
      val columnType = ziplineSchema(i)._2
      val convertedColumn = fromMooliColumn(values(i), columnType)
      result.update(i, convertedColumn)
    }
    result
  }

  // won't handle arbitrary nesting
  // lists are util.ArrayList in both spark and zipline
  // structs in zipline are Array[Any], but in spark they are Row/GenericRow types
  def fromMooliColumn(value: Any, dataType: MDataType): Any = {
    if (value == null) return null
    dataType match {
      case ListType(MStructType(_, _)) => //Irs of LastK, FirstK
        val list = value.asInstanceOf[util.ArrayList[Array[Any]]]
        val result = new util.ArrayList[GenericRow](list.size())
        for (i <- 0 until list.size()) {
          result.set(i, new GenericRow(list.get(i)))
        }
        result
      case MStructType(_, _) =>
        new GenericRow(
          value.asInstanceOf[Array[Any]]
        ) // Irs of avg, last, first
      case _ => value
    }
  }

  def toMooliRow(row: Row, tsIndex: Int): ArrayRow = new ArrayRow(row, tsIndex)

  def toMooliType(dataType: DataType): MDataType =
    dataType match {
      case IntegerType               => IntType
      case LongType                  => MLongType
      case ShortType                 => MShortType
      case ByteType                  => MByteType
      case FloatType                 => MFloatType
      case DoubleType                => MDoubleType
      case StringType                => MStringType
      case BinaryType                => MBinaryType
      case BooleanType               => MBooleanType
      case ArrayType(elementType, _) => ListType(toMooliType(elementType))
      case MapType(keyType, valueType, _) =>
        MMapType(toMooliType(keyType), toMooliType(valueType))
      case StructType(fields) =>
        MStructType(
          null,
          fields.map { field =>
            MStructField(field.name, toMooliType(field.dataType))
          }.toList
        )
      case other => UnknownType(other)
    }

  def fromMooliType(mType: MDataType): DataType =
    mType match {
      case IntType               => IntegerType
      case MLongType             => LongType
      case MShortType            => ShortType
      case MByteType             => ByteType
      case MFloatType            => FloatType
      case MDoubleType           => DoubleType
      case MStringType           => StringType
      case MBinaryType           => BinaryType
      case MBooleanType          => BooleanType
      case ListType(elementType) => ArrayType(fromMooliType(elementType))
      case MMapType(keyType, valueType) =>
        MapType(fromMooliType(keyType), fromMooliType(valueType))
      case MStructType(_, fields) =>
        StructType(fields.map { field =>
          StructField(field.name, fromMooliType(field.fieldType))
        })
      case UnknownType(other) => other.asInstanceOf[DataType]
    }

  def toMooliSchema(schema: StructType): Array[(String, MDataType)] =
    schema.fields.map { field =>
      (field.name, toMooliType(field.dataType))
    }

  def fromMooliSchema(schema: Array[(String, MDataType)]): StructType =
    StructType(schema.map {
      case (name, mType) =>
        StructField(name, fromMooliType(mType))
    })
}
