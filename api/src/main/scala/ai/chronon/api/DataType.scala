/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.api

import java.util
import scala.util.ScalaJavaConversions.{JListOps, ListOps}

sealed trait DataType extends Serializable

object DataType {
  def toString(dataType: DataType): String =
    dataType match {
      case IntType                     => "int"
      case LongType                    => "long"
      case DoubleType                  => "double"
      case FloatType                   => "float"
      case ShortType                   => "short"
      case BooleanType                 => "bool"
      case ByteType                    => "byte"
      case StringType                  => "string"
      case BinaryType                  => "binary"
      case ListType(elementType)       => s"list_${toString(elementType)}"
      case MapType(keyType, valueType) => s"map_${toString(keyType)}_${toString(valueType)}"
      case DateType                    => "date"
      case TimestampType               => "timestamp"
      case StructType(name, _)         => s"struct_$name"
      case UnknownType(any)            => "unknown_type"
    }

  def isScalar(dt: DataType): Boolean =
    dt match {
      case IntType | LongType | ShortType | DoubleType | FloatType | StringType | BinaryType | BooleanType =>
        true
      case _ => false
    }

  def isNumeric(dt: DataType): Boolean =
    dt match {
      case IntType | LongType | DoubleType | FloatType | ShortType | ByteType => true
      case _                                                                  => false
    }

  def isList(dt: DataType): Boolean =
    dt match {
      case ListType(_) => true
      case _           => false
    }

  def isMap(dt: DataType): Boolean =
    dt match {
      case MapType(_, _) => true
      case _             => false
    }

  def fromTDataType(tDataType: TDataType): DataType = {
    val typeParams = tDataType.params
    tDataType.kind match {
      // non parametric types
      case DataKind.BOOLEAN   => BooleanType
      case DataKind.BYTE      => ByteType
      case DataKind.SHORT     => ShortType
      case DataKind.INT       => IntType
      case DataKind.LONG      => LongType
      case DataKind.FLOAT     => FloatType
      case DataKind.DOUBLE    => DoubleType
      case DataKind.STRING    => StringType
      case DataKind.BINARY    => BinaryType
      case DataKind.DATE      => DateType
      case DataKind.TIMESTAMP => TimestampType

      // parametric types
      case DataKind.MAP => {
        assert(typeParams != null && typeParams.size() == 2,
               s"TDataType needs non null `params` with length 2 when kind == MAP. Given: $typeParams")
        MapType(fromTDataType(typeParams.get(0).dataType), fromTDataType(typeParams.get(1).dataType))
      }
      case DataKind.LIST => {
        assert(typeParams != null && typeParams.size() == 1,
               s"TDataType needs non null `params` with length 1 when kind == LIST. Given: $typeParams")
        ListType(fromTDataType(typeParams.get(0).dataType))
      }
      case DataKind.STRUCT => {
        assert(typeParams != null && !typeParams.isEmpty,
               s"TDataType needs non null `params` with non-zero length when kind == Struct. Given: $typeParams")
        val fields = typeParams.toScala
          .map(param => StructField(param.name, fromTDataType(param.dataType)))
        StructType(tDataType.name, fields.toArray)
      }
    }
  }

  def toTDataType(dataType: DataType): TDataType = {
    def toParams(params: (String, DataType)*): util.List[DataField] = {
      params
        .map {
          case (name, dType) => new DataField().setName(name).setDataType(toTDataType(dType))
        }
        .toList
        .toJava
    }
    dataType match {
      case IntType               => new TDataType(DataKind.INT)
      case LongType              => new TDataType(DataKind.LONG)
      case DoubleType            => new TDataType(DataKind.DOUBLE)
      case FloatType             => new TDataType(DataKind.FLOAT)
      case ShortType             => new TDataType(DataKind.SHORT)
      case BooleanType           => new TDataType(DataKind.BOOLEAN)
      case ByteType              => new TDataType(DataKind.BYTE)
      case StringType            => new TDataType(DataKind.STRING)
      case BinaryType            => new TDataType(DataKind.BINARY)
      case ListType(elementType) => new TDataType(DataKind.LIST).setParams(toParams("elem" -> elementType))
      case MapType(keyType, valueType) =>
        new TDataType(DataKind.MAP).setParams(toParams("key" -> keyType, "value" -> valueType))
      case DateType      => new TDataType(DataKind.DATE)
      case TimestampType => new TDataType(DataKind.TIMESTAMP)
      case StructType(name, fields) =>
        new TDataType(DataKind.STRUCT).setName(name).setParams(toParams(fields.map(f => f.name -> f.fieldType): _*))
      case UnknownType(any) => throw new RuntimeException("Cannot convert unknown type")
    }
  }
}

case object IntType extends DataType

case object LongType extends DataType

case object DoubleType extends DataType

case object FloatType extends DataType

case object ShortType extends DataType

case object BooleanType extends DataType

case object ByteType extends DataType

case object StringType extends DataType

// maps to Array[Byte]
case object BinaryType extends DataType

// maps to java.util.ArrayList[ElementType]
case class ListType(elementType: DataType) extends DataType

// maps to java.util.Map[KeyType, ValueType]
case class MapType(keyType: DataType, valueType: DataType) extends DataType

case class StructField(name: String, fieldType: DataType)

// maps to java.sql.Date
case object DateType extends DataType

// maps to java.sql.Timestamp
case object TimestampType extends DataType

// maps to Array[Any]
case class StructType(name: String, fields: Array[StructField])
    extends DataType
    with scala.collection.Seq[StructField] {
  def unpack: scala.collection.Seq[(String, DataType)] = fields.map { field => field.name -> field.fieldType }

  override def apply(idx: Int): StructField = fields(idx)
  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator
  override def stringPrefix: String = this.getClass.getSimpleName
  def typeOf(name: String): Option[DataType] = fields.find(_.name == name).map(_.fieldType)
}

object StructType {
  def from(name: String, fields: Array[(String, DataType)]): StructType = {
    StructType(name, fields.map { case (fieldName, dataType) => StructField(fieldName, dataType) })
  }
}

// mechanism to accept unknown types into the ai.chronon.aggregator.row
// while retaining the original type object for reconstructing the source type information
case class UnknownType(any: Any) extends DataType
