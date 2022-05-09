package ai.zipline.api

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
case class StructType(name: String, fields: Array[StructField]) extends DataType with scala.collection.Seq[StructField] {
  def unpack: scala.collection.Seq[(String, DataType)] = fields.map { field => field.name -> field.fieldType }

  override def apply(idx: Int): StructField = fields(idx)
  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator
  override def stringPrefix: String = "StructType"
}

object StructType {
  def from(name: String, fields: Array[(String, DataType)]): StructType = {
    StructType(name, fields.map { case (fieldName, dataType) => StructField(fieldName, dataType) })
  }
}

// mechanism to accept unknown types into the ai.zipline.aggregator.row
// while retaining the original type object for reconstructing the source type information
case class UnknownType(any: Any) extends DataType
