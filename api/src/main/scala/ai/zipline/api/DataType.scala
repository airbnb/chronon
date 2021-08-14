package ai.zipline.api

sealed trait DataType extends Serializable

case object IntType extends DataType

case object LongType extends DataType

case object DoubleType extends DataType

case object FloatType extends DataType

case object ShortType extends DataType

case object BooleanType extends DataType

case object ByteType extends DataType

case object StringType extends DataType

case object BinaryType extends DataType

// maps to java.util.ArrayList[ElementType]
case class ListType(elementType: DataType) extends DataType

case class MapType(keyType: DataType, valueType: DataType) extends DataType

case class StructField(name: String, fieldType: DataType)

// maps to Array[Any]
case class StructType(name: String, fields: Array[StructField]) extends DataType with Seq[StructField] {
  def unpack: Seq[(String, DataType)] = fields.map { field => field.name -> field.fieldType }

  override def apply(idx: Int): StructField = fields(idx)
  override def length: Int = fields.length

  override def iterator: Iterator[StructField] = fields.iterator
}

object StructType {
  def from(name: String, fields: Array[(String, DataType)]): StructType = {
    StructType(name, fields.map { case (fieldName, dataType) => StructField(fieldName, dataType) })
  }
}

// mechanism to accept unknown types into the ai.zipline.aggregator.row
// while retaining the original type object for reconstructing the source type information
case class UnknownType(any: Any) extends DataType
