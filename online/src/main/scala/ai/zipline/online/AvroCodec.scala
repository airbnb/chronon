package ai.zipline.online

import ai.zipline.api.{Row, _}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._

import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._
import scala.collection.mutable

class AvroCodec(val schemaStr: String) extends Serializable {
  @transient private lazy val parser = new Schema.Parser()
  @transient private lazy val schema = parser.parse(schemaStr)

  // we reuse a lot of intermediate
  // lazy vals so that spark can serialize & ship the codec to executors
  @transient private lazy val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  @transient private lazy val datumReader = new GenericDatumReader[GenericRecord](schema)

  @transient private lazy val outputStream = new ByteArrayOutputStream()
  @transient private var jsonEncoder: JsonEncoder = null
  val fieldNames: Array[String] = schema.getFields.asScala.map(_.name()).toArray
  @transient private lazy val ziplineSchema = AvroUtils.toZiplineSchema(schema)

  @transient private var binaryEncoder: BinaryEncoder = null
  @transient private var decoder: BinaryDecoder = null

  def encode(valueMap: Map[String, AnyRef]): Array[Byte] = {
    val record = new GenericData.Record(schema)
    schema.getFields.asScala.foreach { field =>
      record.put(field.name(), valueMap.get(field.name()).orNull)
    }
    encodeBinary(record)
  }

  def encode(row: Row): Array[Byte] = {
    val record = new GenericData.Record(schema)
    for (i <- 0 until row.length) {
      record.put(i, row.get(i))
    }
    encodeBinary(record)
  }

  def encodeArray(anyArray: Array[Any]): Array[Byte] = {
    val record = new GenericData.Record(schema)
    for (i <- anyArray.indices) {
      record.put(i, anyArray(i))
    }
    encodeBinary(record)
  }

  def encodeBinary(record: GenericRecord): Array[Byte] = {
    binaryEncoder = EncoderFactory.get.binaryEncoder(outputStream, binaryEncoder)
    encodeRecord(record, binaryEncoder)
  }

  def encodeRecord(record: GenericRecord, reusableEncoder: Encoder): Array[Byte] = {
    outputStream.reset()
    datumWriter.write(record, reusableEncoder)
    reusableEncoder.flush()
    outputStream.flush()
    outputStream.toByteArray
  }

  def encodeJson(record: GenericRecord): String = {
    jsonEncoder = EncoderFactory.get.jsonEncoder(schema, outputStream)
    new String(encodeRecord(record, jsonEncoder))
  }

  def decode(bytes: Array[Byte]): GenericRecord = {
    if (bytes == null) return null
    val inputStream = new SeekableByteArrayInput(bytes)
    inputStream.reset()
    decoder = DecoderFactory.get.directBinaryDecoder(inputStream, decoder)
    datumReader.read(null, decoder)
  }

  def decodeRow(bytes: Array[Byte], millis: Long): ArrayRow =
    new ArrayRow(RowConversions.fromAvroRecord(decode(bytes), ziplineSchema).asInstanceOf[Array[Any]], millis)

  def decodeMap(bytes: Array[Byte]): Map[String, AnyRef] = {
    if (bytes == null) return null
    val output = RowConversions
      .fromAvroRecord(decode(bytes), ziplineSchema)
      .asInstanceOf[Array[Any]]
    fieldNames.zip(output.map(_.asInstanceOf[AnyRef])).toMap
  }
}

// to be consumed by RowAggregator
class ArrayRow(values: Array[Any], millis: Long) extends Row {
  override def get(index: Int): Any = values(index)

  override def ts: Long = millis

  override def isBefore: Boolean = false

  override def mutationTs: Long = millis

  override val length: Int = values.length
}

object AvroCodec {
  // creating new codecs is expensive - so we want to do it once per process
  // but at the same-time we want to avoid contention across threads - hence threadlocal
  private val codecMap: ThreadLocal[mutable.HashMap[String, AvroCodec]] =
    new ThreadLocal[mutable.HashMap[String, AvroCodec]] {
      override def initialValue(): mutable.HashMap[String, AvroCodec] = new mutable.HashMap[String, AvroCodec]()
    }

  def of(schemaStr: String): AvroCodec = codecMap.get().getOrElseUpdate(schemaStr, new AvroCodec(schemaStr))
}

object AvroUtils {
  def toZiplineSchema(schema: Schema): DataType = {
    schema.getType match {
      case Schema.Type.RECORD =>
        StructType(schema.getName,
                   schema.getFields.asScala.toArray.map { field =>
                     StructField(field.name(), toZiplineSchema(field.schema()))
                   })
      case Schema.Type.ARRAY   => ListType(toZiplineSchema(schema.getElementType))
      case Schema.Type.MAP     => MapType(StringType, toZiplineSchema(schema.getValueType))
      case Schema.Type.STRING  => StringType
      case Schema.Type.INT     => IntType
      case Schema.Type.LONG    => LongType
      case Schema.Type.FLOAT   => FloatType
      case Schema.Type.DOUBLE  => DoubleType
      case Schema.Type.BYTES   => BinaryType
      case Schema.Type.BOOLEAN => BooleanType
      case Schema.Type.UNION   => toZiplineSchema(schema.getTypes.get(1)) // unions are only used to represent nullability
      case _                   => throw new UnsupportedOperationException(s"Cannot convert avro type ${schema.getType.toString}")
    }
  }

  def fromZiplineSchema(dataType: DataType): Schema = {
    dataType match {
      case StructType(name, fields) =>
        assert(name != null)
        Schema.createRecord(
          name,
          "", // doc
          "ai.zipline.data", // namespace
          false, // isError
          fields
            .map { ziplineField =>
              val defaultValue: AnyRef = null
              new Field(ziplineField.name,
                        Schema.createUnion(Schema.create(Schema.Type.NULL), fromZiplineSchema(ziplineField.fieldType)),
                        "",
                        defaultValue)
            }
            .toList
            .asJava
        )
      case ListType(elementType) => Schema.createArray(fromZiplineSchema(elementType))
      case MapType(keyType, valueType) => {
        assert(keyType == StringType, s"Avro only supports string keys for a map")
        Schema.createMap(fromZiplineSchema(valueType))
      }
      case StringType  => Schema.create(Schema.Type.STRING)
      case IntType     => Schema.create(Schema.Type.INT)
      case LongType    => Schema.create(Schema.Type.LONG)
      case FloatType   => Schema.create(Schema.Type.FLOAT)
      case DoubleType  => Schema.create(Schema.Type.DOUBLE)
      case BinaryType  => Schema.create(Schema.Type.BYTES)
      case BooleanType => Schema.create(Schema.Type.BOOLEAN)
      case _           => throw new UnsupportedOperationException(s"Cannot convert zipline type $dataType")
    }
  }
}
