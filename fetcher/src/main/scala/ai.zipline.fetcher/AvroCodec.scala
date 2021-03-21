package ai.zipline.fetcher

import java.io.ByteArrayOutputStream

import ai.zipline.aggregator.base.{
  BinaryType,
  BooleanType,
  DataType,
  DoubleType,
  FloatType,
  IntType,
  ListType,
  LongType,
  MapType,
  StringType,
  StructField,
  StructType
}
import ai.zipline.aggregator.row.Row
import ai.zipline.api.Constants
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable

class AvroCodec(schema: Schema) {
  private val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  private val datumReader = new GenericDatumReader[GenericRecord](schema)
  private var decoder: BinaryDecoder = null
  private var encoder: BinaryEncoder = null

  private val outputStream = new ByteArrayOutputStream()
  private val tsIndex: Int = schema.getFields.asScala.map(_.name()).indexOf(Constants.TimeColumn)
  private val length: Int = schema.getFields.size()

  def encode(row: Row): Array[Byte] = {
    val record = new GenericData.Record(schema)
    for (i <- 0 until row.length) {
      record.put(i, row.get(i))
    }
    outputStream.reset()
    encoder = EncoderFactory.get.binaryEncoder(outputStream, encoder)
    datumWriter.write(record, encoder)
    outputStream.toByteArray
  }

  protected def decode(bytes: Array[Byte]): Seq[Row] = {
    val inputStream = new SeekableByteArrayInput(bytes)
    decoder = DecoderFactory.get.binaryDecoder(inputStream, decoder)
    val records = mutable.ListBuffer.empty[GenericRecordRow]
    while (!decoder.isEnd) {
      val record = datumReader.read(null, decoder)
      records.append(new GenericRecordRow(record, tsIndex, length))
    }
    records
  }
}

// to be consumed by RowAggregator
class GenericRecordRow(record: GenericRecord, tsIndex: Int, fieldCount: Int) extends Row {
  override def get(index: Int): Any = record.get(index)

  override def ts: Long = tsIndex

  override val length: Int = fieldCount
}

object AvroCodec {
  // creating new codecs is expensive - so we want to do it once per process
  // but at the same-time we want to avoid contention across threads - hence threadlocal
  private val codecMap: ThreadLocal[mutable.HashMap[Schema, AvroCodec]] =
    new ThreadLocal[mutable.HashMap[Schema, AvroCodec]] {
      override def initialValue(): mutable.HashMap[Schema, AvroCodec] = new mutable.HashMap[Schema, AvroCodec]()
    }

  def of(schema: Schema): AvroCodec = codecMap.get().getOrElseUpdate(schema, new AvroCodec(schema))

  def toZiplineSchema(schema: Schema): DataType = {
    schema.getType match {
      case Schema.Type.RECORD =>
        StructType(schema.getName,
                   schema.getFields.asScala.map { field =>
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
      case _                   => throw new UnsupportedOperationException(s"Cannot convert avro type ${schema.getType.toString}")
    }
  }

  def fromZiplineSchema(dataType: DataType): Schema = {
    dataType match {
      case StructType(name, fields) =>
        Schema.createRecord(
          name,
          "Zipline record schema", // doc
          "ai.zipline.data", // nameSpace
          false, // isError
          fields.map { ziplineField =>
            val defaultValue: AnyRef = null
            new Field(ziplineField.name, fromZiplineSchema(ziplineField.fieldType), "", defaultValue)
          }.asJava
        )
      case ListType(elementType) => Schema.createArray(fromZiplineSchema(elementType))
      case MapType(keyType, valueType) => {
        assert(keyType == StringType, s"Avro only supports string keys for a map")
        Schema.createMap(fromZiplineSchema(valueType))
      }
      case StringType  => Schema.create(Schema.Type.STRING)
      case IntType     => Schema.create(Schema.Type.INT)
      case FloatType   => Schema.create(Schema.Type.LONG)
      case FloatType   => Schema.create(Schema.Type.FLOAT)
      case DoubleType  => Schema.create(Schema.Type.DOUBLE)
      case BinaryType  => Schema.create(Schema.Type.BYTES)
      case BooleanType => Schema.create(Schema.Type.BOOLEAN)
      case _           => throw new UnsupportedOperationException(s"Cannot convert zipline type $dataType")
    }
  }
}
