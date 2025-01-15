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

package ai.chronon.online

import ai.chronon.api.{DataType, Row}
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io._

import java.io.ByteArrayOutputStream
import scala.collection.mutable
import scala.util.ScalaJavaConversions.ListOps

class AvroCodec(val schemaStr: String) extends Serializable {
  @transient private lazy val parser = new Schema.Parser()
  @transient lazy val schema = parser.parse(schemaStr)

  // we reuse a lot of intermediate
  // lazy vals so that spark can serialize & ship the codec to executors
  @transient private lazy val datumWriter = new GenericDatumWriter[GenericRecord](schema)
  @transient private lazy val datumReader = new GenericDatumReader[GenericRecord](schema)

  @transient private lazy val outputStream = new ByteArrayOutputStream()
  @transient private var jsonEncoder: JsonEncoder = null
  val fieldNames: Array[String] = schema.getFields.toScala.map(_.name()).toArray
  @transient lazy val chrononSchema: DataType = AvroConversions.toChrononSchema(schema)

  @transient private var binaryEncoder: BinaryEncoder = null
  @transient private var decoder: BinaryDecoder = null
  @transient lazy val schemaElems: Array[Field] = schema.getFields.toScala.toArray
  def encode(valueMap: Map[String, AnyRef]): Array[Byte] = {
    val record = new GenericData.Record(schema)
    schemaElems.foreach { field =>
      record.put(field.name(), AvroConversions.toAvroValue(valueMap.get(field.name()).orNull, field.schema()))
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

  def decodeRow(bytes: Array[Byte]): Array[Any] =
    AvroConversions.toChrononRow(decode(bytes), chrononSchema).asInstanceOf[Array[Any]]

  def decodeRow(bytes: Array[Byte], millis: Long, mutation: Boolean = false): ArrayRow =
    new ArrayRow(decodeRow(bytes), millis, mutation)

  def decodeMap(bytes: Array[Byte]): Map[String, AnyRef] = {
    if (bytes == null) return null
    val output = AvroConversions
      .toChrononRow(decode(bytes), chrononSchema)
      .asInstanceOf[Array[Any]]
    fieldNames.iterator.zip(output.iterator.map(_.asInstanceOf[AnyRef])).toMap
  }
}

/**
  * Consumed by row aggregator after decoding.
  * Mutations follow the same schema as input for value indices. However there are two main differences.
  *  * ts and reversal columns are required for computation
  *  * Mutation ts takes on the role of ts.
  * Since the schema is the same with the sole difference of the added columns, we add these columns on the tail
  * of the Array and extract them accordingly.
  * i.e. for mutations: reversal index = ArrayRow.length - (Constants.MutationAvroColumns.length - (index of reversal in Constants.MutationAvroColumns)
  *
  */
class ArrayRow(values: Array[Any], millis: Long, mutation: Boolean = false) extends Row {
  override def get(index: Int): Any = values(index)

  override def ts: Long = if (mutation) values(values.length - 2).asInstanceOf[Long] else millis

  override def isBefore: Boolean = if (mutation) values(values.length - 1).asInstanceOf[Boolean] else false

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
