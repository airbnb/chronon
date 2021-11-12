package ai.zipline.online

import ai.zipline.api.Row
import ai.zipline.api._
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.util.Utf8

import java.nio.ByteBuffer
import java.util
import scala.collection.AbstractIterator
import scala.collection.JavaConverters._

object RowConversions {
  def toAvroRecord(value: Any, dataType: DataType): Any = {
    // But this also has to happen at the recursive depth - data type and schema inside the compositor need to
    Row.to[GenericRecord, ByteBuffer, util.ArrayList[Any], util.Map[Any, Any]](
      value,
      dataType,
      { (data: Iterator[Any], elemDataType: DataType) =>
        val schema = AvroUtils.fromZiplineSchema(elemDataType)
        val record = new GenericData.Record(schema)
        data.zipWithIndex.foreach { case (value, idx) => record.put(idx, value) }
        record
      },
      ByteBuffer.wrap,
      { (elems: Iterator[Any], size: Int) =>
        val result = new util.ArrayList[Any](size)
        elems.foreach(result.add)
        result
      },
      { m: util.Map[Any, Any] => m }
    )
  }

  def fromAvroRecord(value: Any, dataType: DataType): Any = {
    Row.from[GenericRecord, ByteBuffer, GenericData.Array[Any], Utf8](
      value,
      dataType,
      { (record: GenericRecord, fields: Seq[StructField]) =>
        new AbstractIterator[Any]() {
          var idx = 0
          override def next(): Any = {
            val res = record.get(idx)
            idx += 1
            res
          }
          override def hasNext: Boolean = idx < fields.size
        }
      },
      { (byteBuffer: ByteBuffer) => byteBuffer.array() },
      { (garr: GenericData.Array[Any]) =>
        val arr = new Array[Any](garr.size)
        var idx = 0
        while (idx < garr.size()) {
          arr.update(idx, garr.get(idx))
          idx += 1
        }
        arr
      },
      { (avString: Utf8) => avString.toString }
    )
  }
}
