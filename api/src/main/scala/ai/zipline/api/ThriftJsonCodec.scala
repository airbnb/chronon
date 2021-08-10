package ai.zipline.api

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.thrift.{TBase, TSerializer}
import org.apache.thrift.protocol.TSimpleJSONProtocol

import scala.io.Source._
import scala.reflect.ClassTag

object ThriftJsonCodec {

  def serializer = new TSerializer(new TSimpleJSONProtocol.Factory())

  def toJsonStr[T <: TBase[_, _]: Manifest](obj: T): String = {
    new String(serializer.serialize(obj))
  }

  def fromJsonStr[T <: TBase[_, _]: Manifest](jsonStr: String, check: Boolean = true, clazz: Class[T]): T = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val obj: T = mapper.readValue(jsonStr, clazz)
    if (check) {
      val whiteSpaceNormalizedInput = jsonStr.replaceAll("\\s", "")
      val reSerializedInput = toJsonStr(obj).replaceAll("\\s", "")
      assert(
        whiteSpaceNormalizedInput == reSerializedInput,
        message = s"""
     Parsed Json object isn't reversible.
     Original JSON String:  $whiteSpaceNormalizedInput
     JSON produced by serializing object: $reSerializedInput"""
      )
    }
    obj
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest: ClassTag](fileName: String, check: Boolean): T = {
    val src = fromFile(fileName)
    val jsonStr =
      try src.mkString
      finally src.close()
    val obj: T = fromJsonStr[T](jsonStr, check, clazz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])
    obj
  }
}
