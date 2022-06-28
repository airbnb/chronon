package ai.chronon.api

import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode, ObjectMapper}
import org.apache.thrift.protocol.{TCompactProtocol, TSimpleJSONProtocol}
import org.apache.thrift.{TBase, TDeserializer, TSerializer}

import java.security.MessageDigest
import java.util.Base64
import scala.io.Source._
import scala.reflect.ClassTag

object ThriftJsonCodec {

  def serializer = new TSerializer(new TSimpleJSONProtocol.Factory())

  def toJsonStr[T <: TBase[_, _]: Manifest](obj: T): String = {
    new String(serializer.serialize(obj))
  }

  def toCompactBase64[T <: TBase[_, _]: Manifest](obj: T): String = {
    val compactSerializer = new TSerializer(new TCompactProtocol.Factory())
    Base64.getEncoder.encodeToString(compactSerializer.serialize(obj))
  }

  def md5Digest[T <: TBase[_, _]: Manifest](obj: T): String = {
    val json = ThriftJsonCodec.toJsonStr(obj)
    val digest = MessageDigest.getInstance("MD5").digest(json.getBytes())
    Base64.getEncoder.encodeToString(digest).take(10)
  }
  def fromCompactBase64[T <: TBase[_, _]: Manifest](base: T, base64: String): T = {
    val compactDeserializer = new TDeserializer(new TCompactProtocol.Factory())
    val bytes = Base64.getDecoder.decode(base64)
    try {
      compactDeserializer.deserialize(base, bytes)
      base
    } catch {
      case e: Exception => {
        println("Failed to deserialize using compact protocol, trying Json.")
        fromJsonStr(new String(bytes), check = false, base.getClass)
      }
    }
  }

  def fromJsonStr[T <: TBase[_, _]: Manifest](jsonStr: String, check: Boolean = true, clazz: Class[_ <: T]): T = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    val obj: T = mapper.readValue(jsonStr, clazz)
    if (check) {
      val inputNode: JsonNode = mapper.readTree(jsonStr)
      val reSerializedInput: JsonNode = mapper.readTree(toJsonStr(obj))
      assert(
        inputNode.equals(reSerializedInput),
        message = s"""
     Parsed Json object isn't reversible.
     Original JSON String:  $jsonStr
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
