package ai.chronon.api

import org.apache.thrift.TBase

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.Base64

object HashUtils {

  def md5Base64(string: String): String = {
    md5Base64(string.getBytes)
  }

  def md5Base64(bytes: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(md5Bytes(bytes)).take(10)
  }

  def md5Long(bytes: Array[Byte]): Long = {
    ByteBuffer.wrap(md5Bytes(bytes)).getLong
  }

  def md5Bytes(bytes: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("MD5").digest(bytes)
  }
}
