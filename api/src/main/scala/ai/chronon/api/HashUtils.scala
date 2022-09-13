package ai.chronon.api

import org.apache.thrift.TBase

import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.Base64

object HashUtils {

  def stringToString(string: String): String = {
    bytesToString(string.getBytes)
  }

  def bytesToString(bytes: Array[Byte]): String = {
    Base64.getEncoder.encodeToString(bytesToBytes(bytes)).take(10)
  }

  def bytesToLong(bytes: Array[Byte]): Long = {
    ByteBuffer.wrap(bytesToBytes(bytes)).getLong
  }

  def bytesToBytes(bytes: Array[Byte]): Array[Byte] = {
    MessageDigest.getInstance("MD5").digest(bytes)
  }
}
