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
