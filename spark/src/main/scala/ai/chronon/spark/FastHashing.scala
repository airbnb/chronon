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

package ai.chronon.spark

import org.slf4j.{Logger, LoggerFactory}
import ai.chronon.spark.Extensions._
import com.google.common.hash.{Hasher, Hashing}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.nio.charset.Charset

// TODO: drop data and hashInt, iff we see OOMs on executors for small IRs and large keys
// That is the only case where key size would be a problem
case class KeyWithHash(data: Array[Any], hash: Array[Byte], hashInt: Int) extends Serializable {
  // 16-byte hash from murmur_128
  // P(one collision) ~ 10^-6 when key count ~ 2.6×10^16
  // in-comparison with a 8-byte hash (long)
  // P(one collision) ~ 0.5 when key count ~ 5B ()
  // see: https://en.wikipedia.org/wiki/Birthday_attack
  // ergo, we can't use a long and resort to byte array.
  override def equals(obj: Any): Boolean =
    java.util.Arrays.equals(hash, obj.asInstanceOf[KeyWithHash].hash)

  // used by HashPartitioner to Shuffle data correctly
  // the default Array[] hashcode is dependent on content - so it is useless
  override def hashCode(): Int = hashInt
}

object FastHashing {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)
  // function to generate a fast-ish hasher
  // the approach tries to accumulate several tiny closures to compute the final hash
  def generateKeyBuilder(keys: Array[String], schema: StructType): Row => KeyWithHash = {
    val keySchema = StructType(schema.filter { sf => keys.contains(sf.name) })
    logger.info(s"Generating key builder over keys:\n${keySchema.pretty}\n")
    val keyIndices: Array[Int] = keys.map(schema.fieldIndex)
    // the hash function generation won't be in the hot path - so its okay to
    val hashFunctions: Array[(Hasher, Row) => Unit] = keys.zip(keyIndices).map {
      case (key, index) =>
        val typ = schema.fields(index).dataType
        val hashFunction: (Hasher, Row) => Unit = typ match {
          case IntegerType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putInt(row.getAs[Int](index))
          }
          case LongType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putLong(row.getAs[Long](index))
          }
          case ShortType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putShort(row.getAs[Short](index))
          }
          case StringType => {
            case (hasher: Hasher, row: Row) =>
              // putString has changed between guava versions and makes Chronon less friendly when
              // dealing with build conflicts, so we instead use putBytes
              hasher.putBytes(row.getAs[String](index).getBytes(Utf8))
          }
          case BinaryType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putBytes(row.getAs[Array[Byte]](index))
          }
          case BooleanType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putBoolean(row.getAs[Boolean](index))
          }
          case FloatType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putFloat(row.getAs[Float](index))
          }
          case DoubleType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putDouble(row.getAs[Double](index))
          }
          case DateType => {
            case (hasher: Hasher, row: Row) =>
              // Date is internally represented in spark as a integer representing the
              // number of days since 1970-01-01
              hasher.putInt(row.getAs[Int](index))
          }
          case TimestampType => {
            case (hasher: Hasher, row: Row) =>
              hasher.putLong(row.getAs[Long](index))
          }
          case _ =>
            throw new UnsupportedOperationException(
              s"Hashing unsupported for key column: $key of type: $typ"
            )
        }
        hashFunction
    }

    { row: Row =>
      val hasher = Hashing.murmur3_128().newHasher()
      for (i <- hashFunctions.indices) {
        if (!row.isNullAt(keyIndices(i))) {
          hashFunctions(i)(hasher, row)
        }
      }
      val hashCode = hasher.hash()
      KeyWithHash(keyIndices.map(row.get), hashCode.asBytes(), hashCode.asInt())
    }
  }

  private val Utf8 = Charset.forName("UTF-8")
}
