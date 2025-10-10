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

import ai.chronon.api
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._

import java.util
import scala.collection.mutable
import scala.collection.Seq

// wrapper class of spark ai.chronon.aggregator.row that the RowAggregator can work with
// no copies are happening here, but we wrap the ai.chronon.aggregator.row with an additional class
class RowWrapper(val row: Row, val tsIndex: Int, val reversalIndex: Int = -1, val mutationTsIndex: Int = -1)
    extends api.Row {

  override def get(index: Int): Any = row.get(index)

  override val length: Int = row.size

  override def ts: Long = {
    require(
      tsIndex > -1,
      "Requested timestamp from a ai.chronon.api.Row with missing `ts` column"
    )
    getAs[Long](tsIndex)
  }

  override def isBefore: Boolean = {
    require(reversalIndex > -1, "Requested is_before from a ai.chronon.api.Row with missing `reversal` column")
    getAs[Boolean](reversalIndex)
  }

  override def mutationTs: Long = {
    require(mutationTsIndex > -1,
            "Requested mutation timestamp from a ai.chronon.api.Row with missing `mutation_ts` column")
    getAs[Long](mutationTsIndex)
  }
}

object SparkConversions {

  def toChrononRow(row: Row, tsIndex: Int, reversalIndex: Int = -1, mutationTsIndex: Int = -1): RowWrapper =
    new RowWrapper(row, tsIndex, reversalIndex, mutationTsIndex)

  def toChrononType(name: String, dataType: DataType): api.DataType = {
    val typeName = name.capitalize
    dataType match {
      case IntegerType               => api.IntType
      case LongType                  => api.LongType
      case ShortType                 => api.ShortType
      case ByteType                  => api.ByteType
      case FloatType                 => api.FloatType
      case DoubleType                => api.DoubleType
      case StringType                => api.StringType
      case BinaryType                => api.BinaryType
      case BooleanType               => api.BooleanType
      case DateType                  => api.DateType
      case TimestampType             => api.TimestampType
      case ArrayType(elementType, _) => api.ListType(toChrononType(s"${typeName}Element", elementType))
      case MapType(keyType, valueType, _) =>
        api.MapType(toChrononType(s"${typeName}Key", keyType), toChrononType(s"${typeName}Value", valueType))
      case StructType(fields) =>
        api.StructType(
          s"${typeName}Struct",
          fields.map { field =>
            api.StructField(field.name, toChrononType(field.name, field.dataType))
          }
        )
      case other => api.UnknownType(other)
    }
  }

  def fromChrononType(zType: api.DataType): DataType =
    zType match {
      case api.IntType               => IntegerType
      case api.LongType              => LongType
      case api.ShortType             => ShortType
      case api.ByteType              => ByteType
      case api.FloatType             => FloatType
      case api.DoubleType            => DoubleType
      case api.StringType            => StringType
      case api.BinaryType            => BinaryType
      case api.BooleanType           => BooleanType
      case api.DateType              => DateType
      case api.TimestampType         => TimestampType
      case api.ListType(elementType) => ArrayType(fromChrononType(elementType))
      case api.MapType(keyType, valueType) =>
        MapType(fromChrononType(keyType), fromChrononType(valueType))
      case api.StructType(_, fields) =>
        StructType(fields.map { field =>
          StructField(field.name, fromChrononType(field.fieldType))
        })
      case api.UnknownType(other) => other.asInstanceOf[DataType]
    }

  def toChrononSchema(schema: StructType): Array[(String, api.DataType)] =
    schema.fields.map { field =>
      (field.name, toChrononType(field.name, field.dataType))
    }

  def toChrononStruct(name: String, schema: StructType): api.StructType = {
    api.StructType(
      name,
      schema.map { field =>
        api.StructField(field.name, toChrononType(field.name, field.dataType))
      }.toArray
    )
  }

  def fromChrononSchema(schema: Seq[(String, api.DataType)]): StructType =
    StructType(schema.map {
      case (name, zType) =>
        StructField(name, fromChrononType(zType))
    }.toSeq)

  def fromChrononSchema(schema: api.StructType): StructType =
    StructType(schema.fields.map {
      case api.StructField(name, zType) =>
        StructField(name, fromChrononType(zType))
    })

  def toSparkRowSparkType(value: Any, sparkType: StructType, extraneousRecord: Any => Array[Any] = null): Any = {
    toSparkRow(value, api.StructType.from("record", toChrononSchema(sparkType)), extraneousRecord)
  }

  def toSparkRow(value: Any, dataType: api.DataType, extraneousRecord: Any => Array[Any] = null): Any = {
    api.Row.to[GenericRow, Array[Byte], Array[Any], mutable.Map[Any, Any], StructType](
      value,
      dataType,
      { (data: Iterator[Any], _, _) => new GenericRow(data.toArray) },
      { bytes: Array[Byte] => bytes },
      { (elems: Iterator[Any], size: Int) =>
        val result = new Array[Any](size)
        elems.zipWithIndex.foreach { case (elem, idx) => result.update(idx, elem) }
        result
      },
      { m: util.Map[Any, Any] =>
        val result = new mutable.HashMap[Any, Any]
        val it = m.entrySet().iterator()
        while (it.hasNext) {
          val entry = it.next()
          result.update(entry.getKey, entry.getValue)
        }
        result
      },
      extraneousRecord
    )
  }

  /**
    * Converts a single Spark column value to Chronon normalized IR format.
    *
    * This is the inverse of toSparkRow() - used when reading pre-computed IR values
    * from Spark DataFrames. Each IR column in the DataFrame is converted based on its
    * Chronon IR type.
    *
    * Examples:
    * - Count IR: Long → Long (pass-through, primitives stay primitives)
    * - Sum IR: Double → Double (pass-through)
    * - Average IR: Spark Row(sum, count) → Array[Any](sum, count)
    * - UniqueCount IR: Spark Array[T] → java.util.ArrayList[T]
    * - Histogram IR: Spark Map[K,V] → java.util.HashMap[K,V]
    * - ApproxPercentile IR: Array[Byte] → Array[Byte] (pass-through for binary)
    *
    * @param sparkValue The value from a Spark DataFrame column
    * @param irType The Chronon IR type for this column (from RowAggregator.incrementalOutputSchema)
    * @return Normalized IR value ready for denormalize()
    */
  def fromSparkValue(sparkValue: Any, irType: api.DataType): Any = {
    if (sparkValue == null) return null

    (sparkValue, irType) match {
      // Primitives - pass through (Count, Sum, Min, Max, Binary sketches)
      case (v,
            api.IntType | api.LongType | api.ShortType | api.ByteType | api.FloatType | api.DoubleType |
            api.StringType | api.BooleanType | api.BinaryType) =>
        v

      // Spark Row → Array[Any] (Average, Variance, Skew, Kurtosis, FirstK/LastK)
      case (row: Row, api.StructType(_, fields)) =>
        val arr = new Array[Any](fields.length)
        fields.zipWithIndex.foreach {
          case (field, idx) =>
            arr(idx) = fromSparkValue(row.get(idx), field.fieldType)
        }
        arr

      // Spark mutable.WrappedArray → util.ArrayList (UniqueCount, TopK, BottomK)
      case (arr: mutable.WrappedArray[_], api.ListType(elementType)) =>
        val result = new util.ArrayList[Any](arr.length)
        arr.foreach { elem =>
          result.add(fromSparkValue(elem, elementType))
        }
        result

      // Spark native Array → util.ArrayList (alternative array representation)
      case (arr: Array[_], api.ListType(elementType)) =>
        val result = new util.ArrayList[Any](arr.length)
        arr.foreach { elem =>
          result.add(fromSparkValue(elem, elementType))
        }
        result

      // Spark scala.collection.Map → util.HashMap (Histogram)
      case (map: scala.collection.Map[_, _], api.MapType(keyType, valueType)) =>
        val result = new util.HashMap[Any, Any]()
        map.foreach {
          case (k, v) =>
            result.put(
              fromSparkValue(k, keyType),
              fromSparkValue(v, valueType)
            )
        }
        result

      case (value, tpe) =>
        throw new IllegalArgumentException(
          s"Cannot convert Spark value $value (${value.getClass.getSimpleName}) " +
            s"to Chronon IR type $tpe"
        )
    }
  }
}
