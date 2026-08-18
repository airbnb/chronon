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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.collection.mutable
import scala.util.ScalaJavaConversions.IteratorOps

object SparkInternalRowConversions {
  // the identity function
  private def id(x: Any): Any = x

  // Coerce a numeric value to the exact JVM type Catalyst expects for a given numeric DataType.
  // JSON deserializers (e.g. Jackson) infer a number's type from its literal - `5` becomes an Integer,
  // `1.5` a Double - regardless of the target schema. Spark's typed row accessors (getLong, getInt, ...)
  // unbox to a fixed primitive, so an Integer sitting in a LongType slot throws a ClassCastException at
  // read time. Normalising every java.lang.Number to the schema's type here makes the conversion robust
  // to that mismatch; values that are already the right type are coerced idempotently.
  private def numericConverter(coerce: java.lang.Number => Any): Any => Any = {
    case n: java.lang.Number => coerce(n)
    case other               => other
  }

  // recursively convert sparks byte array based internal row to chronon's fetcher result type (map[string, any])
  // The purpose of this class is to be used on fetcher output in a fetching context
  // we take a data type and build a function that operates on actual value
  // we want to build functions where we only branch at construction time, but not at function execution time.
  def from(dataType: types.DataType, structToMap: Boolean = true): Any => Any = {
    val unguardedFunc: Any => Any = dataType match {
      case types.MapType(keyType, valueType, _) =>
        val keyConverter = from(keyType, structToMap)
        val valueConverter = from(valueType, structToMap)

        def mapConverter(x: Any): Any = {
          val mapData = x.asInstanceOf[MapData]
          val result = new util.HashMap[Any, Any]()
          val size = mapData.numElements()
          val keys = mapData.keyArray()
          val values = mapData.valueArray()
          var idx = 0
          while (idx < size) {
            result.put(keyConverter(keys.get(idx, keyType)), valueConverter(values.get(idx, valueType)))
            idx += 1
          }
          result
        }

        mapConverter
      case types.ArrayType(elementType, _) =>
        val elementConverter = from(elementType, structToMap)

        def arrayConverter(x: Any): Any = {
          val arrayData = x.asInstanceOf[ArrayData]
          val size = arrayData.numElements()
          val result = new util.ArrayList[Any](size)
          var idx = 0
          while (idx < size) {
            result.add(elementConverter(arrayData.get(idx, elementType)))
            idx += 1
          }
          result
        }

        arrayConverter
      case types.StructType(fields) =>
        val funcs = fields.map { _.dataType }.map { from(_, structToMap) }
        val types = fields.map { _.dataType }
        val names = fields.map { _.name }
        val size = funcs.length

        def structToMapConverter(x: Any): Any = {
          val internalRow = x.asInstanceOf[InternalRow]
          val result = new mutable.HashMap[Any, Any]()
          var idx = 0
          while (idx < size) {
            val value = internalRow.get(idx, types(idx))
            result.put(names(idx), funcs(idx)(value))
            idx += 1
          }
          result.toMap
        }

        def structToArrayConverter(x: Any): Any = {
          val internalRow = x.asInstanceOf[InternalRow]
          val result = new Array[Any](size)
          var idx = 0
          while (idx < size) {
            val value = internalRow.get(idx, types(idx))
            result.update(idx, funcs(idx)(value))
            idx += 1
          }
          result
        }

        if (structToMap) structToMapConverter else structToArrayConverter
      case types.StringType =>
        def stringConvertor(x: Any): Any = x.asInstanceOf[UTF8String].toString

        stringConvertor
      case _ => id
    }
    def guardedFunc(x: Any): Any = if (x == null) x else unguardedFunc(x)
    guardedFunc
  }

  // recursively convert fetcher result type - map[string, any] to internalRow.
  // The purpose of this class is to be used on fetcher output in a fetching context
  // we take a data type and build a function that operates on actual value
  // we want to build functions where we only branch at construction time, but not at function execution time.
  def to(dataType: types.DataType, structToMap: Boolean): Any => Any = {
    val unguardedFunc: Any => Any = dataType match {
      case types.MapType(keyType, valueType, _) =>
        val keyConverter = to(keyType, structToMap)
        val valueConverter = to(valueType, structToMap)

        def mapConverter(x: Any): Any = {
          val mapData = x.asInstanceOf[util.HashMap[Any, Any]]
          val keyArray: ArrayData = new GenericArrayData(
            mapData.entrySet().iterator().toScala.map(_.getKey).map(keyConverter).toArray)
          val valueArray: ArrayData = new GenericArrayData(
            mapData.entrySet().iterator().toScala.map(_.getValue).map(valueConverter).toArray)
          new ArrayBasedMapData(keyArray, valueArray)
        }

        mapConverter
      case types.ArrayType(elementType, _) =>
        val elementConverter = to(elementType, structToMap)

        def arrayConverter(x: Any): Any = {
          val arrayData = x match {
            case listValue: util.ArrayList[_] => listValue.iterator().toScala.map(elementConverter).toArray
            case arrayValue: Array[_]         => arrayValue.iterator.map(elementConverter).toArray
          }
          new GenericArrayData(arrayData)
        }

        arrayConverter
      case types.StructType(fields) =>
        val funcs = fields.map { _.dataType }.map { to(_, structToMap) }
        val names = fields.map { _.name }

        def mapConverter(structMap: Any): Any = {
          val getValue: Any => Any = structMap match {
            case scalaMap: Map[_, _]          => (name: Any) => scalaMap.asInstanceOf[Map[Any, Any]].get(name).orNull
            case javaMap: java.util.Map[_, _] => (name: Any) => javaMap.asInstanceOf[java.util.Map[Any, Any]].get(name)
            case _                            => throw new ClassCastException(s"Cannot cast ${structMap.getClass.getName} to Map")
          }

          val valueArr = names.iterator
            .zip(funcs.iterator)
            .map {
              case (name, func) =>
                val value = getValue(name)
                if (value != null) func(value) else null
            }
            .toArray
          new GenericInternalRow(valueArr)
        }

        def arrayConverter(structData: Any): Any = {
          val valueArr = structData match {
            case listValue: java.util.List[_] =>
              listValue
                .iterator()
                .toScala
                .zip(funcs.iterator)
                .map { case (value, func) => if (value != null) func(value) else null }
                .toArray
            case arrayValue: Array[_] =>
              arrayValue.iterator
                .zip(funcs.iterator)
                .map { case (value, func) => if (value != null) func(value) else null }
                .toArray
            case javaMap: java.util.Map[_, _] =>
              val map = javaMap.asInstanceOf[java.util.Map[Any, Any]]
              names.iterator
                .zip(funcs.iterator)
                .map {
                  case (name, func) =>
                    val value = map.get(name)
                    if (value != null) func(value) else null
                }
                .toArray
            case scalaMap: Map[_, _] =>
              val map = scalaMap.asInstanceOf[Map[Any, Any]]
              names.iterator
                .zip(funcs.iterator)
                .map {
                  case (name, func) =>
                    val value = map.get(name).orNull
                    if (value != null) func(value) else null
                }
                .toArray
            case _ => throw new ClassCastException(s"Cannot cast ${structData.getClass.getName} to Array, List, or Map")
          }
          new GenericInternalRow(valueArr)
        }

        if (structToMap) mapConverter else arrayConverter
      case types.ByteType    => numericConverter(_.byteValue())
      case types.ShortType   => numericConverter(_.shortValue())
      case types.IntegerType => numericConverter(_.intValue())
      case types.LongType    => numericConverter(_.longValue())
      case types.FloatType   => numericConverter(_.floatValue())
      case types.DoubleType  => numericConverter(_.doubleValue())
      case types.StringType =>
        def stringConvertor(x: Any): Any = { UTF8String.fromString(x.asInstanceOf[String]) }

        stringConvertor
      case _ => id
    }
    def guardedFunc(x: Any): Any = if (x == null) x else unguardedFunc(x)
    guardedFunc
  }
}
