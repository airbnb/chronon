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

package ai.chronon.online.serde

import ai.chronon.api.{DataType, ListType, MapType, StructType}

import java.util
import scala.util.ScalaJavaConversions.{ListOps, MapOps}

/** A named, nested view of a Chronon feature value.
  *
  * Unlike the classic `Map[String, AnyRef]` fetcher response, where struct-typed values are
  * positional `Array[Any]` and the field names live only in the schema, every level of a
  * FeatureRecord is keyed by field name. Struct-typed fields resolve to nested FeatureRecords and
  * lists of structs resolve to `List[FeatureRecord]`, so callers can navigate arbitrarily deep
  * nesting by name instead of by position.
  *
  * `values` carries everything the underlying fetcher response carried, including the
  * `<name>_exception` keys the fetcher uses to report partial failures (see [[errors]]).
  * `schema` is narrowed to the declared fields actually present in `values`.
  */
case class FeatureRecord(schema: StructType, values: Map[String, Any]) {

  private def opt(field: String): Option[Any] = values.get(field).flatMap(Option(_))

  private def asNumber(value: Any, field: String): Number =
    value match {
      case n: Number => n
      case other =>
        throw new IllegalArgumentException(
          s"Field '$field' holds a ${other.getClass.getName}, which is not numeric")
    }

  private def asString(value: Any): String =
    value match {
      // Utf8 and friends implement CharSequence; avro string values are normally already
      // converted to String by AvroConversions.toChrononRow, but derived values may not be.
      case s: String       => s
      case c: CharSequence => c.toString
      case other           => other.toString
    }

  def getString(field: String): String = asString(values(field))
  def getStringOpt(field: String): Option[String] = opt(field).map(asString)

  // Numeric getters widen through Number so that, for example, an IntType field can be read as a
  // Long. Chronon and consumer-side types (notably Thrift i64) do not always line up exactly.
  def getLong(field: String): Long = asNumber(values(field), field).longValue()
  def getLongOpt(field: String): Option[Long] = opt(field).map(asNumber(_, field).longValue())

  def getInt(field: String): Int = asNumber(values(field), field).intValue()
  def getIntOpt(field: String): Option[Int] = opt(field).map(asNumber(_, field).intValue())

  def getDouble(field: String): Double = asNumber(values(field), field).doubleValue()
  def getDoubleOpt(field: String): Option[Double] = opt(field).map(asNumber(_, field).doubleValue())

  def getBoolean(field: String): Boolean = values(field).asInstanceOf[Boolean]
  def getBooleanOpt(field: String): Option[Boolean] = opt(field).map(_.asInstanceOf[Boolean])

  def getStruct(field: String): FeatureRecord = values(field).asInstanceOf[FeatureRecord]
  def getStructOpt(field: String): Option[FeatureRecord] = opt(field).map(_.asInstanceOf[FeatureRecord])

  def getStructList(field: String): List[FeatureRecord] = values(field).asInstanceOf[List[FeatureRecord]]

  def getList[T](field: String): List[T] = values(field).asInstanceOf[List[T]]

  def getMap[V](field: String): Map[String, V] = values(field).asInstanceOf[Map[String, V]]

  /** The `<name>_exception` entries the fetcher uses to report per-groupBy, per-external-part,
    * derivation and codec failures. These are not part of the declared value schema, so they are
    * surfaced here rather than through the typed getters.
    */
  def errors: Map[String, Any] = values.filter(_._1.endsWith("_exception"))

  def hasErrors: Boolean = values.keysIterator.exists(_.endsWith("_exception"))
}

object FeatureRecord {

  /** Build a FeatureRecord out of a flat fetcher response map (e.g. the output of
    * `FetcherBase.fetchGroupBys` / `Fetcher.fetchJoin`) plus the StructType describing it.
    *
    * The top level of `values` is already keyed by field name; this walks it alongside `schema`
    * and recursively attaches names to any nested struct-typed values, which otherwise arrive as
    * bare positional `Array[Any]` (see `AvroConversions.toChrononRow`).
    *
    * Entries with no matching schema field are passed through untouched rather than dropped, so
    * the fetcher's `<name>_exception` keys survive into the structured response.
    */
  def fromValueMap(schema: StructType, values: Map[String, Any]): FeatureRecord = {
    if (values == null) return null
    val declaredTypes: Map[String, DataType] = schema.fields.iterator.map(f => f.name -> f.fieldType).toMap
    val named = values.map {
      case (name, value) =>
        name -> declaredTypes.get(name).map(attachNames(value, _)).getOrElse(value)
    }
    // Narrow the schema to what the response actually carried. The join value schema in
    // particular is a superset: it spans base and derived (and model) fields, while a given
    // response holds only one of those sets.
    val presentFields = schema.fields.filter(f => values.contains(f.name))
    FeatureRecord(StructType(schema.name, presentFields), named)
  }

  private def attachNames(value: Any, dataType: DataType): Any = {
    if (value == null) return null
    dataType match {
      case _ if value.isInstanceOf[FeatureRecord] => value

      case st: StructType => namedStruct(value, st)

      case ListType(elemType) =>
        elementsOf(value).map(elem => attachNames(elem, elemType)).toList

      case MapType(_, valueType) =>
        value match {
          case m: util.Map[_, _] =>
            m.asInstanceOf[util.Map[Any, Any]].toScala.map { case (k, v) => k -> attachNames(v, valueType) }.toMap
          case m: Map[_, _] =>
            m.asInstanceOf[Map[Any, Any]].map { case (k, v) => k -> attachNames(v, valueType) }
          case other =>
            throw new IllegalArgumentException(s"Expected a map value, but got ${other.getClass.getName}")
        }

      case _ => value
    }
  }

  private def namedStruct(value: Any, st: StructType): FeatureRecord =
    value match {
      // The common case: avro-decoded structs are positional, in schema-declared field order.
      case arr: Array[_] =>
        val named = st.fields.iterator.zipWithIndex
          .filter { case (_, idx) => idx < arr.length }
          .map { case (field, idx) => field.name -> attachNames(arr(idx), field.fieldType) }
          .toMap
        FeatureRecord(st, named)
      // Struct values can also arrive keyed by field name, e.g. nested rows surfaced through the
      // Spark SQL transforms used to evaluate derivations. Row.to handles the same two shapes.
      case m: Map[_, _] =>
        fromValueMap(st, m.asInstanceOf[Map[String, Any]])
      case m: util.Map[_, _] =>
        fromValueMap(st, m.asInstanceOf[util.Map[String, Any]].toScala)
      case other =>
        throw new IllegalArgumentException(
          s"Expected a positional array or a field-keyed map for struct '${st.name}', " +
            s"but got ${other.getClass.getName}")
    }

  private def elementsOf(value: Any): Iterator[Any] =
    value match {
      case list: util.List[_]           => list.asInstanceOf[util.List[Any]].toScala.iterator
      case seq: scala.collection.Seq[_] => seq.iterator
      case arr: Array[_]                => arr.iterator
      case other =>
        throw new IllegalArgumentException(s"Expected a list value, but got ${other.getClass.getName}")
    }
}
