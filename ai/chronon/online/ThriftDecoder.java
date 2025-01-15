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

package ai.chronon.online;

import ai.chronon.api.DataType;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ThriftDecoder implements Serializable {

  public final DataType dataType;
  private final SerializableFunction<Object, Object> convertor;

  public ThriftDecoder(DataType d, SerializableFunction<Object, Object> f) {
    dataType = d;
    convertor = f;
  }

  public Object apply(Object o) {
    if (o == null) {
      return null;
    } else {
      return convertor.apply(o);
    }
  }

  public static Map<? extends TFieldIdEnum, FieldMetaData> getMetaDataMap(String clsName)
      throws ClassNotFoundException {
    return FieldMetaData.getStructMetaDataMap(
        new StructMetaData(TType.STRUCT, (Class<? extends TBase>) Class.forName(clsName))
            .structClass);
  }

  // helper methods to build a decoder
  public static Object convertTimestamp(Object other) {
    return new Timestamp(((Instant) other).toEpochMilli());
  }

  public static Object convertDate(Object other) {
    return Date.valueOf((LocalDate) other);
  }

  public static Object convertEnum(Object other) {
    return other.toString();
  }

  public static Object convertBytes(Object other) {
    return ((ByteBuffer) other).array();
  }

  public static Object id(Object other) {
    return other;
  }

  public static SerializableFunction<Object, Object> wrapList(
      SerializableFunction<Object, Object> elemConverter) {
    return o -> {
      List<Object> inp = ((List<Object>) o);
      return inp.stream().map(elemConverter).collect(Collectors.toList());
    };
  }

  public static SerializableFunction<Object, Object> wrapSet(
      SerializableFunction<Object, Object> elemConverter) {
    return o -> {
      Set<Object> inp = ((Set<Object>) o);
      return inp.stream().map(elemConverter).collect(Collectors.toList());
    };
  }

  public static SerializableFunction<Object, Object> wrapMap(
      SerializableFunction<Object, Object> keyConverter,
      SerializableFunction<Object, Object> valueConverter) {
    return o -> {
      Map<Object, Object> inp = ((Map<Object, Object>) o);
      Map<Object, Object> out = new java.util.HashMap<>();
      for (Map.Entry<Object, Object> entry : inp.entrySet()) {
        out.put(keyConverter.apply(entry.getKey()), valueConverter.apply(entry.getValue()));
      }
      return out;
    };
  }

  public static SerializableFunction<Object, Object> coalesce(
      List<SerializableFunction<Object, Object>> funcs) {
    return (Object o) -> {
      Object[] result = new Object[funcs.size()];
      for (int i = 0; i < funcs.size(); i += 1) {
        result[i] = funcs.get(i).apply(o);
      }
      return result;
    };
  }
}
