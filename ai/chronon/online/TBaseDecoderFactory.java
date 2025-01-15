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

import ai.chronon.api.*;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.TType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Traverse the class structure of .thrift file generated java class (TBase), recursively to produce
// 1. the ChrononSchema for only the relevant fields - based on tokens
// 2. the converter function that takes the class object and normalizes into
//    a valid chronon object.
public class TBaseDecoderFactory implements Serializable {

  private final String rootClass;
  private final Set<String> tokens;
  private final SerializableFunction<String, String> nameTransformer;

  public TBaseDecoderFactory(
      String rootClass, Set<String> tokens, SerializableFunction<String, String> nameTransformer) {
    this.rootClass = rootClass;
    this.tokens = tokens;
    this.nameTransformer = nameTransformer;
  }

  public ThriftDecoder build() throws ClassNotFoundException, NoSuchMethodException {
    Class<? extends TBase> clazz = (Class<? extends TBase>) Class.forName(rootClass);
    return build(new StructMetaData(TType.STRUCT, clazz));
  }

  private static ThriftDecoder field(DataType d, SerializableFunction<Object, Object> f) {
    return new ThriftDecoder(d, f);
  }

  private ThriftDecoder build(FieldValueMetaData meta) throws NoSuchMethodException {
    byte type = meta.type;
    if (type == TType.MAP) {
      MapMetaData mapMeta = (MapMetaData) meta;
      ThriftDecoder key = build(mapMeta.keyMetaData);
      ThriftDecoder value = build(mapMeta.valueMetaData);
      DataType d = new MapType(key.dataType, value.dataType);
      return new ThriftDecoder(d, ThriftDecoder.wrapMap(key::apply, value::apply));
    } else if (type == TType.LIST) {
      ListMetaData listMetaData = (ListMetaData) meta;
      ThriftDecoder elem = build(listMetaData.elemMetaData);
      DataType d = new ListType(elem.dataType);
      return new ThriftDecoder(d, ThriftDecoder.wrapList(elem::apply));
    } else if (type == TType.SET) {
      SetMetaData setMeta = (SetMetaData) meta;
      ThriftDecoder elem = build(setMeta.elemMetaData);
      DataType d = new ListType(elem.dataType);
      return new ThriftDecoder(d, ThriftDecoder.wrapSet(elem::apply));
    } else if (type == TType.ENUM) {
      // Special case for enums, which we always map to String
      return field(StringType$.MODULE$, ThriftDecoder::convertEnum);
    } else if (type == TType.STRING) {
      if (meta.isBinary()) {
        return field(BinaryType$.MODULE$, ThriftDecoder::convertBytes);
      } else {
        return field(StringType$.MODULE$, ThriftDecoder::id);
      }
    } else if (type == TType.DOUBLE) {
      return field(DoubleType$.MODULE$, ThriftDecoder::id);
    } else if (type == TType.I32) {
      return field(IntType$.MODULE$, ThriftDecoder::id);
    } else if (type == TType.I64) {
      return field(LongType$.MODULE$, ThriftDecoder::id);
    } else if (type == TType.BOOL) {
      return field(BooleanType$.MODULE$, ThriftDecoder::id);
    } else if (type == TType.I16) {
      return field(ShortType$.MODULE$, ThriftDecoder::id);
    } else if (type == TType.BYTE) {
      return field(ByteType$.MODULE$, ThriftDecoder::id);
    } else if (type == TType.STRUCT) {
      return buildStructDataType((StructMetaData) meta);
    } else {
      throw new NoSuchMethodException("Unable to handle " + meta.getTypedefName());
    }
  }

  private ThriftDecoder buildStructDataType(StructMetaData meta) throws NoSuchMethodException {
    Map<? extends TFieldIdEnum, FieldMetaData> fieldMeta =
        FieldMetaData.getStructMetaDataMap(meta.structClass);
    List<StructField> fields = new ArrayList<>();
    List<SerializableFunction<Object, Object>> funcs = new ArrayList<>();

    for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> field : fieldMeta.entrySet()) {
      String fieldName = field.getValue().fieldName;
      String transformedName = nameTransformer.apply(fieldName);
      if (tokens != null && !tokens.contains(transformedName)) continue;
      ThriftDecoder f = build(field.getValue().valueMetaData);
      fields.add(new StructField(transformedName, f.dataType));
      short fieldId = field.getKey().getThriftFieldId();
      funcs.add(
          (SerializableFunction<Object, Object>)
              o -> {
                TBase<?, TFieldIdEnum> tb = (TBase<?, TFieldIdEnum>) o;
                return f.apply(tb.getFieldValue(tb.fieldForId(fieldId)));
              });
    }

    SerializableFunction<Object, Object> megaFunc =
        (Object o) -> {
          Object[] result = new Object[funcs.size()];
          for (int i = 0; i < funcs.size(); i += 1) {
            result[i] = funcs.get(i).apply(o);
          }
          return result;
        };

    StructField[] arr = new StructField[fields.size()];
    fields.toArray(arr);
    String[] nameParts = meta.structClass.getName().split("\\.");
    StructType structType = new StructType(nameParts[nameParts.length - 1], arr);
    return new ThriftDecoder(structType, megaFunc);
  }
}
