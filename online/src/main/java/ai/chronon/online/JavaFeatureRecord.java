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

import ai.chronon.online.serde.FeatureRecord;
import scala.util.ScalaVersionSpecificCollectionsConverter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Java-friendly view of a Scala {@link FeatureRecord}. Fields are accessed by name at every
 * level of nesting instead of by position: struct-typed fields resolve to nested
 * JavaFeatureRecords, and lists of structs resolve to {@code List<JavaFeatureRecord>}.
 *
 * <p>{@code values} carries everything the underlying fetcher response carried, including the
 * {@code <name>_exception} keys the fetcher uses to report partial failures (see {@link #errors}).
 */
public class JavaFeatureRecord {
    private static final String EXCEPTION_SUFFIX = "_exception";

    public final Map<String, Object> values;

    public JavaFeatureRecord(FeatureRecord scalaRecord) {
        this.values = scalaRecord == null ? null : convertValues(scalaRecord);
    }

    private static Map<String, Object> convertValues(FeatureRecord scalaRecord) {
        Map<String, Object> scalaAsJavaMap =
                ScalaVersionSpecificCollectionsConverter.convertScalaMapToJava(scalaRecord.values());
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : scalaAsJavaMap.entrySet()) {
            result.put(entry.getKey(), convertValue(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static Object convertValue(Object value) {
        if (value instanceof FeatureRecord) {
            return new JavaFeatureRecord((FeatureRecord) value);
        } else if (value instanceof scala.collection.immutable.List) {
            List<Object> scalaAsJavaList = ScalaVersionSpecificCollectionsConverter
                    .convertScalaListToJava((scala.collection.immutable.List<Object>) value);
            List<Object> converted = new ArrayList<>(scalaAsJavaList.size());
            for (Object item : scalaAsJavaList) {
                converted.add(convertValue(item));
            }
            return converted;
        } else if (value instanceof scala.collection.Map) {
            Map<Object, Object> scalaAsJavaMap = ScalaVersionSpecificCollectionsConverter
                    .convertScalaMapToJava((scala.collection.immutable.Map<Object, Object>) value);
            Map<Object, Object> converted = new HashMap<>();
            for (Map.Entry<Object, Object> entry : scalaAsJavaMap.entrySet()) {
                converted.put(entry.getKey(), convertValue(entry.getValue()));
            }
            return converted;
        } else {
            return value;
        }
    }

    private Number asNumber(String field) {
        Object value = values.get(field);
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException(
                    "Field '" + field + "' holds a " + value.getClass().getName() + ", which is not numeric");
        }
        return (Number) value;
    }

    public String getString(String field) {
        Object value = values.get(field);
        if (value == null) {
            return null;
        }
        return value instanceof String ? (String) value : value.toString();
    }

    public Optional<String> getStringOpt(String field) {
        return Optional.ofNullable(getString(field));
    }

    // Numeric getters widen through Number so that, for example, an IntType field can be read as a
    // Long. Chronon and consumer-side types (notably Thrift i64) do not always line up exactly.
    public Long getLong(String field) {
        Number n = asNumber(field);
        return n == null ? null : n.longValue();
    }

    public Optional<Long> getLongOpt(String field) {
        return Optional.ofNullable(getLong(field));
    }

    public Integer getInt(String field) {
        Number n = asNumber(field);
        return n == null ? null : n.intValue();
    }

    public Optional<Integer> getIntOpt(String field) {
        return Optional.ofNullable(getInt(field));
    }

    public Double getDouble(String field) {
        Number n = asNumber(field);
        return n == null ? null : n.doubleValue();
    }

    public Optional<Double> getDoubleOpt(String field) {
        return Optional.ofNullable(getDouble(field));
    }

    public Boolean getBoolean(String field) {
        return (Boolean) values.get(field);
    }

    public Optional<Boolean> getBooleanOpt(String field) {
        return Optional.ofNullable(getBoolean(field));
    }

    public JavaFeatureRecord getStruct(String field) {
        return (JavaFeatureRecord) values.get(field);
    }

    public Optional<JavaFeatureRecord> getStructOpt(String field) {
        return Optional.ofNullable(getStruct(field));
    }

    @SuppressWarnings("unchecked")
    public List<JavaFeatureRecord> getStructList(String field) {
        return (List<JavaFeatureRecord>) values.get(field);
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String field) {
        return (List<T>) values.get(field);
    }

    @SuppressWarnings("unchecked")
    public <V> Map<String, V> getMap(String field) {
        return (Map<String, V>) values.get(field);
    }

    /**
     * The {@code <name>_exception} entries the fetcher uses to report per-groupBy,
     * per-external-part, derivation and codec failures. These are not part of the declared value
     * schema, so they are surfaced here rather than through the typed getters.
     */
    public Map<String, Object> errors() {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getKey().endsWith(EXCEPTION_SUFFIX)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public boolean hasErrors() {
        for (String key : values.keySet()) {
            if (key.endsWith(EXCEPTION_SUFFIX)) {
                return true;
            }
        }
        return false;
    }
}
