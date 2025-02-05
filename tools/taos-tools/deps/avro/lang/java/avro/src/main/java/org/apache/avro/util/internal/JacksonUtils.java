/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.util.internal;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;

public class JacksonUtils {

  private JacksonUtils() {
  }

  public static JsonNode toJsonNode(Object datum) {
    if (datum == null) {
      return null;
    }
    try {
      TokenBuffer generator = new TokenBuffer(new ObjectMapper(), false);
      toJson(datum, generator);
      return new ObjectMapper().readTree(generator.asParser());
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @SuppressWarnings(value = "unchecked")
  static void toJson(Object datum, JsonGenerator generator) throws IOException {
    if (datum == JsonProperties.NULL_VALUE) { // null
      generator.writeNull();
    } else if (datum instanceof Map) { // record, map
      generator.writeStartObject();
      for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) datum).entrySet()) {
        generator.writeFieldName(entry.getKey().toString());
        toJson(entry.getValue(), generator);
      }
      generator.writeEndObject();
    } else if (datum instanceof Collection) { // array
      generator.writeStartArray();
      for (Object element : (Collection<?>) datum) {
        toJson(element, generator);
      }
      generator.writeEndArray();
    } else if (datum instanceof byte[]) { // bytes, fixed
      generator.writeString(new String((byte[]) datum, StandardCharsets.ISO_8859_1));
    } else if (datum instanceof CharSequence || datum instanceof Enum<?>) { // string, enum
      generator.writeString(datum.toString());
    } else if (datum instanceof Double) { // double
      generator.writeNumber((Double) datum);
    } else if (datum instanceof Float) { // float
      generator.writeNumber((Float) datum);
    } else if (datum instanceof Long) { // long
      generator.writeNumber((Long) datum);
    } else if (datum instanceof Integer) { // int
      generator.writeNumber((Integer) datum);
    } else if (datum instanceof Boolean) { // boolean
      generator.writeBoolean((Boolean) datum);
    } else if (datum instanceof BigInteger) {
      generator.writeNumber((BigInteger) datum);
    } else if (datum instanceof BigDecimal) {
      generator.writeNumber((BigDecimal) datum);
    } else {
      throw new AvroRuntimeException("Unknown datum class: " + datum.getClass());
    }
  }

  public static Object toObject(JsonNode jsonNode) {
    return toObject(jsonNode, null);
  }

  public static Object toObject(JsonNode jsonNode, Schema schema) {
    if (schema != null && schema.getType().equals(Schema.Type.UNION)) {
      return toObject(jsonNode, schema.getTypes().get(0));
    }
    if (jsonNode == null) {
      return null;
    } else if (jsonNode.isNull()) {
      return JsonProperties.NULL_VALUE;
    } else if (jsonNode.isBoolean()) {
      return jsonNode.asBoolean();
    } else if (jsonNode.isInt()) {
      if (schema == null || schema.getType().equals(Schema.Type.INT)) {
        return jsonNode.asInt();
      } else if (schema.getType().equals(Schema.Type.LONG)) {
        return jsonNode.asLong();
      } else if (schema.getType().equals(Schema.Type.FLOAT)) {
        return (float) jsonNode.asDouble();
      } else if (schema.getType().equals(Schema.Type.DOUBLE)) {
        return jsonNode.asDouble();
      }
    } else if (jsonNode.isLong()) {
      if (schema == null || schema.getType().equals(Schema.Type.LONG)) {
        return jsonNode.asLong();
      } else if (schema.getType().equals(Schema.Type.INT)) {
        if (jsonNode.canConvertToInt()) {
          return jsonNode.asInt();
        } else {
          return jsonNode.asLong();
        }
      } else if (schema.getType().equals(Schema.Type.FLOAT)) {
        return (float) jsonNode.asDouble();
      } else if (schema.getType().equals(Schema.Type.DOUBLE)) {
        return jsonNode.asDouble();
      }
    } else if (jsonNode.isDouble() || jsonNode.isFloat()) {
      if (schema == null || schema.getType().equals(Schema.Type.DOUBLE)) {
        return jsonNode.asDouble();
      } else if (schema.getType().equals(Schema.Type.FLOAT)) {
        return (float) jsonNode.asDouble();
      }
    } else if (jsonNode.isTextual()) {
      if (schema == null || schema.getType().equals(Schema.Type.STRING) || schema.getType().equals(Schema.Type.ENUM)) {
        return jsonNode.asText();
      } else if (schema.getType().equals(Schema.Type.BYTES) || schema.getType().equals(Schema.Type.FIXED)) {
        return jsonNode.textValue().getBytes(StandardCharsets.ISO_8859_1);
      }
    } else if (jsonNode.isArray()) {
      List<Object> l = new ArrayList<>();
      for (JsonNode node : jsonNode) {
        l.add(toObject(node, schema == null ? null : schema.getElementType()));
      }
      return l;
    } else if (jsonNode.isObject()) {
      Map<Object, Object> m = new LinkedHashMap<>();
      for (Iterator<String> it = jsonNode.fieldNames(); it.hasNext();) {
        String key = it.next();
        final Schema s;
        if (schema != null && schema.getType().equals(Schema.Type.MAP)) {
          s = schema.getValueType();
        } else if (schema != null && schema.getType().equals(Schema.Type.RECORD)) {
          s = schema.getField(key).schema();
        } else {
          s = null;
        }
        Object value = toObject(jsonNode.get(key), s);
        m.put(key, value);
      }
      return m;
    }
    return null;
  }

  /**
   * Convert an object into a map
   * 
   * @param datum The object
   * @return Its Map representation
   */
  public static Map objectToMap(Object datum) {
    ObjectMapper mapper = new ObjectMapper();
    // we only care about fields
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper.convertValue(datum, Map.class);
  }
}
