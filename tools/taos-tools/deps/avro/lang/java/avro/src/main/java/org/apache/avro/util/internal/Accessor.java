/**
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

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Field.Order;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

public class Accessor {
  public abstract static class JsonPropertiesAccessor {
    protected abstract void addProp(JsonProperties props, String name, JsonNode value);
  }

  public abstract static class FieldAccessor {
    protected abstract JsonNode defaultValue(Field field);

    protected abstract Field createField(String name, Schema schema, String doc, JsonNode defaultValue,
        boolean validate, Order order);

    protected abstract Field createField(String name, Schema schema, String doc, JsonNode defaultValue);
  }

  public abstract static class ResolvingGrammarGeneratorAccessor {
    protected abstract void encode(Encoder e, Schema s, JsonNode n) throws IOException;
  }

  public abstract static class EncoderFactoryAccessor {
    protected abstract JsonEncoder jsonEncoder(EncoderFactory factory, Schema schema, JsonGenerator gen)
        throws IOException;
  }

  private static volatile JsonPropertiesAccessor jsonPropertiesAccessor;

  private static volatile FieldAccessor fieldAccessor;

  private static volatile ResolvingGrammarGeneratorAccessor resolvingGrammarGeneratorAccessor;

  public static void setAccessor(JsonPropertiesAccessor accessor) {
    if (jsonPropertiesAccessor != null)
      throw new IllegalStateException("JsonPropertiesAccessor already initialized");
    jsonPropertiesAccessor = accessor;
  }

  public static void setAccessor(FieldAccessor accessor) {
    if (fieldAccessor != null)
      throw new IllegalStateException("FieldAccessor already initialized");
    fieldAccessor = accessor;
  }

  private static FieldAccessor fieldAccessor() {
    if (fieldAccessor == null)
      ensureLoaded(Field.class);
    return fieldAccessor;
  }

  public static void setAccessor(ResolvingGrammarGeneratorAccessor accessor) {
    if (resolvingGrammarGeneratorAccessor != null)
      throw new IllegalStateException("ResolvingGrammarGeneratorAccessor already initialized");
    resolvingGrammarGeneratorAccessor = accessor;
  }

  private static ResolvingGrammarGeneratorAccessor resolvingGrammarGeneratorAccessor() {
    if (resolvingGrammarGeneratorAccessor == null)
      ensureLoaded(ResolvingGrammarGenerator.class);
    return resolvingGrammarGeneratorAccessor;
  }

  private static void ensureLoaded(Class<?> c) {
    try {
      Class.forName(c.getName());
    } catch (ClassNotFoundException e) {
      // Shall never happen as the class is specified by its Class instance
    }
  }

  public static void addProp(JsonProperties props, String name, JsonNode value) {
    jsonPropertiesAccessor.addProp(props, name, value);
  }

  public static JsonNode defaultValue(Field field) {
    return fieldAccessor.defaultValue(field);
  }

  public static void encode(Encoder e, Schema s, JsonNode n) throws IOException {
    resolvingGrammarGeneratorAccessor().encode(e, s, n);
  }

  public static Field createField(String name, Schema schema, String doc, JsonNode defaultValue, boolean validate,
      Order order) {
    return fieldAccessor().createField(name, schema, doc, defaultValue, validate, order);
  }

  public static Field createField(String name, Schema schema, String doc, JsonNode defaultValue) {
    return fieldAccessor().createField(name, schema, doc, defaultValue);
  }

}
