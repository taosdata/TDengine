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
package org.apache.avro.protobuf;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.IdentityHashMap;
import java.util.concurrent.ConcurrentHashMap;

import java.io.IOException;
import java.io.File;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DescriptorProtos.FileOptions;

import org.apache.avro.util.ClassUtils;
import org.apache.avro.util.internal.Accessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/** Utilities for serializing Protobuf data in Avro format. */
public class ProtobufData extends GenericData {
  private static final ProtobufData INSTANCE = new ProtobufData();

  protected ProtobufData() {
  }

  /** Return the singleton instance. */
  public static ProtobufData get() {
    return INSTANCE;
  }

  @Override
  public DatumReader createDatumReader(Schema schema) {
    return new ProtobufDatumReader(schema, schema, this);
  }

  @Override
  public DatumWriter createDatumWriter(Schema schema) {
    return new ProtobufDatumWriter(schema, this);
  }

  @Override
  public void setField(Object r, String n, int pos, Object value) {
    setField(r, n, pos, value, getRecordState(r, getSchema(r.getClass())));
  }

  @Override
  public Object getField(Object r, String name, int pos) {
    return getField(r, name, pos, getRecordState(r, getSchema(r.getClass())));
  }

  @Override
  protected void setField(Object record, String name, int position, Object value, Object state) {
    Builder b = (Builder) record;
    FieldDescriptor f = ((FieldDescriptor[]) state)[position];
    switch (f.getType()) {
    case MESSAGE:
      if (value == null) {
        b.clearField(f);
        break;
      }
    default:
      b.setField(f, value);
    }
  }

  @Override
  protected Object getField(Object record, String name, int pos, Object state) {
    Message m = (Message) record;
    FieldDescriptor f = ((FieldDescriptor[]) state)[pos];
    switch (f.getType()) {
    case MESSAGE:
      if (!f.isRepeated() && !m.hasField(f))
        return null;
    default:
      return m.getField(f);
    }
  }

  private final Map<Descriptor, FieldDescriptor[]> fieldCache = new ConcurrentHashMap<>();

  @Override
  protected Object getRecordState(Object r, Schema s) {
    Descriptor d = ((MessageOrBuilder) r).getDescriptorForType();
    FieldDescriptor[] fields = fieldCache.get(d);
    if (fields == null) { // cache miss
      fields = new FieldDescriptor[s.getFields().size()];
      for (Field f : s.getFields())
        fields[f.pos()] = d.findFieldByName(f.name());
      fieldCache.put(d, fields); // update cache
    }
    return fields;
  }

  @Override
  protected boolean isRecord(Object datum) {
    return datum instanceof Message;
  }

  @Override
  public Object newRecord(Object old, Schema schema) {
    try {
      Class c = SpecificData.get().getClass(schema);
      if (c == null)
        return super.newRecord(old, schema); // punt to generic
      if (c.isInstance(old))
        return old; // reuse instance
      return c.getMethod("newBuilder").invoke(null);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean isArray(Object datum) {
    return datum instanceof List;
  }

  @Override
  protected boolean isBytes(Object datum) {
    return datum instanceof ByteString;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    Descriptor descriptor = ((Message) record).getDescriptorForType();
    Schema schema = schemaCache.get(descriptor);

    if (schema == null) {
      schema = getSchema(descriptor);
      schemaCache.put(descriptor, schema);
    }
    return schema;
  }

  private final Map<Object, Schema> schemaCache = new ConcurrentHashMap<>();

  /** Return a record schema given a protobuf message class. */
  public Schema getSchema(Class c) {
    Schema schema = schemaCache.get(c);

    if (schema == null) { // cache miss
      try {
        Object descriptor = c.getMethod("getDescriptor").invoke(null);
        if (c.isEnum())
          schema = getSchema((EnumDescriptor) descriptor);
        else
          schema = getSchema((Descriptor) descriptor);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      schemaCache.put(c, schema); // update cache
    }
    return schema;
  }

  private static final ThreadLocal<Map<Descriptor, Schema>> SEEN = ThreadLocal.withInitial(IdentityHashMap::new);

  public Schema getSchema(Descriptor descriptor) {
    Map<Descriptor, Schema> seen = SEEN.get();
    if (seen.containsKey(descriptor)) // stop recursion
      return seen.get(descriptor);
    boolean first = seen.isEmpty();

    Conversion conversion = getConversionByDescriptor(descriptor);
    if (conversion != null) {
      Schema converted = conversion.getRecommendedSchema();
      seen.put(descriptor, converted);
      return converted;
    }

    try {
      Schema result = Schema.createRecord(descriptor.getName(), null,
          getNamespace(descriptor.getFile(), descriptor.getContainingType()), false);

      seen.put(descriptor, result);

      List<Field> fields = new ArrayList<>(descriptor.getFields().size());
      for (FieldDescriptor f : descriptor.getFields())
        fields.add(Accessor.createField(f.getName(), getSchema(f), null, getDefault(f)));
      result.setFields(fields);
      return result;

    } finally {
      if (first)
        seen.clear();
    }
  }

  public String getNamespace(FileDescriptor fd, Descriptor containing) {
    FileOptions o = fd.getOptions();
    String p = o.hasJavaPackage() ? o.getJavaPackage() : fd.getPackage();
    String outer = "";
    if (!o.getJavaMultipleFiles()) {
      if (o.hasJavaOuterClassname()) {
        outer = o.getJavaOuterClassname();
      } else {
        outer = new File(fd.getName()).getName();
        outer = outer.substring(0, outer.lastIndexOf('.'));
        outer = toCamelCase(outer);
      }
    }
    StringBuilder inner = new StringBuilder();
    while (containing != null) {
      if (inner.length() == 0) {
        inner.insert(0, containing.getName());
      } else {
        inner.insert(0, containing.getName() + "$");
      }
      containing = containing.getContainingType();
    }
    String d1 = (!outer.isEmpty() || inner.length() != 0 ? "." : "");
    String d2 = (!outer.isEmpty() && inner.length() != 0 ? "$" : "");
    return p + d1 + outer + d2 + inner;
  }

  private static String toCamelCase(String s) {
    String[] parts = s.split("_");
    StringBuilder camelCaseString = new StringBuilder(s.length());
    for (String part : parts) {
      camelCaseString.append(cap(part));
    }
    return camelCaseString.toString();
  }

  private static String cap(String s) {
    return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
  }

  private static final Schema NULL = Schema.create(Schema.Type.NULL);

  public Schema getSchema(FieldDescriptor f) {
    Schema s = getNonRepeatedSchema(f);
    if (f.isRepeated())
      s = Schema.createArray(s);
    return s;
  }

  private Schema getNonRepeatedSchema(FieldDescriptor f) {
    Schema result;
    switch (f.getType()) {
    case BOOL:
      return Schema.create(Schema.Type.BOOLEAN);
    case FLOAT:
      return Schema.create(Schema.Type.FLOAT);
    case DOUBLE:
      return Schema.create(Schema.Type.DOUBLE);
    case STRING:
      Schema s = Schema.create(Schema.Type.STRING);
      GenericData.setStringType(s, GenericData.StringType.String);
      return s;
    case BYTES:
      return Schema.create(Schema.Type.BYTES);
    case INT32:
    case UINT32:
    case SINT32:
    case FIXED32:
    case SFIXED32:
      return Schema.create(Schema.Type.INT);
    case INT64:
    case UINT64:
    case SINT64:
    case FIXED64:
    case SFIXED64:
      return Schema.create(Schema.Type.LONG);
    case ENUM:
      return getSchema(f.getEnumType());
    case MESSAGE:
      result = getSchema(f.getMessageType());
      if (f.isOptional())
        // wrap optional record fields in a union with null
        result = Schema.createUnion(Arrays.asList(NULL, result));
      return result;
    case GROUP: // groups are deprecated
    default:
      throw new RuntimeException("Unexpected type: " + f.getType());
    }
  }

  public Schema getSchema(EnumDescriptor d) {
    List<String> symbols = new ArrayList<>(d.getValues().size());
    for (EnumValueDescriptor e : d.getValues()) {
      symbols.add(e.getName());
    }
    return Schema.createEnum(d.getName(), null, getNamespace(d.getFile(), d.getContainingType()), symbols);
  }

  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private JsonNode getDefault(FieldDescriptor f) {
    if (f.isRequired()) // no default
      return null;

    if (f.isRepeated()) // empty array as repeated fields' default value
      return NODES.arrayNode();

    if (f.hasDefaultValue()) { // parse spec'd default value
      Object value = f.getDefaultValue();
      switch (f.getType()) {
      case ENUM:
        value = ((EnumValueDescriptor) value).getName();
        break;
      }
      String json = toString(value);
      try {
        return MAPPER.readTree(FACTORY.createParser(json));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    switch (f.getType()) { // generate default for type
    case BOOL:
      return NODES.booleanNode(false);
    case FLOAT:
      return NODES.numberNode(0.0F);
    case DOUBLE:
      return NODES.numberNode(0.0D);
    case INT32:
    case UINT32:
    case SINT32:
    case FIXED32:
    case SFIXED32:
    case INT64:
    case UINT64:
    case SINT64:
    case FIXED64:
    case SFIXED64:
      return NODES.numberNode(0);
    case STRING:
    case BYTES:
      return NODES.textNode("");
    case ENUM:
      return NODES.textNode(f.getEnumType().getValues().get(0).getName());
    case MESSAGE:
      return NODES.nullNode();
    case GROUP: // groups are deprecated
    default:
      throw new RuntimeException("Unexpected type: " + f.getType());
    }

  }

  /**
   * Get Conversion from protobuf descriptor via protobuf classname.
   *
   * @param descriptor protobuf descriptor
   * @return Conversion | null
   */
  private Conversion getConversionByDescriptor(Descriptor descriptor) {
    String namespace = getNamespace(descriptor.getFile(), descriptor.getContainingType());
    String name = descriptor.getName();
    String dot = namespace.endsWith("$") ? "" : "."; // back-compatibly handle $

    try {
      Class clazz = ClassUtils.forName(getClassLoader(), namespace + dot + name);
      return getConversionByClass(clazz);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }
}
