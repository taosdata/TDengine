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
package org.apache.avro.thrift;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import org.apache.avro.util.ClassUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.TUnion;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.StructMetaData;

/** Utilities for serializing Thrift data in Avro format. */
public class ThriftData extends GenericData {
  static final String THRIFT_TYPE = "thrift";
  static final String THRIFT_PROP = "thrift";

  private static final ThriftData INSTANCE = new ThriftData();

  protected ThriftData() {
  }

  /** Return the singleton instance. */
  public static ThriftData get() {
    return INSTANCE;
  }

  @Override
  public DatumReader createDatumReader(Schema schema) {
    return new ThriftDatumReader(schema, schema, this);
  }

  @Override
  public DatumWriter createDatumWriter(Schema schema) {
    return new ThriftDatumWriter(schema, this);
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
    if (value == null && record instanceof TUnion)
      return;
    ((TBase) record).setFieldValue(((TFieldIdEnum[]) state)[position], value);
  }

  @Override
  protected Object getField(Object record, String name, int pos, Object state) {
    TFieldIdEnum f = ((TFieldIdEnum[]) state)[pos];
    TBase struct = (TBase) record;
    if (struct.isSet(f))
      return struct.getFieldValue(f);
    return null;
  }

  private final Map<Schema, TFieldIdEnum[]> fieldCache = new ConcurrentHashMap<>();

  @Override
  @SuppressWarnings("unchecked")
  protected Object getRecordState(Object r, Schema s) {
    TFieldIdEnum[] fields = fieldCache.get(s);
    if (fields == null) { // cache miss
      fields = new TFieldIdEnum[s.getFields().size()];
      Class c = r.getClass();
      for (TFieldIdEnum f : FieldMetaData.getStructMetaDataMap((Class<? extends TBase>) c).keySet())
        fields[s.getField(f.getFieldName()).pos()] = f;
      fieldCache.put(s, fields); // update cache
    }
    return fields;
  }

  @Override
  protected String getSchemaName(Object datum) {
    // support implicit conversion from thrift's i16
    // to avro INT for thrift's optional fields
    if (datum instanceof Short)
      return Schema.Type.INT.getName();
    // support implicit conversion from thrift's byte
    // to avro INT for thrift's optional fields
    if (datum instanceof Byte)
      return Schema.Type.INT.getName();

    return super.getSchemaName(datum);
  }

  @Override
  protected boolean isRecord(Object datum) {
    return datum instanceof TBase;
  }

  @Override
  protected boolean isEnum(Object datum) {
    return datum instanceof TEnum;
  }

  @Override
  protected Schema getEnumSchema(Object datum) {
    return getSchema(datum.getClass());
  }

  @Override
  // setFieldValue takes ByteBuffer but getFieldValue returns byte[]
  protected boolean isBytes(Object datum) {
    if (datum instanceof ByteBuffer)
      return true;
    if (datum == null)
      return false;
    Class c = datum.getClass();
    return c.isArray() && c.getComponentType() == Byte.TYPE;
  }

  @Override
  public Object newRecord(Object old, Schema schema) {
    try {
      Class c = ClassUtils.forName(SpecificData.getClassName(schema));
      if (c == null)
        return super.newRecord(old, schema); // punt to generic
      if (c.isInstance(old))
        return old; // reuse instance
      return c.newInstance(); // create new instance
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    return getSchema(record.getClass());
  }

  private final Map<Class, Schema> schemaCache = new ConcurrentHashMap<>();

  /** Return a record schema given a thrift generated class. */
  @SuppressWarnings("unchecked")
  public Schema getSchema(Class c) {
    Schema schema = schemaCache.get(c);

    if (schema == null) { // cache miss
      try {
        if (TEnum.class.isAssignableFrom(c)) { // enum
          List<String> symbols = new ArrayList<>();
          for (Enum e : ((Class<? extends Enum>) c).getEnumConstants())
            symbols.add(e.name());
          schema = Schema.createEnum(c.getName(), null, null, symbols);
        } else if (TBase.class.isAssignableFrom(c)) { // struct
          schema = Schema.createRecord(c.getName(), null, null, Throwable.class.isAssignableFrom(c));
          List<Field> fields = new ArrayList<>();
          for (FieldMetaData f : FieldMetaData.getStructMetaDataMap((Class<? extends TBase>) c).values()) {
            Schema s = getSchema(f.valueMetaData);
            if (f.requirementType == TFieldRequirementType.OPTIONAL && (s.getType() != Schema.Type.UNION))
              s = nullable(s);
            fields.add(new Field(f.fieldName, s, null, null));
          }
          schema.setFields(fields);
        } else {
          throw new RuntimeException("Not a Thrift-generated class: " + c);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      schemaCache.put(c, schema); // update cache
    }
    return schema;
  }

  private static final Schema NULL = Schema.create(Schema.Type.NULL);

  private Schema getSchema(FieldValueMetaData f) {
    switch (f.type) {
    case TType.BOOL:
      return Schema.create(Schema.Type.BOOLEAN);
    case TType.BYTE:
      Schema b = Schema.create(Schema.Type.INT);
      b.addProp(THRIFT_PROP, "byte");
      return b;
    case TType.I16:
      Schema s = Schema.create(Schema.Type.INT);
      s.addProp(THRIFT_PROP, "short");
      return s;
    case TType.I32:
      return Schema.create(Schema.Type.INT);
    case TType.I64:
      return Schema.create(Schema.Type.LONG);
    case TType.DOUBLE:
      return Schema.create(Schema.Type.DOUBLE);
    case TType.ENUM:
      EnumMetaData enumMeta = (EnumMetaData) f;
      return nullable(getSchema(enumMeta.enumClass));
    case TType.LIST:
      ListMetaData listMeta = (ListMetaData) f;
      return nullable(Schema.createArray(getSchema(listMeta.elemMetaData)));
    case TType.MAP:
      MapMetaData mapMeta = (MapMetaData) f;
      if (mapMeta.keyMetaData.type != TType.STRING)
        throw new AvroRuntimeException("Map keys must be strings: " + f);
      Schema map = Schema.createMap(getSchema(mapMeta.valueMetaData));
      GenericData.setStringType(map, GenericData.StringType.String);
      return nullable(map);
    case TType.SET:
      SetMetaData setMeta = (SetMetaData) f;
      Schema set = Schema.createArray(getSchema(setMeta.elemMetaData));
      set.addProp(THRIFT_PROP, "set");
      return nullable(set);
    case TType.STRING:
      if (f.isBinary())
        return nullable(Schema.create(Schema.Type.BYTES));
      Schema string = Schema.create(Schema.Type.STRING);
      GenericData.setStringType(string, GenericData.StringType.String);
      return nullable(string);
    case TType.STRUCT:
      StructMetaData structMeta = (StructMetaData) f;
      Schema record = getSchema(structMeta.structClass);
      return nullable(record);
    case TType.VOID:
      return NULL;
    default:
      throw new RuntimeException("Unexpected type in field: " + f);
    }
  }

  private Schema nullable(Schema schema) {
    return Schema.createUnion(Arrays.asList(NULL, schema));
  }

}
