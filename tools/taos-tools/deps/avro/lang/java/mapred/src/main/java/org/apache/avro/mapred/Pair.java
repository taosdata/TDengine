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

package org.apache.avro.mapred;

import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.WeakHashMap;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificData.SchemaConstructable;
import org.apache.avro.reflect.ReflectData;

/** A key/value pair. */
public class Pair<K, V> implements IndexedRecord, Comparable<Pair>, SchemaConstructable {

  private static final String PAIR = Pair.class.getName();
  private static final String KEY = "key";
  private static final String VALUE = "value";

  private Schema schema;
  private K key;
  private V value;

  public Pair(Schema schema) {
    checkIsPairSchema(schema);
    this.schema = schema;
  }

  public Pair(K key, Schema keySchema, V value, Schema valueSchema) {
    this.schema = getPairSchema(keySchema, valueSchema);
    this.key = key;
    this.value = value;
  }

  private static void checkIsPairSchema(Schema schema) {
    if (!PAIR.equals(schema.getFullName()))
      throw new IllegalArgumentException("Not a Pair schema: " + schema);
  }

  /** Return a pair's key schema. */
  public static Schema getKeySchema(Schema pair) {
    checkIsPairSchema(pair);
    return pair.getField(KEY).schema();
  }

  /** Return a pair's value schema. */
  public static Schema getValueSchema(Schema pair) {
    checkIsPairSchema(pair);
    return pair.getField(VALUE).schema();
  }

  private static final Map<Schema, Map<Schema, Schema>> SCHEMA_CACHE = new WeakHashMap<>();

  /** Get a pair schema. */
  public static Schema getPairSchema(Schema key, Schema value) {
    Map<Schema, Schema> valueSchemas;
    synchronized (SCHEMA_CACHE) {
      valueSchemas = SCHEMA_CACHE.computeIfAbsent(key, k -> new WeakHashMap<>());
      Schema result;
      result = valueSchemas.get(value);
      if (result == null) {
        result = makePairSchema(key, value);
        valueSchemas.put(value, result);
      }
      return result;
    }
  }

  private static Schema makePairSchema(Schema key, Schema value) {
    Schema pair = Schema.createRecord(PAIR, null, null, false);
    List<Field> fields = Arrays.asList(new Field(KEY, key, "", null),
        new Field(VALUE, value, "", null, Field.Order.IGNORE));
    pair.setFields(fields);
    return pair;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  /** Get the key. */
  public K key() {
    return key;
  }

  /** Set the key. */
  public void key(K key) {
    this.key = key;
  }

  /** Get the value. */
  public V value() {
    return value;
  }

  /** Set the value. */
  public void value(V value) {
    this.value = value;
  }

  /** Set both the key and value. */
  public void set(K key, V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true; // identical object
    if (!(o instanceof Pair))
      return false; // not a pair
    Pair that = (Pair) o;
    if (!this.schema.equals(that.schema))
      return false; // not the same schema
    return this.compareTo(that) == 0;
  }

  @Override
  public int hashCode() {
    return GenericData.get().hashCode(this, schema);
  }

  @Override
  public int compareTo(Pair that) {
    return GenericData.get().compare(this, that, schema);
  }

  @Override
  public String toString() {
    return GenericData.get().toString(this);
  }

  @Override
  public Object get(int i) {
    switch (i) {
    case 0:
      return key;
    case 1:
      return value;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index: " + i);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void put(int i, Object o) {
    switch (i) {
    case 0:
      this.key = (K) o;
      break;
    case 1:
      this.value = (V) o;
      break;
    default:
      throw new org.apache.avro.AvroRuntimeException("Bad index: " + i);
    }
  }

  private static final Schema STRING_SCHEMA = Schema.create(Type.STRING);
  private static final Schema BYTES_SCHEMA = Schema.create(Type.BYTES);
  private static final Schema INT_SCHEMA = Schema.create(Type.INT);
  private static final Schema LONG_SCHEMA = Schema.create(Type.LONG);
  private static final Schema FLOAT_SCHEMA = Schema.create(Type.FLOAT);
  private static final Schema DOUBLE_SCHEMA = Schema.create(Type.DOUBLE);
  private static final Schema NULL_SCHEMA = Schema.create(Type.NULL);

  @SuppressWarnings("unchecked")
  public Pair(Object key, Object value) {
    this((K) key, getSchema(key), (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, GenericContainer value) {
    this((K) key, getSchema(key), (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, CharSequence value) {
    this((K) key, getSchema(key), (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, ByteBuffer value) {
    this((K) key, getSchema(key), (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, Integer value) {
    this((K) key, getSchema(key), (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, Long value) {
    this((K) key, getSchema(key), (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, Float value) {
    this((K) key, getSchema(key), (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, Double value) {
    this((K) key, getSchema(key), (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Object key, Void value) {
    this((K) key, getSchema(key), (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, Object value) {
    this((K) key, key.getSchema(), (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, GenericContainer value) {
    this((K) key, key.getSchema(), (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, CharSequence value) {
    this((K) key, key.getSchema(), (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, ByteBuffer value) {
    this((K) key, key.getSchema(), (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, Integer value) {
    this((K) key, key.getSchema(), (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, Long value) {
    this((K) key, key.getSchema(), (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, Float value) {
    this((K) key, key.getSchema(), (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, Double value) {
    this((K) key, key.getSchema(), (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(GenericContainer key, Void value) {
    this((K) key, key.getSchema(), (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, Object value) {
    this((K) key, STRING_SCHEMA, (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, GenericContainer value) {
    this((K) key, STRING_SCHEMA, (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, CharSequence value) {
    this((K) key, STRING_SCHEMA, (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, ByteBuffer value) {
    this((K) key, STRING_SCHEMA, (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, Integer value) {
    this((K) key, STRING_SCHEMA, (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, Long value) {
    this((K) key, STRING_SCHEMA, (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, Float value) {
    this((K) key, STRING_SCHEMA, (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, Double value) {
    this((K) key, STRING_SCHEMA, (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(CharSequence key, Void value) {
    this((K) key, STRING_SCHEMA, (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, Object value) {
    this((K) key, BYTES_SCHEMA, (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, GenericContainer value) {
    this((K) key, BYTES_SCHEMA, (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, CharSequence value) {
    this((K) key, BYTES_SCHEMA, (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, ByteBuffer value) {
    this((K) key, BYTES_SCHEMA, (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, Integer value) {
    this((K) key, BYTES_SCHEMA, (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, Long value) {
    this((K) key, BYTES_SCHEMA, (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, Float value) {
    this((K) key, BYTES_SCHEMA, (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, Double value) {
    this((K) key, BYTES_SCHEMA, (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(ByteBuffer key, Void value) {
    this((K) key, BYTES_SCHEMA, (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, Object value) {
    this((K) key, INT_SCHEMA, (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, GenericContainer value) {
    this((K) key, INT_SCHEMA, (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, CharSequence value) {
    this((K) key, INT_SCHEMA, (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, ByteBuffer value) {
    this((K) key, INT_SCHEMA, (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, Integer value) {
    this((K) key, INT_SCHEMA, (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, Long value) {
    this((K) key, INT_SCHEMA, (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, Float value) {
    this((K) key, INT_SCHEMA, (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, Double value) {
    this((K) key, INT_SCHEMA, (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Integer key, Void value) {
    this((K) key, INT_SCHEMA, (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, Object value) {
    this((K) key, LONG_SCHEMA, (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, GenericContainer value) {
    this((K) key, LONG_SCHEMA, (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, CharSequence value) {
    this((K) key, LONG_SCHEMA, (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, ByteBuffer value) {
    this((K) key, LONG_SCHEMA, (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, Integer value) {
    this((K) key, LONG_SCHEMA, (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, Long value) {
    this((K) key, LONG_SCHEMA, (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, Float value) {
    this((K) key, LONG_SCHEMA, (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, Double value) {
    this((K) key, LONG_SCHEMA, (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Long key, Void value) {
    this((K) key, LONG_SCHEMA, (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, Object value) {
    this((K) key, FLOAT_SCHEMA, (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, GenericContainer value) {
    this((K) key, FLOAT_SCHEMA, (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, CharSequence value) {
    this((K) key, FLOAT_SCHEMA, (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, ByteBuffer value) {
    this((K) key, FLOAT_SCHEMA, (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, Integer value) {
    this((K) key, FLOAT_SCHEMA, (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, Long value) {
    this((K) key, FLOAT_SCHEMA, (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, Float value) {
    this((K) key, FLOAT_SCHEMA, (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, Double value) {
    this((K) key, FLOAT_SCHEMA, (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Float key, Void value) {
    this((K) key, FLOAT_SCHEMA, (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, Object value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, GenericContainer value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, CharSequence value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, ByteBuffer value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, Integer value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, Long value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, Float value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, Double value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Double key, Void value) {
    this((K) key, DOUBLE_SCHEMA, (V) value, NULL_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, Object value) {
    this((K) key, NULL_SCHEMA, (V) value, getSchema(value));
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, GenericContainer value) {
    this((K) key, NULL_SCHEMA, (V) value, value.getSchema());
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, CharSequence value) {
    this((K) key, NULL_SCHEMA, (V) value, STRING_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, ByteBuffer value) {
    this((K) key, NULL_SCHEMA, (V) value, BYTES_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, Integer value) {
    this((K) key, NULL_SCHEMA, (V) value, INT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, Long value) {
    this((K) key, NULL_SCHEMA, (V) value, LONG_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, Float value) {
    this((K) key, NULL_SCHEMA, (V) value, FLOAT_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, Double value) {
    this((K) key, NULL_SCHEMA, (V) value, DOUBLE_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  public Pair(Void key, Void value) {
    this((K) key, NULL_SCHEMA, (V) value, NULL_SCHEMA);
  }

  private static Schema getSchema(Object o) {
    try {
      return ReflectData.get().getSchema(o.getClass());
    } catch (AvroRuntimeException e) {
      throw new AvroRuntimeException(
          "Cannot infer schema for : " + o.getClass() + ".  Must create Pair with explicit key and value schemas.", e);
    }
  }

  // private static final String[][] TABLE = new String[][] {
  // {"Object", "getSchema({0})"},
  // {"GenericContainer", "{0}.getSchema()"},
  // {"CharSequence", "STRING_SCHEMA"},
  // {"ByteBuffer", "BYTES_SCHEMA"},
  // {"Integer", "INT_SCHEMA"},
  // {"Long", "LONG_SCHEMA"},
  // {"Float", "FLOAT_SCHEMA"},
  // {"Double", "DOUBLE_SCHEMA"},
  // {"Void", "NULL_SCHEMA"},
  // };

  // private static String f(String pattern, String value) {
  // return java.text.MessageFormat.format(pattern, value);
  // }

  // public static void main(String... args) throws Exception {
  // StringBuffer b = new StringBuffer();
  // for (String[] k : TABLE) {
  // for (String[] v : TABLE) {
  // b.append("@SuppressWarnings(\"unchecked\")\n");
  // b.append("public Pair("+k[0]+" key, "+v[0]+" value) {\n");
  // b.append(" this((K)key, "+f(k[1],"key")
  // +", (V)value, "+f(v[1],"value")+");\n");
  // b.append("}\n");
  // }
  // }
  // System.out.println(b);
  // }

}
