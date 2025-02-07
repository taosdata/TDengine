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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.hadoop.io;

import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * A helper object for working with the Avro generic records that are used to
 * store key/value pairs in an Avro container file.
 *
 * @param <K> The java type for the key.
 * @param <V> The java type for the value.
 */
public class AvroKeyValue<K, V> {
  /** The name of the key value pair generic record. */
  public static final String KEY_VALUE_PAIR_RECORD_NAME = "KeyValuePair";

  /** The namespace of the key value pair generic record. */
  public static final String KEY_VALUE_PAIR_RECORD_NAMESPACE = "org.apache.avro.mapreduce";

  /** The name of the generic record field containing the key. */
  public static final String KEY_FIELD = "key";

  /** The name of the generic record field containing the value. */
  public static final String VALUE_FIELD = "value";

  /** The key/value generic record wrapped by this class. */
  private final GenericRecord mKeyValueRecord;

  /**
   * Wraps a GenericRecord that is a key value pair.
   */
  public AvroKeyValue(GenericRecord keyValueRecord) {
    mKeyValueRecord = keyValueRecord;
  }

  /**
   * Gets the wrapped key/value GenericRecord.
   *
   * @return The key/value Avro generic record.
   */
  public GenericRecord get() {
    return mKeyValueRecord;
  }

  /**
   * Read the key.
   *
   * @return The key from the key/value generic record.
   */
  @SuppressWarnings("unchecked")
  public K getKey() {
    return (K) mKeyValueRecord.get(KEY_FIELD);
  }

  /**
   * Read the value.
   *
   * @return The value from the key/value generic record.
   */
  @SuppressWarnings("unchecked")
  public V getValue() {
    return (V) mKeyValueRecord.get(VALUE_FIELD);
  }

  /**
   * Sets the key.
   *
   * @param key The key.
   */
  public void setKey(K key) {
    mKeyValueRecord.put(KEY_FIELD, key);
  }

  /**
   * Sets the value.
   *
   * @param value The value.
   */
  public void setValue(V value) {
    mKeyValueRecord.put(VALUE_FIELD, value);
  }

  /**
   * Creates a KeyValuePair generic record schema.
   *
   * @return A schema for a generic record with two fields: 'key' and 'value'.
   */
  public static Schema getSchema(Schema keySchema, Schema valueSchema) {
    Schema schema = Schema.createRecord(KEY_VALUE_PAIR_RECORD_NAME, "A key/value pair", KEY_VALUE_PAIR_RECORD_NAMESPACE,
        false);
    schema.setFields(Arrays.asList(new Schema.Field(KEY_FIELD, keySchema, "The key", null),
        new Schema.Field(VALUE_FIELD, valueSchema, "The value", null)));
    return schema;
  }

  /**
   * A wrapper for iterators over GenericRecords that are known to be KeyValue
   * records.
   *
   * @param <K> The key type.
   * @param <V> The value type.
   */
  public static class Iterator<K, V> implements java.util.Iterator<AvroKeyValue<K, V>> {
    /** The wrapped iterator. */
    private final java.util.Iterator<? extends GenericRecord> mGenericIterator;

    /**
     * Constructs an iterator over key-value map entries out of a generic iterator.
     *
     * @param genericIterator An iterator over some generic record entries.
     */
    public Iterator(java.util.Iterator<? extends GenericRecord> genericIterator) {
      mGenericIterator = genericIterator;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
      return mGenericIterator.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public AvroKeyValue<K, V> next() {
      GenericRecord genericRecord = mGenericIterator.next();
      if (null == genericRecord) {
        return null;
      }
      return new AvroKeyValue<>(genericRecord);
    }

    /** {@inheritDoc} */
    @Override
    public void remove() {
      mGenericIterator.remove();
    }
  }
}
