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
package org.apache.avro.reflect;

import java.util.Map;

/**
 * Class to make Avro immune from the naming variations of key/value fields
 * among several {@link java.util.Map.Entry} implementations. If objects of this
 * class are used instead of the regular ones obtained by
 * {@link Map#entrySet()}, then we need not worry about the actual field-names
 * or any changes to them in the future.<BR>
 * Example: {@code ConcurrentHashMap.MapEntry} does not name the fields as key/
 * value in Java 1.8 while it used to do so in Java 1.7
 *
 * @param <K> Key of the map-entry
 * @param <V> Value of the map-entry
 * @deprecated Use org.apache.avro.util.MapEntry
 */
@Deprecated
public class MapEntry<K, V> implements Map.Entry<K, V> {

  K key;
  V value;

  public MapEntry(K key, V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public V setValue(V value) {
    V oldValue = this.value;
    this.value = value;
    return oldValue;
  }
}
