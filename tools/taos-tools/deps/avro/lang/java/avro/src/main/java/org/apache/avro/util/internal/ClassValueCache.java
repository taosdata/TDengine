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

import java.util.function.Function;

/**
 * Wraps a {@link ClassValue} cache so it can be overridden in an android
 * environment, where it isn't available.
 *
 * @param <R> Return type of the ClassValue
 */
public class ClassValueCache<R> implements Function<Class<?>, R> {

  private final Function<Class<?>, R> ifAbsent;

  private final ClassValue<R> cache = new ClassValue<R>() {
    @Override
    protected R computeValue(Class<?> c) {
      return ifAbsent.apply(c);
    }
  };

  public ClassValueCache(Function<Class<?>, R> ifAbsent) {
    this.ifAbsent = ifAbsent;
  }

  @Override
  public R apply(Class<?> c) {
    return cache.get(c);
  }
}
