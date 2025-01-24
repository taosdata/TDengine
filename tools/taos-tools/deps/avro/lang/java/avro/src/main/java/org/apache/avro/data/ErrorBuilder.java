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
package org.apache.avro.data;

/** Interface for error builders */
public interface ErrorBuilder<T> extends RecordBuilder<T> {

  /** Gets the value */
  Object getValue();

  /** Sets the value */
  ErrorBuilder<T> setValue(Object value);

  /** Checks whether the value has been set */
  boolean hasValue();

  /** Clears the value */
  ErrorBuilder<T> clearValue();

  /** Gets the error cause */
  Throwable getCause();

  /** Sets the error cause */
  ErrorBuilder<T> setCause(Throwable cause);

  /** Checks whether the cause has been set */
  boolean hasCause();

  /** Clears the cause */
  ErrorBuilder<T> clearCause();

}
