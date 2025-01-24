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
package org.apache.trevni;

/** The datatypes that may be stored in a column. */
public enum ValueType {
  NULL, BOOLEAN, INT, LONG, FIXED32, FIXED64, FLOAT, DOUBLE, STRING, BYTES;

  private final String name;

  private ValueType() {
    this.name = this.name().toLowerCase();
  }

  /** Return the name of this type. */
  public String getName() {
    return name;
  }

  /** Return a type given its name. */
  public static ValueType forName(String name) {
    return valueOf(name.toUpperCase());
  }

}
