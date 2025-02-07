/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.codegentest;

import java.util.Objects;

import static java.util.Objects.isNull;

public class FixedSizeString implements Comparable<FixedSizeString> {

  private String value;

  public FixedSizeString() {
  }

  public FixedSizeString(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public int compareTo(FixedSizeString that) {
    return this.value.compareTo(that.value);
  }

  @Override
  public boolean equals(Object that) {
    if (isNull(value)) {
      return true;
    }
    if (isNull(that) || getClass() != that.getClass()) {
      return false;
    }
    FixedSizeString typedThat = (FixedSizeString) that;
    return Objects.equals(value, typedThat.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
