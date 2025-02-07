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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/** Metadata for a column. */
public class ColumnMetaData extends MetaData<ColumnMetaData> {

  static final String NAME_KEY = RESERVED_KEY_PREFIX + "name";
  static final String TYPE_KEY = RESERVED_KEY_PREFIX + "type";
  static final String VALUES_KEY = RESERVED_KEY_PREFIX + "values";
  static final String PARENT_KEY = RESERVED_KEY_PREFIX + "parent";
  static final String ARRAY_KEY = RESERVED_KEY_PREFIX + "array";

  // cache these values for better performance
  private String name;
  private ValueType type;
  private boolean values;
  private ColumnMetaData parent;
  private boolean isArray;

  private transient List<ColumnMetaData> children = new ArrayList<>(0);
  private transient int number = -1;

  private ColumnMetaData() {
  } // non-public ctor

  /** Construct given a name and type. */
  public ColumnMetaData(String name, ValueType type) {
    this.name = name;
    setReserved(NAME_KEY, name);
    this.type = type;
    setReserved(TYPE_KEY, type.getName());
  }

  /** Return this column's name. */
  public String getName() {
    return name;
  }

  /** Return this column's type. */
  public ValueType getType() {
    return type;
  }

  /** Return this column's parent or null. */
  public ColumnMetaData getParent() {
    return parent;
  }

  /** Return this column's children or null. */
  public List<ColumnMetaData> getChildren() {
    return children;
  }

  /** Return true if this column is an array. */
  public boolean isArray() {
    return isArray;
  }

  /** Return this column's number in a file. */
  public int getNumber() {
    return number;
  }

  void setNumber(int number) {
    this.number = number;
  }

  /**
   * Set whether this column has an index of blocks by value. This only makes
   * sense for sorted columns and permits one to seek into a column by value.
   */
  public ColumnMetaData hasIndexValues(boolean values) {
    if (isArray)
      throw new TrevniRuntimeException("Array column cannot have index: " + this);
    this.values = values;
    return setReservedBoolean(VALUES_KEY, values);
  }

  /** Set this column's parent. A parent must be a preceding array column. */
  public ColumnMetaData setParent(ColumnMetaData parent) {
    if (!parent.isArray())
      throw new TrevniRuntimeException("Parent is not an array: " + parent);
    if (values)
      throw new TrevniRuntimeException("Array column cannot have index: " + this);
    this.parent = parent;
    parent.children.add(this);
    return setReserved(PARENT_KEY, parent.getName());
  }

  /** Set whether this column is an array. */
  public ColumnMetaData isArray(boolean isArray) {
    if (values)
      throw new TrevniRuntimeException("Array column cannot have index: " + this);
    this.isArray = isArray;
    return setReservedBoolean(ARRAY_KEY, isArray);
  }

  /** Get whether this column has an index of blocks by value. */
  public boolean hasIndexValues() {
    return getBoolean(VALUES_KEY);
  }

  static ColumnMetaData read(InputBuffer in, ColumnFileReader file) throws IOException {
    ColumnMetaData result = new ColumnMetaData();
    MetaData.read(in, result);
    result.name = result.getString(NAME_KEY);
    result.type = ValueType.forName(result.getString(TYPE_KEY));
    result.values = result.getBoolean(VALUES_KEY);
    result.isArray = result.getBoolean(ARRAY_KEY);

    String parentName = result.getString(PARENT_KEY);
    if (parentName != null)
      result.setParent(file.getColumnMetaData(parentName));

    return result;
  }

}
