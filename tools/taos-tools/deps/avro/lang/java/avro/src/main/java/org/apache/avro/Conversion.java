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

package org.apache.avro;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;

/**
 * Conversion between generic and logical type instances.
 * <p>
 * Instances of this class are added to GenericData to convert a logical type to
 * a particular representation.
 * <p>
 * Implementations must provide: * {@link #getConvertedType()}: get the Java
 * class used for the logical type * {@link #getLogicalTypeName()}: get the
 * logical type this implements
 * <p>
 * Subclasses must also override all of the conversion methods for Avro's base
 * types that are valid for the logical type, or else risk causing
 * {@code UnsupportedOperationException} at runtime.
 * <p>
 * Optionally, use {@link #getRecommendedSchema()} to provide a Schema that will
 * be used when a Schema is generated for the class returned by
 * {@code getConvertedType}.
 *
 * @param <T> a Java type that generic data is converted to
 */
public abstract class Conversion<T> {

  /**
   * Return the Java class representing the logical type.
   *
   * @return a Java class returned by from methods and accepted by to methods
   */
  public abstract Class<T> getConvertedType();

  /**
   * Return the logical type this class converts.
   *
   * @return a String logical type name
   */
  public abstract String getLogicalTypeName();

  /**
   * Certain logical types may require adjusting the code within the "setter"
   * methods to make sure the data that is set is properly formatted. This method
   * allows the Conversion to generate custom setter code if required.
   * 
   * @param varName
   * @param valParamName
   * @return a String for the body of the setter method
   */
  public String adjustAndSetValue(String varName, String valParamName) {
    return varName + " = " + valParamName + ";";
  }

  public Schema getRecommendedSchema() {
    throw new UnsupportedOperationException("No recommended schema for " + getLogicalTypeName());
  }

  public T fromBoolean(Boolean value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromBoolean is not supported for " + type.getName());
  }

  public T fromInt(Integer value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromInt is not supported for " + type.getName());
  }

  public T fromLong(Long value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromLong is not supported for " + type.getName());
  }

  public T fromFloat(Float value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromFloat is not supported for " + type.getName());
  }

  public T fromDouble(Double value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromDouble is not supported for " + type.getName());
  }

  public T fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromCharSequence is not supported for " + type.getName());
  }

  public T fromEnumSymbol(GenericEnumSymbol value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromEnumSymbol is not supported for " + type.getName());
  }

  public T fromFixed(GenericFixed value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromFixed is not supported for " + type.getName());
  }

  public T fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromBytes is not supported for " + type.getName());
  }

  public T fromArray(Collection<?> value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromArray is not supported for " + type.getName());
  }

  public T fromMap(Map<?, ?> value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromMap is not supported for " + type.getName());
  }

  public T fromRecord(IndexedRecord value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("fromRecord is not supported for " + type.getName());
  }

  public Boolean toBoolean(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toBoolean is not supported for " + type.getName());
  }

  public Integer toInt(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toInt is not supported for " + type.getName());
  }

  public Long toLong(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toLong is not supported for " + type.getName());
  }

  public Float toFloat(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toFloat is not supported for " + type.getName());
  }

  public Double toDouble(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toDouble is not supported for " + type.getName());
  }

  public CharSequence toCharSequence(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toCharSequence is not supported for " + type.getName());
  }

  public GenericEnumSymbol toEnumSymbol(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toEnumSymbol is not supported for " + type.getName());
  }

  public GenericFixed toFixed(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toFixed is not supported for " + type.getName());
  }

  public ByteBuffer toBytes(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toBytes is not supported for " + type.getName());
  }

  public Collection<?> toArray(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toArray is not supported for " + type.getName());
  }

  public Map<?, ?> toMap(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toMap is not supported for " + type.getName());
  }

  public IndexedRecord toRecord(T value, Schema schema, LogicalType type) {
    throw new UnsupportedOperationException("toRecord is not supported for " + type.getName());
  }

}
