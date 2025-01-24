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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares that a Java type should have a specified Avro schema, overriding the
 * normally inferred schema. May be used for classes, parameters, fields and
 * method return types.
 * <p>
 * This is useful for slight alterations to the schema that would be
 * automatically inferred. For example, a <code>List&lt;Integer&gt;</code>whose
 * elements may be null might use the annotation
 * 
 * <pre>
 * &#64;AvroSchema("{\"type\":\"array\",\"items\":[\"null\",\"int\"]}")
 * </pre>
 * 
 * since the {@link Nullable} annotation could not be used here.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.PARAMETER, ElementType.METHOD, ElementType.FIELD })
@Documented
public @interface AvroSchema {
  /** The schema to use for this value. */
  String value();
}
