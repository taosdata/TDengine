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

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;

/**
 * Expert: a custom encoder and decoder that writes an object directly to avro.
 * No validation is performed to check that the encoding conforms to the schema.
 * Invalid implementations may result in an unreadable file. The use of
 * {@link org.apache.avro.io.ValidatingEncoder} is recommended.
 *
 * @param <T> The class of objects that can be serialized with this encoder /
 *            decoder.
 */
public abstract class CustomEncoding<T> {

  protected Schema schema;

  protected abstract void write(Object datum, Encoder out) throws IOException;

  protected abstract T read(Object reuse, Decoder in) throws IOException;

  T read(Decoder in) throws IOException {
    return this.read(null, in);
  }

  protected Schema getSchema() {
    return schema;
  }

}
