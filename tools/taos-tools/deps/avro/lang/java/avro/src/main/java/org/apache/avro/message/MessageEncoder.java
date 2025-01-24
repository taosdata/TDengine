/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.message;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Serializes an individual datum as a ByteBuffer or to an OutputStream.
 * 
 * @param <D> a datum class
 */
public interface MessageEncoder<D> {

  /**
   * Serialize a single datum to a ByteBuffer.
   *
   * @param datum a datum
   * @return a ByteBuffer containing the serialized datum
   * @throws IOException
   */
  ByteBuffer encode(D datum) throws IOException;

  /**
   * Serialize a single datum to an OutputStream.
   *
   * @param datum  a datum
   * @param stream an OutputStream to serialize the datum to
   * @throws IOException
   */
  void encode(D datum, OutputStream stream) throws IOException;

}
