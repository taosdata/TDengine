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
package org.apache.avro.ipc.netty;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Ignore;

import io.netty.handler.codec.compression.JdkZlibDecoder;
import io.netty.handler.codec.compression.JdkZlibEncoder;

public class TestNettyServerWithCompression extends TestNettyServer {

  @BeforeClass
  public static void initializeConnections() throws Exception {
    initializeConnections(ch -> {
      ch.pipeline().addFirst("deflater", new JdkZlibEncoder(6));
      ch.pipeline().addFirst("inflater", new JdkZlibDecoder());
    });
  }

  @Ignore
  @Override
  public void testBadRequest() throws IOException {
    // this tests in the base class needs to be skipped
    // as the decompression/compression algorithms will write the gzip header out
    // prior to the stream closing so the stream is not completely empty
  }
}
