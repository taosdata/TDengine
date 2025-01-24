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
package org.apache.avro.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** Implementation of IPC that remains in process. */
public class LocalTransceiver extends Transceiver {
  private Responder responder;

  public LocalTransceiver(Responder responder) {
    this.responder = responder;
  }

  @Override
  public String getRemoteName() {
    return "local";
  }

  @Override
  public List<ByteBuffer> transceive(List<ByteBuffer> request) throws IOException {
    return responder.respond(request);
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    throw new UnsupportedOperationException();
  }
}
