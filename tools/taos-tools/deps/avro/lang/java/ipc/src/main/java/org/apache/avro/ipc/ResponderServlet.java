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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.avro.AvroRuntimeException;

/** An {@link HttpServlet} that responds to Avro RPC requests. */
public class ResponderServlet extends HttpServlet {
  private Responder responder;

  public ResponderServlet(Responder responder) throws IOException {
    this.responder = responder;
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    response.setContentType(HttpTransceiver.CONTENT_TYPE);
    List<ByteBuffer> requestBufs = HttpTransceiver.readBuffers(request.getInputStream());
    try {
      List<ByteBuffer> responseBufs = responder.respond(requestBufs);
      response.setContentLength(HttpTransceiver.getLength(responseBufs));
      HttpTransceiver.writeBuffers(responseBufs, response.getOutputStream());
    } catch (AvroRuntimeException e) {
      throw new ServletException(e);
    }
  }
}
