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
package org.apache.avro.tool;

import org.apache.avro.Protocol;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.ipc.HandshakeRequest;
import org.apache.avro.ipc.HandshakeResponse;
import org.apache.avro.ipc.Ipc;
import org.apache.avro.ipc.MD5;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.avro.util.ByteBufferOutputStream;

import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Tool to grab the protocol from a remote running service.
 */
public class RpcProtocolTool implements Tool {

  @Override
  public String getName() {
    return "rpcprotocol";
  }

  @Override
  public String getShortDescription() {
    return "Output the protocol of a RPC service";
  }

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {

    if (args.size() != 1) {
      err.println("Usage: uri");
      return 1;
    }

    URI uri = URI.create(args.get(0));

    try (Transceiver transceiver = Ipc.createTransceiver(uri)) {

      // write an empty HandshakeRequest
      HandshakeRequest rq = HandshakeRequest.newBuilder().setClientHash(new MD5(new byte[16]))
          .setServerHash(new MD5(new byte[16])).setClientProtocol(null).setMeta(new LinkedHashMap<>()).build();

      DatumWriter<HandshakeRequest> handshakeWriter = new SpecificDatumWriter<>(HandshakeRequest.class);

      ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream();

      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteBufferOutputStream, null);

      handshakeWriter.write(rq, encoder);
      encoder.flush();

      // send it and get the response
      List<ByteBuffer> response = transceiver.transceive(byteBufferOutputStream.getBufferList());

      // parse the response
      ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(response);

      DatumReader<HandshakeResponse> handshakeReader = new SpecificDatumReader<>(HandshakeResponse.class);

      HandshakeResponse handshakeResponse = handshakeReader.read(null,
          DecoderFactory.get().binaryDecoder(byteBufferInputStream, null));

      Protocol p = Protocol.parse(handshakeResponse.getServerProtocol());

      // finally output the protocol
      out.println(p.toString(true));

    }
    return 0;
  }
}
