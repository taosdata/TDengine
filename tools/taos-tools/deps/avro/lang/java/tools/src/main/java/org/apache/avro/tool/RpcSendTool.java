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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.File;
import java.net.URI;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Protocol.Message;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.ipc.Ipc;
import org.apache.avro.ipc.generic.GenericRequestor;

/**
 * Sends a single RPC message.
 */
public class RpcSendTool implements Tool {
  @Override
  public String getName() {
    return "rpcsend";
  }

  @Override
  public String getShortDescription() {
    return "Sends a single RPC message.";
  }

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    OptionParser p = new OptionParser();
    OptionSpec<String> file = p.accepts("file", "Data file containing request parameters.").withRequiredArg()
        .ofType(String.class);
    OptionSpec<String> data = p.accepts("data", "JSON-encoded request parameters.").withRequiredArg()
        .ofType(String.class);
    OptionSet opts = p.parse(args.toArray(new String[0]));
    args = (List<String>) opts.nonOptionArguments();

    if (args.size() != 3) {
      err.println("Usage: uri protocol_file message_name (-data d | -file f)");
      p.printHelpOn(err);
      return 1;
    }

    URI uri = new URI(args.get(0));
    Protocol protocol = Protocol.parse(new File(args.get(1)));
    String messageName = args.get(2);
    Message message = protocol.getMessages().get(messageName);
    if (message == null) {
      err.println(String.format("No message named '%s' found in protocol '%s'.", messageName, protocol));
      return 1;
    }

    Object datum;
    if (data.value(opts) != null) {
      datum = Util.jsonToGenericDatum(message.getRequest(), data.value(opts));
    } else if (file.value(opts) != null) {
      datum = Util.datumFromFile(message.getRequest(), file.value(opts));
    } else {
      err.println("One of -data or -file must be specified.");
      return 1;
    }

    GenericRequestor client = new GenericRequestor(protocol, Ipc.createTransceiver(uri));
    Object response = client.request(message.getName(), datum);
    dumpJson(out, message.getResponse(), response);
    return 0;
  }

  private void dumpJson(PrintStream out, Schema schema, Object datum) throws IOException {
    DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(schema, out, true);
    writer.write(datum, jsonEncoder);
    jsonEncoder.flush();
    out.println();
    out.flush();
  }

}
