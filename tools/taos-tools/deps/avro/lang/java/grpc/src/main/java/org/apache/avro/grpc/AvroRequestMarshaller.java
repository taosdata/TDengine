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

package org.apache.avro.grpc;

import com.google.common.io.ByteStreams;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import io.grpc.MethodDescriptor;
import io.grpc.Status;

/** Marshaller for Avro RPC request. */
public class AvroRequestMarshaller implements MethodDescriptor.Marshaller<Object[]> {
  private static final EncoderFactory ENCODER_FACTORY = new EncoderFactory();
  private static final DecoderFactory DECODER_FACTORY = new DecoderFactory();
  private final Protocol.Message message;

  public AvroRequestMarshaller(Protocol.Message message) {
    this.message = message;
  }

  @Override
  public InputStream stream(Object[] value) {
    return new AvroRequestInputStream(value, message);
  }

  @Override
  public Object[] parse(InputStream stream) {
    try {
      BinaryDecoder in = DECODER_FACTORY.binaryDecoder(stream, null);
      Schema reqSchema = message.getRequest();
      GenericRecord request = (GenericRecord) new SpecificDatumReader<>(reqSchema).read(null, in);
      Object[] args = new Object[reqSchema.getFields().size()];
      int i = 0;
      for (Schema.Field field : reqSchema.getFields()) {
        args[i++] = request.get(field.name());
      }
      return args;
    } catch (IOException e) {
      throw Status.INTERNAL.withCause(e).withDescription("Error deserializing avro request arguments")
          .asRuntimeException();
    } finally {
      AvroGrpcUtils.skipAndCloseQuietly(stream);
    }
  }

  private static class AvroRequestInputStream extends AvroInputStream {
    private final Protocol.Message message;
    private Object[] args;

    AvroRequestInputStream(Object[] args, Protocol.Message message) {
      this.args = args;
      this.message = message;
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
      int written;
      if (getPartial() != null) {
        written = (int) ByteStreams.copy(getPartial(), target);
      } else {
        Schema reqSchema = message.getRequest();
        CountingOutputStream outputStream = new CountingOutputStream(target);
        BinaryEncoder out = ENCODER_FACTORY.binaryEncoder(outputStream, null);
        int i = 0;
        for (Schema.Field param : reqSchema.getFields()) {
          new SpecificDatumWriter<>(param.schema()).write(args[i++], out);
        }
        out.flush();
        args = null;
        written = outputStream.getWrittenCount();
      }
      return written;
    }
  }
}
