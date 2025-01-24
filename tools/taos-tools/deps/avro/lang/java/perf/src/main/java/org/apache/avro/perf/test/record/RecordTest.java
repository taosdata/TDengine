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

package org.apache.avro.perf.test.record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.perf.test.BasicRecord;
import org.apache.avro.perf.test.BasicState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class RecordTest {

  private static final String RECORD_SCHEMA = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n" + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n" + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n" + "{ \"name\": \"f6\", \"type\": \"int\" }\n" + "] }";

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void decode(final TestStateDecode state, final Blackhole blackHole) throws Exception {
    final Decoder d = state.decoder;
    double sd = 0.0;
    int si = 0;

    for (int i = 0; i < state.getBatchSize(); i++) {
      sd += d.readDouble();
      sd += d.readDouble();
      sd += d.readDouble();

      si += d.readInt();
      si += d.readInt();
      si += d.readInt();
    }

    blackHole.consume(sd);
    blackHole.consume(si);
  }

  @State(Scope.Thread)
  public static class TestStateDecode extends BasicState {

    private final Schema readerSchema;

    private byte[] testData;
    private Decoder decoder;

    public TestStateDecode() {
      super();
      this.readerSchema = new Schema.Parser().parse(RECORD_SCHEMA);
    }

    /**
     * Generate test data.
     *
     * @throws IOException Could not setup test data
     */
    @Setup(Level.Trial)
    public void doSetupTrial() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Encoder encoder = super.newEncoder(true, baos);

      for (int i = 0; i < getBatchSize(); i++) {
        final BasicRecord r = new BasicRecord(super.getRandom());
        encoder.writeDouble(r.f1);
        encoder.writeDouble(r.f2);
        encoder.writeDouble(r.f3);
        encoder.writeInt(r.f4);
        encoder.writeInt(r.f5);
        encoder.writeInt(r.f6);
      }

      this.testData = baos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() throws Exception {
      this.decoder = DecoderFactory.get().validatingDecoder(readerSchema, super.newDecoder(this.testData));
    }
  }
}
