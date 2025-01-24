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

package org.apache.avro.perf.test.generic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.perf.test.BasicState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class GenericWithPromotionTest {

  private static final String RECORD_SCHEMA = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n" + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n" + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n" + "{ \"name\": \"f6\", \"type\": \"int\" }\n" + "] }";

  private static final String RECORD_SCHEMA_WITH_PROMOTION = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n" + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n" + "{ \"name\": \"f4\", \"type\": \"long\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"long\" },\n" + "{ \"name\": \"f6\", \"type\": \"long\" }\n" + "] }";

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void decode(final Blackhole blackhole, final TestStateDecode state) throws Exception {
    final Decoder d = state.decoder;
    final GenericDatumReader<Object> reader = new GenericDatumReader<>(state.writerSchema, state.readerSchema);
    for (int i = 0; i < state.getBatchSize(); i++) {
      blackhole.consume(reader.read(null, d));
    }
  }

  @State(Scope.Thread)
  public static class TestStateDecode extends BasicState {

    private final Schema readerSchema;
    private final Schema writerSchema;

    private byte[] testData;
    private Decoder decoder;

    public TestStateDecode() {
      super();
      this.readerSchema = new Schema.Parser().parse(RECORD_SCHEMA_WITH_PROMOTION);
      this.writerSchema = new Schema.Parser().parse(RECORD_SCHEMA);
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

      final GenericDatumWriter<Object> writer = new GenericDatumWriter<>(this.writerSchema);

      final Random r = super.getRandom();
      for (int i = 0; i < getBatchSize(); i++) {
        final GenericRecord rec = new GenericData.Record(writerSchema);
        rec.put(0, r.nextDouble());
        rec.put(1, r.nextDouble());
        rec.put(2, r.nextDouble());
        rec.put(3, r.nextInt());
        rec.put(4, r.nextInt());
        rec.put(5, r.nextInt());
        writer.write(rec, encoder);
      }

      encoder.flush();

      this.testData = baos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() throws Exception {
      this.decoder = super.newDecoder(this.testData);
    }
  }
}
