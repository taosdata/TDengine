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
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.perf.test.BasicState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class GenericStringTest {

  private static final String GENERIC_STRINGS = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"string\" },\n" + "{ \"name\": \"f2\", \"type\": \"string\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"string\" }\n" + "] }";

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void encode(final TestStateEncode state) throws Exception {
    final Encoder e = state.encoder;
    final GenericDatumWriter<Object> writer = new GenericDatumWriter<>(state.readerSchema);
    for (final GenericRecord rec : state.testData) {
      writer.write(rec, e);
    }
  }

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void decode(final Blackhole blackhole, final TestStateDecode state) throws Exception {
    final Decoder d = state.decoder;
    final GenericDatumReader<Object> reader = new GenericDatumReader<>(state.readerSchema);
    for (int i = 0; i < state.getBatchSize(); i++) {
      blackhole.consume(reader.read(null, d));
    }
  }

  @State(Scope.Thread)
  public static class TestStateEncode extends BasicState {

    private final Schema readerSchema;

    private GenericRecord[] testData;
    private Encoder encoder;

    public TestStateEncode() {
      super();
      this.readerSchema = new Schema.Parser().parse(GENERIC_STRINGS);
    }

    /**
     * Setup the trial data.
     *
     * @throws IOException Could not setup test data
     */
    @Setup(Level.Trial)
    public void doSetupTrial() throws Exception {
      this.encoder = super.newEncoder(false, getNullOutputStream());
      this.testData = new GenericRecord[getBatchSize()];

      for (int i = 0; i < testData.length; i++) {
        GenericRecord rec = new GenericData.Record(readerSchema);
        rec.put(0, randomString(super.getRandom()));
        rec.put(1, randomString(super.getRandom()));
        rec.put(2, randomString(super.getRandom()));
        testData[i] = rec;
      }
    }
  }

  @State(Scope.Thread)
  public static class TestStateDecode extends BasicState {

    private final Schema readerSchema;

    private byte[] testData;
    private Decoder decoder;

    public TestStateDecode() {
      super();
      this.readerSchema = new Schema.Parser().parse(GENERIC_STRINGS);
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
        encoder.writeString(randomString(super.getRandom()));
        encoder.writeString(randomString(super.getRandom()));
        encoder.writeString(randomString(super.getRandom()));
      }

      this.testData = baos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() throws Exception {
      this.decoder = DecoderFactory.get().validatingDecoder(readerSchema, super.newDecoder(this.testData));
    }
  }

  private static String randomString(Random r) {
    char[] data = new char[r.nextInt(70)];
    for (int j = 0; j < data.length; j++) {
      data[j] = (char) ('a' + r.nextInt('z' - 'a'));
    }
    return new String(data);
  }
}
