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

public class GenericNestedTest {

  private static final String NESTED_RECORD_SCHEMA = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \n" + "{ \"type\": \"record\", \"name\": \"D\", \"fields\": [\n"
      + "{\"name\": \"dbl\", \"type\": \"double\" }]\n" + "} },\n" + "{ \"name\": \"f2\", \"type\": \"D\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"D\" },\n" + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n" + "{ \"name\": \"f6\", \"type\": \"int\" }\n" + "] }";

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void encode(final TestStateEncode state) throws Exception {
    final Encoder e = state.encoder;
    for (final GenericRecord rec : state.testData) {
      GenericRecord inner;
      inner = (GenericRecord) rec.get(0);
      e.writeDouble((Double) inner.get(0));
      inner = (GenericRecord) rec.get(1);
      e.writeDouble((Double) inner.get(0));
      inner = (GenericRecord) rec.get(2);
      e.writeDouble((Double) inner.get(0));
      e.writeInt((Integer) rec.get(3));
      e.writeInt((Integer) rec.get(4));
      e.writeInt((Integer) rec.get(5));
    }
  }

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void decode(final Blackhole blackhole, final TestStateDecode state) throws Exception {
    final Decoder d = state.decoder;
    Schema doubleSchema = state.schema.getFields().get(0).schema();
    for (int i = 0; i < state.getBatchSize(); i++) {
      GenericRecord rec = new GenericData.Record(state.schema);
      GenericRecord inner;
      inner = new GenericData.Record(doubleSchema);
      inner.put(0, d.readDouble());
      rec.put(0, inner);
      inner = new GenericData.Record(doubleSchema);
      inner.put(0, d.readDouble());
      rec.put(1, inner);
      inner = new GenericData.Record(doubleSchema);
      inner.put(0, d.readDouble());
      rec.put(2, inner);
      rec.put(3, d.readInt());
      rec.put(4, d.readInt());
      rec.put(5, d.readInt());
    }
  }

  @State(Scope.Thread)
  public static class TestStateEncode extends BasicState {

    private final Schema schema;

    private GenericRecord[] testData;
    private Encoder encoder;

    public TestStateEncode() {
      super();
      this.schema = new Schema.Parser().parse(NESTED_RECORD_SCHEMA);
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

      final Random r = super.getRandom();
      Schema doubleSchema = schema.getFields().get(0).schema();
      for (int i = 0; i < testData.length; i++) {
        GenericRecord rec = new GenericData.Record(schema);
        GenericRecord inner;
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, r.nextDouble());
        rec.put(0, inner);
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, r.nextDouble());
        rec.put(1, inner);
        inner = new GenericData.Record(doubleSchema);
        inner.put(0, r.nextDouble());
        rec.put(2, inner);
        rec.put(3, r.nextInt());
        rec.put(4, r.nextInt());
        rec.put(5, r.nextInt());
        testData[i] = rec;
      }
    }
  }

  @State(Scope.Thread)
  public static class TestStateDecode extends BasicState {

    private final Schema schema;

    private byte[] testData;
    private Decoder decoder;

    public TestStateDecode() {
      super();
      this.schema = new Schema.Parser().parse(NESTED_RECORD_SCHEMA);
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

      final Random r = super.getRandom();
      for (int i = 0; i < getBatchSize(); i++) {
        encoder.writeDouble(r.nextDouble());
        encoder.writeDouble(r.nextDouble());
        encoder.writeDouble(r.nextDouble());
        encoder.writeInt(r.nextInt());
        encoder.writeInt(r.nextInt());
        encoder.writeInt(r.nextInt());
      }

      this.testData = baos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() throws Exception {
      this.decoder = DecoderFactory.get().validatingDecoder(schema, super.newDecoder(this.testData));
    }
  }
}
