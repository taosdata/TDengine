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

package org.apache.avro.perf.test.reflect;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.perf.test.BasicArrayState;
import org.apache.avro.perf.test.BasicState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

public class ReflectFloatArrayTest {

  private static final int ARRAY_SIZE = 10;

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void encode(final TestStateEncode state) throws Exception {
    for (final float[] r : state.testData) {
      state.datumWriter.write(r, state.encoder);
    }
  }

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void decode(final Blackhole blackhole, final TestStateDecode state) throws Exception {
    final Decoder d = state.decoder;
    final ReflectDatumReader<float[]> datumReader = new ReflectDatumReader<>(state.schema);

    for (int i = 0; i < state.getBatchSize(); i++) {
      blackhole.consume(datumReader.read(null, d));
    }
  }

  @State(Scope.Thread)
  public static class TestStateEncode extends BasicArrayState {

    private final Schema schema;

    private float[][] testData;
    private Encoder encoder;
    private ReflectDatumWriter<float[]> datumWriter;

    public TestStateEncode() {
      super(ARRAY_SIZE);
      final String jsonText = ReflectData.get().getSchema(float[].class).toString();
      this.schema = new Schema.Parser().parse(jsonText);
    }

    /**
     * Setup the trial data.
     *
     * @throws IOException Could not setup test data
     */
    @Setup(Level.Trial)
    public void doSetupTrial() throws Exception {
      this.encoder = super.newEncoder(false, getNullOutputStream());
      this.datumWriter = new ReflectDatumWriter<>(schema);

      this.testData = new float[getBatchSize()][];

      for (int i = 0; i < testData.length; i++) {
        this.testData[i] = populateFloatArray(getRandom(), getArraySize());
      }
    }
  }

  @State(Scope.Thread)
  public static class TestStateDecode extends BasicArrayState {

    private final Schema schema;

    private byte[] testData;
    private Decoder decoder;

    public TestStateDecode() {
      super(ARRAY_SIZE);
      final String jsonText = ReflectData.get().getSchema(float[].class).toString();
      this.schema = new Schema.Parser().parse(jsonText);
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
      ReflectDatumWriter<float[]> writer = new ReflectDatumWriter<>(schema);

      for (int i = 0; i < getBatchSize(); i++) {
        final float[] r = populateFloatArray(getRandom(), getArraySize());
        writer.write(r, encoder);
      }

      this.testData = baos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() throws Exception {
      this.decoder = DecoderFactory.get().validatingDecoder(schema, super.newDecoder(this.testData));
    }
  }

  static float[] populateFloatArray(final Random r, final int size) {
    float[] result = new float[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextFloat();
    }
    return result;
  }
}
