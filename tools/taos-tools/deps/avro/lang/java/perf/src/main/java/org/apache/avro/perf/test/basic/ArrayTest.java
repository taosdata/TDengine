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

package org.apache.avro.perf.test.basic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.perf.test.BasicState;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

public class ArrayTest {

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void encode(final TestStateEncode state) throws Exception {
    final Encoder e = state.encoder;
    final int items = state.getBatchSize() / 4;

    e.writeArrayStart();
    e.setItemCount(1);
    e.startItem();
    e.writeArrayStart();
    e.setItemCount(items);
    for (int i = 0; i < state.getBatchSize(); i += 4) {
      e.startItem();
      e.writeFloat(state.testData[i + 0]);
      e.writeFloat(state.testData[i + 1]);
      e.writeFloat(state.testData[i + 2]);
      e.writeFloat(state.testData[i + 3]);
    }
    e.writeArrayEnd();
    e.writeArrayEnd();
  }

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public float decode(final TestStateDecode state) throws Exception {
    final Decoder d = state.decoder;
    float total = 0.0f;
    d.readArrayStart();
    for (long i = d.readArrayStart(); i != 0; i = d.arrayNext()) {
      for (long j = 0; j < i; j++) {
        total += d.readFloat();
        total += d.readFloat();
        total += d.readFloat();
        total += d.readFloat();
      }
    }
    d.arrayNext();
    return total;
  }

  @State(Scope.Thread)
  public static class TestStateEncode extends BasicState {

    private float[] testData;
    private Encoder encoder;

    public TestStateEncode() {
      super();
    }

    /**
     * Setup each trial
     *
     * @throws IOException Could not setup test data
     */
    @Setup(Level.Trial)
    public void doSetupTrial() throws Exception {
      this.encoder = super.newEncoder(false, getNullOutputStream());
      this.testData = new float[getBatchSize()];

      for (int i = 0; i < testData.length; i++) {
        testData[i] = super.getRandom().nextFloat();
      }
    }
  }

  @State(Scope.Thread)
  public static class TestStateDecode extends BasicState {

    private byte[] testData;
    private Decoder decoder;

    public TestStateDecode() {
      super();
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

      final int items = getBatchSize() / 4;

      encoder.writeArrayStart();
      encoder.setItemCount(1);
      encoder.startItem();
      encoder.writeArrayStart();
      encoder.setItemCount(items);
      for (int i = 0; i < getBatchSize(); i += 4) {
        encoder.startItem();
        encoder.writeFloat(super.getRandom().nextFloat());
        encoder.writeFloat(super.getRandom().nextFloat());
        encoder.writeFloat(super.getRandom().nextFloat());
        encoder.writeFloat(super.getRandom().nextFloat());
      }
      encoder.writeArrayEnd();
      encoder.writeArrayEnd();

      this.testData = baos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() throws Exception {
      this.decoder = super.newDecoder(this.testData);
    }
  }
}
