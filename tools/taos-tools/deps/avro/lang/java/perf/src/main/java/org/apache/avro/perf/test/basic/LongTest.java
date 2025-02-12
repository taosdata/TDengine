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

public class LongTest {

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public void encode(final TestStateEncode state) throws Exception {
    final Encoder e = state.encoder;
    for (int i = 0; i < state.getBatchSize(); i += 4) {
      e.writeLong(state.testData[i + 0]);
      e.writeLong(state.testData[i + 1]);
      e.writeLong(state.testData[i + 2]);
      e.writeLong(state.testData[i + 3]);
    }
  }

  @Benchmark
  @OperationsPerInvocation(BasicState.BATCH_SIZE)
  public long decode(final TestStateDecode state) throws Exception {
    final Decoder d = state.decoder;
    long total = 0;
    for (int i = 0; i < state.getBatchSize(); i += 4) {
      total += d.readLong();
      total += d.readLong();
      total += d.readLong();
      total += d.readLong();
    }
    return total;
  }

  @State(Scope.Thread)
  public static class TestStateEncode extends BasicState {

    private long[] testData;
    private Encoder encoder;

    public TestStateEncode() {
      super();
    }

    /**
     * Avro uses Zig-Zag variable length encoding for numeric values. Ensure there
     * are some int of each possible size.
     *
     * @throws IOException Could not setup test data
     */
    @Setup(Level.Trial)
    public void doSetupTrial() throws Exception {
      this.encoder = super.newEncoder(false, getNullOutputStream());
      this.testData = new long[getBatchSize()];

      for (int i = 0; i < testData.length; i += 4) {
        // half fit in 1, half in 2
        testData[i + 0] = super.getRandom().nextLong() % 0x7FL;
        // half fit in <=3, half in 4
        testData[i + 1] = super.getRandom().nextLong() % 0x1FFFFFL;
        // half in <=5, half in 6
        testData[i + 2] = super.getRandom().nextLong() % 0x3FFFFFFFFL;
        // half in <=8, half in 9
        testData[i + 3] = super.getRandom().nextLong() % 0x1FFFFFFFFFFFFL;
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
     * Avro uses Zig-Zag variable length encoding for numeric values. Ensure there
     * are some numeric values of each possible size.
     *
     * @throws IOException Could not setup test data
     */
    @Setup(Level.Trial)
    public void doSetupTrial() throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Encoder encoder = super.newEncoder(true, baos);

      for (int i = 0; i < getBatchSize(); i += 4) {
        // half fit in 1, half in 2
        encoder.writeLong(super.getRandom().nextLong() % 0x7FL);

        // half fit in <=3, half in 4
        encoder.writeLong(super.getRandom().nextLong() % 0x1FFFFFL);

        // half in <=5, half in 6
        encoder.writeLong(super.getRandom().nextLong() % 0x3FFFFFFFFL);

        // half in <=8, half in 9
        encoder.writeLong(super.getRandom().nextLong() % 0x1FFFFFFFFFFFFL);
      }

      this.testData = baos.toByteArray();
    }

    @Setup(Level.Invocation)
    public void doSetupInvocation() throws Exception {
      this.decoder = super.newDecoder(this.testData);
    }
  }
}
