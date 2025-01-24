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
package org.apache.avro.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This class has more exhaustive tests for Blocking IO. The reason we have both
 * TestBlockingIO and TestBlockingIO2 is that with the mnemonics used in
 * TestBlockingIO2, it is hard to test skip() operations. and with the test
 * infrastructure of TestBlockingIO, it is hard to test enums, unions etc.
 */
@RunWith(Parameterized.class)
public class TestBlockingIO2 {
  private final Decoder decoder;
  private final String calls;
  private Object[] values;
  private String msg;

  public TestBlockingIO2(int bufferSize, int skipLevel, String calls) throws IOException {

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    EncoderFactory factory = new EncoderFactory().configureBlockSize(bufferSize);
    Encoder encoder = factory.blockingBinaryEncoder(os, null);
    this.values = TestValidatingIO.randomValues(calls);

    TestValidatingIO.generate(encoder, calls, values);
    encoder.flush();

    byte[] bb = os.toByteArray();

    decoder = DecoderFactory.get().binaryDecoder(bb, null);
    this.calls = calls;
    this.msg = "Case: { " + bufferSize + ", " + skipLevel + ", \"" + calls + "\" }";
  }

  @Test
  public void testScan() throws IOException {
    TestValidatingIO.check(msg, decoder, calls, values, -1);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { 64, 0, "" }, { 64, 0, "S0" }, { 64, 0, "S3" }, { 64, 0, "S64" },
        { 64, 0, "S65" }, { 64, 0, "S100" }, { 64, 1, "[]" }, { 64, 1, "[c1sS0]" }, { 64, 1, "[c1sS3]" },
        { 64, 1, "[c1sS61]" }, { 64, 1, "[c1sS62]" }, { 64, 1, "[c1sS64]" }, { 64, 1, "[c1sS65]" },
        { 64, 1, "[c2sS0sS0]" }, { 64, 1, "[c2sS0sS10]" }, { 64, 1, "[c2sS0sS63]" }, { 64, 1, "[c2sS0sS64]" },
        { 64, 1, "[c2sS0sS65]" }, { 64, 1, "[c2sS10sS0]" }, { 64, 1, "[c2sS10sS10]" }, { 64, 1, "[c2sS10sS51]" },
        { 64, 1, "[c2sS10sS52]" }, { 64, 1, "[c2sS10sS54]" }, { 64, 1, "[c2sS10sS55]" }, { 64, 1, "[c3sS0sS0sS0]" },
        { 64, 1, "[c3sS0sS0sS63]" }, { 64, 1, "[c3sS0sS0sS64]" }, { 64, 1, "[c3sS0sS0sS65]" },
        { 64, 1, "[c3sS10sS20sS10]" }, { 64, 1, "[c3sS10sS20sS23]" }, { 64, 1, "[c3sS10sS20sS24]" },
        { 64, 1, "[c3sS10sS20sS25]" }, { 64, 1, "[c1s[]]" }, { 64, 1, "[c1s[c1sS0]]" }, { 64, 1, "[c1s[c1sS10]]" },
        { 64, 1, "[c2s[c1sS10]s[]]" }, { 64, 1, "[c2s[c1sS59]s[]]" }, { 64, 1, "[c2s[c1sS60]s[]]" },
        { 64, 1, "[c2s[c1sS100]s[]]" }, { 64, 1, "[c2s[c2sS10sS53]s[]]" }, { 64, 1, "[c2s[c2sS10sS54]s[]]" },
        { 64, 1, "[c2s[c2sS10sS55]s[]]" },

        { 64, 1, "[c2s[]s[c1sS0]]" }, { 64, 1, "[c2s[]s[c1sS10]]" }, { 64, 1, "[c2s[]s[c1sS63]]" },
        { 64, 1, "[c2s[]s[c1sS64]]" }, { 64, 1, "[c2s[]s[c1sS65]]" }, { 64, 1, "[c2s[]s[c2sS10sS53]]" },
        { 64, 1, "[c2s[]s[c2sS10sS54]]" }, { 64, 1, "[c2s[]s[c2sS10sS55]]" },

        { 64, 1, "[c1s[c1sS10]]" }, { 64, 1, "[c1s[c1sS62]]" }, { 64, 1, "[c1s[c1sS63]]" }, { 64, 1, "[c1s[c1sS64]]" },

        { 64, 1, "[c1s[c2sS10sS10]]" }, { 64, 1, "[c1s[c2sS10sS52]]" }, { 64, 1, "[c1s[c2sS10sS53]]" },
        { 64, 1, "[c1s[c2sS10sS54]]" },

        { 64, 1, "[c1s[c1s[c1sS10]]]" }, { 64, 1, "[c1s[c1s[c1sS62]]]" }, { 64, 1, "[c1s[c1s[c1sS63]]]" },
        { 64, 1, "[c1s[c1s[c1sS64]]]" },

        { 64, 1, "[c1s[c1s[c2sS10sS10]]]" }, { 64, 1, "[c1s[c1s[c2sS10sS52]]]" }, { 64, 1, "[c1s[c1s[c2sS10sS53]]]" },
        { 64, 1, "[c1s[c1s[c2sS10sS54]]]" },

        { 64, 1, "[c1s[c2sS10s[c1sS10]]]" }, { 64, 1, "[c1s[c2sS10s[c1sS52]]]" }, { 64, 1, "[c1s[c2sS10s[c1sS53]]]" },
        { 64, 1, "[c1s[c2sS10s[c1sS54]]]" },

        { 64, 1, "{}" }, { 64, 1, "{c1sK5S1}" }, { 64, 1, "{c1sK5[]}" }, { 100, 1, "{c1sK5[]}" },
        { 100, 1, "{c1sK5[c1sS10]}" },

        { 100, 1, "{c1sK5e10}" }, { 100, 1, "{c1sK5U1S10}" }, { 100, 1, "{c1sK5f10S10}" }, { 100, 1, "{c1sK5NS10}" },
        { 100, 1, "{c1sK5BS10}" }, { 100, 1, "{c1sK5IS10}" }, { 100, 1, "{c1sK5LS10}" }, { 100, 1, "{c1sK5FS10}" },
        { 100, 1, "{c1sK5DS10}" }, });
  }
}
