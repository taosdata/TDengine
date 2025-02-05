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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.io.TestValidatingIO.Encoding;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestResolvingIO {

  protected final Encoding eEnc;
  protected final int iSkipL;
  protected final String sJsWrtSchm;
  protected final String sWrtCls;
  protected final String sJsRdrSchm;
  protected final String sRdrCls;

  public TestResolvingIO(Encoding encoding, int skipLevel, String jsonWriterSchema, String writerCalls,
      String jsonReaderSchema, String readerCalls) {
    this.eEnc = encoding;
    this.iSkipL = skipLevel;
    this.sJsWrtSchm = jsonWriterSchema;
    this.sWrtCls = writerCalls;
    this.sJsRdrSchm = jsonReaderSchema;
    this.sRdrCls = readerCalls;
  }

  @Test
  public void testIdentical() throws IOException {
    performTest(eEnc, iSkipL, sJsWrtSchm, sWrtCls, sJsWrtSchm, sWrtCls);
  }

  private static final int COUNT = 10;

  @Test
  public void testCompatible() throws IOException {
    performTest(eEnc, iSkipL, sJsWrtSchm, sWrtCls, sJsRdrSchm, sRdrCls);
  }

  private void performTest(Encoding encoding, int skipLevel, String jsonWriterSchema, String writerCalls,
      String jsonReaderSchema, String readerCalls) throws IOException {
    for (int i = 0; i < COUNT; i++) {
      testOnce(jsonWriterSchema, writerCalls, jsonReaderSchema, readerCalls, encoding, skipLevel);
    }
  }

  private void testOnce(String jsonWriterSchema, String writerCalls, String jsonReaderSchema, String readerCalls,
      Encoding encoding, int skipLevel) throws IOException {
    Object[] values = TestValidatingIO.randomValues(writerCalls);
    Object[] expected = TestValidatingIO.randomValues(readerCalls);

    Schema writerSchema = new Schema.Parser().parse(jsonWriterSchema);
    byte[] bytes = TestValidatingIO.make(writerSchema, writerCalls, values, encoding);
    Schema readerSchema = new Schema.Parser().parse(jsonReaderSchema);
    TestValidatingIO.print(encoding, skipLevel, writerSchema, readerSchema, values, expected);
    check(writerSchema, readerSchema, bytes, readerCalls, expected, encoding, skipLevel);
  }

  static void check(Schema wsc, Schema rsc, byte[] bytes, String calls, Object[] values, Encoding encoding,
      int skipLevel) throws IOException {
    // TestValidatingIO.dump(bytes);
    // System.out.println(new String(bytes, "UTF-8"));
    Decoder bvi = null;
    switch (encoding) {
    case BINARY:
    case BLOCKING_BINARY:
      bvi = DecoderFactory.get().binaryDecoder(bytes, null);
      break;
    case JSON:
      InputStream in = new ByteArrayInputStream(bytes);
      bvi = new JsonDecoder(wsc, in);
      break;
    }
    Decoder vi = new ResolvingDecoder(wsc, rsc, bvi);
    String msg = "Error in resolving case: w=" + wsc + ", r=" + rsc;
    TestValidatingIO.check(msg, vi, calls, values, skipLevel);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data2() {
    return Arrays.asList(TestValidatingIO.convertTo2dArray(encodings, skipLevels, testSchemas()));
  }

  static Object[][] encodings = new Object[][] { { Encoding.BINARY }, { Encoding.BLOCKING_BINARY }, { Encoding.JSON } };
  static Object[][] skipLevels = new Object[][] { { -1 }, { 0 }, { 1 }, { 2 } };

  private static Object[][] testSchemas() {
    // The mnemonics are the same as {@link TestValidatingIO#testSchemas}
    return new Object[][] { { "\"int\"", "I", "\"float\"", "F" }, { "\"int\"", "I", "\"double\"", "D" },
        { "\"int\"", "I", "\"long\"", "L" }, { "\"long\"", "L", "\"float\"", "F" },
        { "\"long\"", "L", "\"double\"", "D" }, { "\"float\"", "F", "\"double\"", "D" },

        { "{\"type\":\"array\", \"items\": \"int\"}", "[]", "{\"type\":\"array\", \"items\": \"long\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[]", "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[]", "{\"type\":\"array\", \"items\": \"double\"}", "[]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[]", "{\"type\":\"array\", \"items\": \"double\"}", "[]" },

        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]", "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]", "{\"type\":\"array\", \"items\": \"double\"}",
            "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]", "{\"type\":\"array\", \"items\": \"double\"}",
            "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]", "{\"type\":\"array\", \"items\": \"double\"}",
            "[c1sD]" },

        { "{\"type\":\"map\", \"values\": \"int\"}", "{}", "{\"type\":\"map\", \"values\": \"long\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{}", "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{}", "{\"type\":\"map\", \"values\": \"double\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{}", "{\"type\":\"map\", \"values\": \"double\"}", "{}" },

        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}", "{\"type\":\"map\", \"values\": \"long\"}",
            "{c1sK5L}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}", "{\"type\":\"map\", \"values\": \"double\"}",
            "{c1sK5D}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sK5L}", "{\"type\":\"map\", \"values\": \"double\"}",
            "{c1sK5D}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sK5F}", "{\"type\":\"map\", \"values\": \"double\"}",
            "{c1sK5D}" },

        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"long\"}]}", "L" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"int\"}]}", "I",
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"double\"}]}", "D" },

        // multi-field record with promotions
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f0\", \"type\":\"boolean\"},"
            + "{\"name\":\"f1\", \"type\":\"int\"}," + "{\"name\":\"f2\", \"type\":\"float\"},"
            + "{\"name\":\"f3\", \"type\":\"bytes\"}," + "{\"name\":\"f4\", \"type\":\"string\"}]}", "BIFbS",
            "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f0\", \"type\":\"boolean\"},"
                + "{\"name\":\"f1\", \"type\":\"long\"}," + "{\"name\":\"f2\", \"type\":\"double\"},"
                + "{\"name\":\"f3\", \"type\":\"string\"}," + "{\"name\":\"f4\", \"type\":\"bytes\"}]}",
            "BLDSb" },

        { "[\"int\"]", "U0I", "[\"long\"]", "U0L" }, { "[\"int\"]", "U0I", "[\"double\"]", "U0D" },
        { "[\"long\"]", "U0L", "[\"double\"]", "U0D" }, { "[\"float\"]", "U0F", "[\"double\"]", "U0D" },

        { "\"int\"", "I", "[\"int\"]", "U0I" },

        { "[\"int\"]", "U0I", "\"int\"", "I" }, { "[\"int\"]", "U0I", "\"long\"", "L" },

        { "[\"boolean\", \"int\"]", "U1I", "[\"boolean\", \"long\"]", "U1L" },
        { "[\"boolean\", \"int\"]", "U1I", "[\"long\", \"boolean\"]", "U0L" }, };
  }
}
