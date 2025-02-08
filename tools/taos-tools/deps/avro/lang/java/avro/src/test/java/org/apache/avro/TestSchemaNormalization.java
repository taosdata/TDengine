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
package org.apache.avro;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;

import org.apache.avro.util.CaseFinder;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class TestSchemaNormalization {

  @RunWith(Parameterized.class)
  public static class TestCanonical {
    String input, expectedOutput;

    public TestCanonical(String i, String o) {
      input = i;
      expectedOutput = o;
    }

    @Parameters
    public static List<Object[]> cases() throws IOException {
      return CaseFinder.find(data(), "canonical", new ArrayList<>());
    }

    @Test
    public void testCanonicalization() throws Exception {
      assertEquals(SchemaNormalization.toParsingForm(new Schema.Parser().parse(input)), expectedOutput);
    }
  }

  @RunWith(Parameterized.class)
  public static class TestFingerprint {
    String input, expectedOutput;

    public TestFingerprint(String i, String o) {
      input = i;
      expectedOutput = o;
    }

    @Parameters
    public static List<Object[]> cases() throws IOException {
      return CaseFinder.find(data(), "fingerprint", new ArrayList<>());
    }

    @Test
    public void testCanonicalization() throws Exception {
      Schema s = new Schema.Parser().parse(input);
      long carefulFP = altFingerprint(SchemaNormalization.toParsingForm(s));
      assertEquals(carefulFP, Long.parseLong(expectedOutput));
      assertEqHex(carefulFP, SchemaNormalization.parsingFingerprint64(s));
    }
  }

  // see AVRO-1493
  @RunWith(Parameterized.class)
  public static class TestFingerprintInternationalization {
    String input, expectedOutput;

    public TestFingerprintInternationalization(String i, String o) {
      input = i;
      expectedOutput = o;
    }

    @Parameters
    public static List<Object[]> cases() throws IOException {
      return CaseFinder.find(data(), "fingerprint", new ArrayList<>());
    }

    @Test
    public void testCanonicalization() throws Exception {
      Locale originalDefaultLocale = Locale.getDefault();
      Locale.setDefault(Locale.forLanguageTag("tr"));
      Schema s = new Schema.Parser().parse(input);
      long carefulFP = altFingerprint(SchemaNormalization.toParsingForm(s));
      assertEquals(carefulFP, Long.parseLong(expectedOutput));
      assertEqHex(carefulFP, SchemaNormalization.parsingFingerprint64(s));
      Locale.setDefault(originalDefaultLocale);
    }
  }

  private static String DATA_FILE = (System.getProperty("share.dir", "../../../share") + "/test/data/schema-tests.txt");

  private static BufferedReader data() throws IOException {
    return Files.newBufferedReader(Paths.get(DATA_FILE), UTF_8);
  }

  /**
   * Compute the fingerprint of <i>bytes[s,s+l)</i> using a slow algorithm that's
   * an alternative to that implemented in {@link SchemaNormalization}. Algo from
   * Broder93 ("Some applications of Rabin's fingerprinting method").
   */
  public static long altFingerprint(String s) {
    // In our algorithm, we multiply all inputs by x^64 (which is
    // equivalent to prepending it with a single "1" bit followed
    // by 64 zero bits). This both deals with the fact that
    // CRCs ignore leading zeros, and also ensures some degree of
    // randomness for small inputs

    long tmp = altExtend(SchemaNormalization.EMPTY64, 64, ONE, s.getBytes(UTF_8));
    return altExtend(SchemaNormalization.EMPTY64, 64, tmp, POSTFIX);
  }

  private static long altExtend(long poly, int degree, long fp, byte[] b) {
    final long overflowBit = 1L << (64 - degree);
    for (byte b1 : b) {
      for (int j = 1; j < 129; j = j << 1) {
        boolean overflow = (0 != (fp & overflowBit));
        fp >>>= 1;
        if (0 != (j & b1))
          fp |= ONE; // shift in the input bit
        if (overflow) {
          fp ^= poly; // hi-order coeff of poly kills overflow bit
        }
      }
    }
    return fp;
  }

  private static final long ONE = 0x8000000000000000L;
  private static final byte[] POSTFIX = { 0, 0, 0, 0, 0, 0, 0, 0 };

  private static void assertEqHex(long expected, long actual) {
    String m = format("0x%016x != 0x%016x", expected, actual);
    assertTrue(m, expected == actual);
  }

  private static String format(String f, Object... args) {
    return (new Formatter()).format(f, args).toString();
  }
}
