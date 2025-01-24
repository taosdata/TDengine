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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestBlockingIO {

  private final int iSize;
  private final int iDepth;
  private final String sInput;

  public TestBlockingIO(int sz, int dp, String inp) {
    this.iSize = sz;
    this.iDepth = dp;
    this.sInput = inp;
  }

  private static class Tests {
    private final JsonParser parser;
    private final Decoder input;
    private final int depth;

    public Tests(int bufferSize, int depth, String input) throws IOException {

      this.depth = depth;
      byte[] in = input.getBytes(StandardCharsets.UTF_8);
      JsonFactory f = new JsonFactory();
      JsonParser p = f.createParser(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));

      ByteArrayOutputStream os = new ByteArrayOutputStream();
      EncoderFactory factory = new EncoderFactory().configureBlockSize(bufferSize);
      Encoder cos = factory.blockingBinaryEncoder(os, null);
      serialize(cos, p, os);
      cos.flush();

      byte[] bb = os.toByteArray();
      // dump(bb);
      this.input = DecoderFactory.get().binaryDecoder(bb, null);
      this.parser = f.createParser(new ByteArrayInputStream(in));
    }

    public void scan() throws IOException {
      ArrayDeque<S> countStack = new ArrayDeque<>();
      long count = 0;
      while (parser.nextToken() != null) {
        switch (parser.getCurrentToken()) {
        case END_ARRAY:
          assertEquals(0, count);
          assertTrue(countStack.peek().isArray);
          count = countStack.pop().count;
          break;
        case END_OBJECT:
          assertEquals(0, count);
          assertFalse(countStack.peek().isArray);
          count = countStack.pop().count;
          break;
        case START_ARRAY:
          countStack.push(new S(count, true));
          count = input.readArrayStart();
          continue;
        case VALUE_STRING: {
          String s = parser.getText();
          int n = s.getBytes(StandardCharsets.UTF_8).length;
          checkString(s, input, n);
          break;
        }
        case FIELD_NAME: {
          String s = parser.getCurrentName();
          int n = s.getBytes(StandardCharsets.UTF_8).length;
          checkString(s, input, n);
          continue;
        }
        case START_OBJECT:
          countStack.push(new S(count, false));
          count = input.readMapStart();
          if (count < 0) {
            count = -count;
            input.readLong(); // byte count
          }
          continue;
        default:
          throw new RuntimeException("Unsupported: " + parser.getCurrentToken());
        }
        count--;
        if (count == 0) {
          count = countStack.peek().isArray ? input.arrayNext() : input.mapNext();
        }
      }
    }

    public void skip(int skipLevel) throws IOException {
      ArrayDeque<S> countStack = new ArrayDeque<>();
      long count = 0;
      while (parser.nextToken() != null) {
        switch (parser.getCurrentToken()) {
        case END_ARRAY:
          // assertEquals(0, count);
          assertTrue(countStack.peek().isArray);
          count = countStack.pop().count;
          break;
        case END_OBJECT:
          // assertEquals(0, count);
          assertFalse(countStack.peek().isArray);
          count = countStack.pop().count;
          break;
        case START_ARRAY:
          if (countStack.size() == skipLevel) {
            skipArray(parser, input, depth - skipLevel);
            break;
          } else {
            countStack.push(new S(count, true));
            count = input.readArrayStart();
            continue;
          }
        case VALUE_STRING: {
          if (countStack.size() == skipLevel) {
            input.skipBytes();
          } else {
            String s = parser.getText();
            int n = s.getBytes(StandardCharsets.UTF_8).length;
            checkString(s, input, n);
          }
          break;
        }
        case FIELD_NAME: {
          String s = parser.getCurrentName();
          int n = s.getBytes(StandardCharsets.UTF_8).length;
          checkString(s, input, n);
          continue;
        }
        case START_OBJECT:
          if (countStack.size() == skipLevel) {
            skipMap(parser, input, depth - skipLevel);
            break;
          } else {
            countStack.push(new S(count, false));
            count = input.readMapStart();
            if (count < 0) {
              count = -count;
              input.readLong(); // byte count
            }
            continue;
          }
        default:
          throw new RuntimeException("Unsupported: " + parser.getCurrentToken());
        }
        count--;
        if (count == 0) {
          count = countStack.peek().isArray ? input.arrayNext() : input.mapNext();
        }
      }
    }
  }

  protected static void dump(byte[] bb) {
    int col = 0;
    for (byte b : bb) {
      if (col % 16 == 0) {
        System.out.println();
      }
      col++;
      System.out.print(Integer.toHexString(b & 0xff) + " ");
    }
    System.out.println();
  }

  private static class S {
    public final long count;
    public final boolean isArray;

    public S(long count, boolean isArray) {
      this.count = count;
      this.isArray = isArray;
    }
  }

  @Test
  public void testScan() throws IOException {
    Tests t = new Tests(iSize, iDepth, sInput);
    t.scan();
  }

  @Test
  public void testSkip1() throws IOException {
    testSkip(iSize, iDepth, sInput, 0);
  }

  @Test
  public void testSkip2() throws IOException {
    testSkip(iSize, iDepth, sInput, 1);
  }

  @Test
  public void testSkip3() throws IOException {
    testSkip(iSize, iDepth, sInput, 2);
  }

  private void testSkip(int bufferSize, int depth, String input, int skipLevel) throws IOException {
    Tests t = new Tests(bufferSize, depth, input);
    t.skip(skipLevel);
  }

  private static void skipMap(JsonParser parser, Decoder input, int depth) throws IOException {
    for (long l = input.skipMap(); l != 0; l = input.skipMap()) {
      for (long i = 0; i < l; i++) {
        if (depth == 0) {
          input.skipBytes();
        } else {
          skipArray(parser, input, depth - 1);
        }
      }
    }
    parser.skipChildren();
  }

  private static void skipArray(JsonParser parser, Decoder input, int depth) throws IOException {
    for (long l = input.skipArray(); l != 0; l = input.skipArray()) {
      for (long i = 0; i < l; i++) {
        if (depth == 1) {
          input.skipBytes();
        } else {
          skipArray(parser, input, depth - 1);
        }
      }
    }
    parser.skipChildren();
  }

  private static void checkString(String s, Decoder input, int n) throws IOException {
    ByteBuffer buf = input.readBytes(null);
    assertEquals(n, buf.remaining());
    String s2 = new String(buf.array(), buf.position(), buf.remaining(), StandardCharsets.UTF_8);
    assertEquals(s, s2);
  }

  private static void serialize(Encoder cos, JsonParser p, ByteArrayOutputStream os) throws IOException {
    boolean[] isArray = new boolean[100];
    int[] counts = new int[100];
    int stackTop = -1;

    while (p.nextToken() != null) {
      switch (p.getCurrentToken()) {
      case END_ARRAY:
        assertTrue(isArray[stackTop]);
        cos.writeArrayEnd();
        stackTop--;
        break;
      case END_OBJECT:
        assertFalse(isArray[stackTop]);
        cos.writeMapEnd();
        stackTop--;
        break;
      case START_ARRAY:
        if (stackTop >= 0 && isArray[stackTop]) {
          cos.setItemCount(1);
          cos.startItem();
          counts[stackTop]++;
        }
        cos.writeArrayStart();
        isArray[++stackTop] = true;
        counts[stackTop] = 0;
        continue;
      case VALUE_STRING:
        if (stackTop >= 0 && isArray[stackTop]) {
          cos.setItemCount(1);
          cos.startItem();
          counts[stackTop]++;
        }
        byte[] bb = p.getText().getBytes(StandardCharsets.UTF_8);
        cos.writeBytes(bb);
        break;
      case START_OBJECT:
        if (stackTop >= 0 && isArray[stackTop]) {
          cos.setItemCount(1);
          cos.startItem();
          counts[stackTop]++;
        }
        cos.writeMapStart();
        isArray[++stackTop] = false;
        counts[stackTop] = 0;
        continue;
      case FIELD_NAME:
        cos.setItemCount(1);
        cos.startItem();
        counts[stackTop]++;
        cos.writeBytes(p.getCurrentName().getBytes(StandardCharsets.UTF_8));
        break;
      default:
        throw new RuntimeException("Unsupported: " + p.getCurrentToken());
      }
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] { { 64, 0, "" }, { 64, 0, jss(0, 'a') }, { 64, 0, jss(3, 'a') },
        { 64, 0, jss(64, 'a') }, { 64, 0, jss(65, 'a') }, { 64, 0, jss(100, 'a') }, { 64, 1, "[]" },
        { 64, 1, "[" + jss(0, 'a') + "]" }, { 64, 1, "[" + jss(3, 'a') + "]" }, { 64, 1, "[" + jss(61, 'a') + "]" },
        { 64, 1, "[" + jss(62, 'a') + "]" }, { 64, 1, "[" + jss(64, 'a') + "]" }, { 64, 1, "[" + jss(65, 'a') + "]" },
        { 64, 1, "[" + jss(0, 'a') + "," + jss(0, '0') + "]" }, { 64, 1, "[" + jss(0, 'a') + "," + jss(10, '0') + "]" },
        { 64, 1, "[" + jss(0, 'a') + "," + jss(63, '0') + "]" },
        { 64, 1, "[" + jss(0, 'a') + "," + jss(64, '0') + "]" },
        { 64, 1, "[" + jss(0, 'a') + "," + jss(65, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(0, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(10, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(51, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(52, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(54, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(55, '0') + "]" },

        { 64, 1, "[" + jss(0, 'a') + "," + jss(0, 'a') + "," + jss(0, '0') + "]" },
        { 64, 1, "[" + jss(0, 'a') + "," + jss(0, 'a') + "," + jss(63, '0') + "]" },
        { 64, 1, "[" + jss(0, 'a') + "," + jss(0, 'a') + "," + jss(64, '0') + "]" },
        { 64, 1, "[" + jss(0, 'a') + "," + jss(0, 'a') + "," + jss(65, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(20, 'A') + "," + jss(10, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(20, 'A') + "," + jss(23, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(20, 'A') + "," + jss(24, '0') + "]" },
        { 64, 1, "[" + jss(10, 'a') + "," + jss(20, 'A') + "," + jss(25, '0') + "]" }, { 64, 2, "[[]]" },
        { 64, 2, "[[" + jss(0, 'a') + "], []]" }, { 64, 2, "[[" + jss(10, 'a') + "], []]" },
        { 64, 2, "[[" + jss(59, 'a') + "], []]" }, { 64, 2, "[[" + jss(60, 'a') + "], []]" },
        { 64, 2, "[[" + jss(100, 'a') + "], []]" }, { 64, 2, "[[" + jss(10, '0') + ", " + jss(53, 'a') + "], []]" },
        { 64, 2, "[[" + jss(10, '0') + ", " + jss(54, 'a') + "], []]" },
        { 64, 2, "[[" + jss(10, '0') + ", " + jss(55, 'a') + "], []]" },

        { 64, 2, "[[], [" + jss(0, 'a') + "]]" }, { 64, 2, "[[], [" + jss(10, 'a') + "]]" },
        { 64, 2, "[[], [" + jss(63, 'a') + "]]" }, { 64, 2, "[[], [" + jss(64, 'a') + "]]" },
        { 64, 2, "[[], [" + jss(65, 'a') + "]]" }, { 64, 2, "[[], [" + jss(10, '0') + ", " + jss(53, 'a') + "]]" },
        { 64, 2, "[[], [" + jss(10, '0') + ", " + jss(54, 'a') + "]]" },
        { 64, 2, "[[], [" + jss(10, '0') + ", " + jss(55, 'a') + "]]" },

        { 64, 2, "[[" + jss(10, '0') + "]]" }, { 64, 2, "[[" + jss(62, '0') + "]]" },
        { 64, 2, "[[" + jss(63, '0') + "]]" }, { 64, 2, "[[" + jss(64, '0') + "]]" },
        { 64, 2, "[[" + jss(10, 'a') + ", " + jss(10, '0') + "]]" },
        { 64, 2, "[[" + jss(10, 'a') + ", " + jss(52, '0') + "]]" },
        { 64, 2, "[[" + jss(10, 'a') + ", " + jss(53, '0') + "]]" },
        { 64, 2, "[[" + jss(10, 'a') + ", " + jss(54, '0') + "]]" }, { 64, 3, "[[[" + jss(10, '0') + "]]]" },
        { 64, 3, "[[[" + jss(62, '0') + "]]]" }, { 64, 3, "[[[" + jss(63, '0') + "]]]" },
        { 64, 3, "[[[" + jss(64, '0') + "]]]" }, { 64, 3, "[[[" + jss(10, 'a') + ", " + jss(10, '0') + "]]]" },
        { 64, 3, "[[[" + jss(10, 'a') + ", " + jss(52, '0') + "]]]" },
        { 64, 3, "[[[" + jss(10, 'a') + ", " + jss(53, '0') + "]]]" },
        { 64, 3, "[[[" + jss(10, 'a') + "], [" + jss(54, '0') + "]]]" },
        { 64, 3, "[[[" + jss(10, 'a') + "], [" + jss(10, '0') + "]]]" },
        { 64, 3, "[[[" + jss(10, 'a') + "], [" + jss(52, '0') + "]]]" },
        { 64, 3, "[[[" + jss(10, 'a') + "], [" + jss(53, '0') + "]]]" },
        { 64, 3, "[[[" + jss(10, 'a') + "], [" + jss(54, '0') + "]]]" },

        { 64, 2, "[[\"p\"], [\"mn\"]]" }, { 64, 2, "[[\"pqr\"], [\"mn\"]]" },
        { 64, 2, "[[\"pqrstuvwxyz\"], [\"mn\"]]" }, { 64, 2, "[[\"abc\", \"pqrstuvwxyz\"], [\"mn\"]]" },
        { 64, 2, "[[\"mn\"], [\"\"]]" }, { 64, 2, "[[\"mn\"], \"abc\"]" }, { 64, 2, "[[\"mn\"], \"abcdefghijk\"]" },
        { 64, 2, "[[\"mn\"], \"pqr\", \"abc\"]" }, { 64, 2, "[[\"mn\"]]" }, { 64, 2, "[[\"p\"], [\"mnopqrstuvwx\"]]" },
        { 64, 2, "[[\"pqr\"], [\"mnopqrstuvwx\"]]" }, { 64, 2, "[[\"pqrstuvwxyz\"], [\"mnopqrstuvwx\"]]" },
        { 64, 2, "[[\"abc\"], \"pqrstuvwxyz\", [\"mnopqrstuvwx\"]]" }, { 64, 2, "[[\"mnopqrstuvwx\"], [\"\"]]" },
        { 64, 2, "[[\"mnopqrstuvwx\"], [\"abc\"]]" }, { 64, 2, "[[\"mnopqrstuvwx\"], [\"abcdefghijk\"]]" },
        { 64, 2, "[[\"mnopqrstuvwx\"], [\"pqr\", \"abc\"]]" }, { 100, 2, "[[\"pqr\", \"mnopqrstuvwx\"]]" },
        { 100, 2, "[[\"pqr\", \"ab\", \"mnopqrstuvwx\"]]" }, { 64, 2, "[[[\"pqr\"]], [[\"ab\"], [\"mnopqrstuvwx\"]]]" },

        { 64, 1, "{}" }, { 64, 1, "{\"n\": \"v\"}" }, { 64, 1, "{\"n1\": \"v\", \"n2\": []}" },
        { 100, 1, "{\"n1\": \"v\", \"n2\": []}" }, { 100, 1, "{\"n1\": \"v\", \"n2\": [\"abc\"]}" }, });
  }

  /**
   * Returns a new JSON String {@code n} bytes long with consecutive characters
   * starting with {@code c}.
   */
  private static String jss(final int n, char c) {
    char[] cc = new char[n + 2];
    cc[0] = cc[n + 1] = '"';
    for (int i = 1; i < n + 1; i++) {
      if (c == 'Z') {
        c = 'a';
      } else if (c == 'z') {
        c = '0';
      } else if (c == '9') {
        c = 'A';
      } else {
        c++;
      }
      cc[i] = c;
    }
    return new String(cc);
  }
}
