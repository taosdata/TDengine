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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestValidatingIO {
  enum Encoding {
    BINARY, BLOCKING_BINARY, JSON,
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestValidatingIO.class);

  private Encoding eEnc;
  private int iSkipL;
  private String sJsSch;
  private String sCl;

  public TestValidatingIO(Encoding enc, int skip, String js, String cls) {
    this.eEnc = enc;
    this.iSkipL = skip;
    this.sJsSch = js;
    this.sCl = cls;
  }

  private static final int COUNT = 1;

  @Test
  public void testMain() throws IOException {
    for (int i = 0; i < COUNT; i++) {
      testOnce(new Schema.Parser().parse(sJsSch), sCl, iSkipL, eEnc);
    }
  }

  private void testOnce(Schema schema, String calls, int skipLevel, Encoding encoding) throws IOException {
    Object[] values = randomValues(calls);
    print(eEnc, iSkipL, schema, schema, values, values);
    byte[] bytes = make(schema, calls, values, encoding);
    check(schema, bytes, calls, values, skipLevel, encoding);
  }

  public static byte[] make(Schema sc, String calls, Object[] values, Encoding encoding) throws IOException {
    EncoderFactory factory = EncoderFactory.get();
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    Encoder bvo = null;
    switch (encoding) {
    case BINARY:
      bvo = factory.binaryEncoder(ba, null);
      break;
    case BLOCKING_BINARY:
      bvo = factory.blockingBinaryEncoder(ba, null);
      break;
    case JSON:
      bvo = factory.jsonEncoder(sc, ba);
      break;
    }

    Encoder vo = factory.validatingEncoder(sc, bvo);
    generate(vo, calls, values);
    vo.flush();
    return ba.toByteArray();
  }

  public static class InputScanner {
    private final char[] chars;
    private int cpos = 0;

    public InputScanner(char[] chars) {
      this.chars = chars;
    }

    public boolean next() {
      if (cpos < chars.length) {
        cpos++;
      }
      return cpos != chars.length;
    }

    public char cur() {
      return chars[cpos];
    }

    public boolean isDone() {
      return cpos == chars.length;
    }
  }

  public static void generate(Encoder vw, String calls, Object[] values) throws IOException {
    InputScanner cs = new InputScanner(calls.toCharArray());
    int p = 0;
    while (!cs.isDone()) {
      char c = cs.cur();
      cs.next();
      switch (c) {
      case 'N':
        vw.writeNull();
        break;
      case 'B':
        boolean b = (Boolean) values[p++];
        vw.writeBoolean(b);
        break;
      case 'I':
        int ii = (Integer) values[p++];
        vw.writeInt(ii);
        break;
      case 'L':
        long l = (Long) values[p++];
        vw.writeLong(l);
        break;
      case 'F':
        float f = (Float) values[p++];
        vw.writeFloat(f);
        break;
      case 'D':
        double d = (Double) values[p++];
        vw.writeDouble(d);
        break;
      case 'S': {
        extractInt(cs);
        String s = (String) values[p++];
        vw.writeString(new Utf8(s));
        break;
      }
      case 'K': {
        extractInt(cs);
        String s = (String) values[p++];
        vw.writeString(s);
        break;
      }
      case 'b': {
        extractInt(cs);
        byte[] bb = (byte[]) values[p++];
        vw.writeBytes(bb);
        break;
      }
      case 'f': {
        extractInt(cs);
        byte[] bb = (byte[]) values[p++];
        vw.writeFixed(bb);
        break;
      }
      case 'e': {
        int e = extractInt(cs);
        vw.writeEnum(e);
        break;
      }
      case '[':
        vw.writeArrayStart();
        break;
      case ']':
        vw.writeArrayEnd();
        break;
      case '{':
        vw.writeMapStart();
        break;
      case '}':
        vw.writeMapEnd();
        break;
      case 'c':
        vw.setItemCount(extractInt(cs));
        break;
      case 's':
        vw.startItem();
        break;
      case 'U': {
        vw.writeIndex(extractInt(cs));
        break;
      }
      default:
        fail();
        break;
      }
    }
  }

  public static Object[] randomValues(String calls) {
    Random r = new Random(0L);
    InputScanner cs = new InputScanner(calls.toCharArray());
    List<Object> result = new ArrayList<>();
    while (!cs.isDone()) {
      char c = cs.cur();
      cs.next();
      switch (c) {
      case 'N':
        break;
      case 'B':
        result.add(r.nextBoolean());
        break;
      case 'I':
        result.add(r.nextInt());
        break;
      case 'L':
        result.add((long) r.nextInt());
        break;
      case 'F':
        result.add((float) r.nextInt());
        break;
      case 'D':
        result.add((double) r.nextInt());
        break;
      case 'S':
      case 'K':
        result.add(nextString(r, extractInt(cs)));
        break;
      case 'b':
      case 'f':
        result.add(nextBytes(r, extractInt(cs)));
        break;
      case 'e':
      case 'c':
      case 'U':
        extractInt(cs);
      case '[':
      case ']':
      case '{':
      case '}':
      case 's':
        break;
      default:
        fail();
        break;
      }
    }
    return result.toArray();
  }

  private static int extractInt(InputScanner sc) {
    int r = 0;
    while (!sc.isDone()) {
      if (Character.isDigit(sc.cur())) {
        r = r * 10 + sc.cur() - '0';
        sc.next();
      } else {
        break;
      }
    }
    return r;
  }

  private static byte[] nextBytes(Random r, int length) {
    byte[] bb = new byte[length];
    r.nextBytes(bb);
    return bb;
  }

  private static String nextString(Random r, int length) {
    char[] cc = new char[length];
    for (int i = 0; i < length; i++) {
      cc[i] = (char) ('A' + r.nextInt(26));
    }
    return new String(cc);
  }

  private static void check(Schema sc, byte[] bytes, String calls, Object[] values, final int skipLevel,
      Encoding encoding) throws IOException {
    // dump(bytes);
    // System.out.println(new String(bytes, "UTF-8"));
    Decoder bvi = null;
    switch (encoding) {
    case BINARY:
    case BLOCKING_BINARY:
      bvi = DecoderFactory.get().binaryDecoder(bytes, null);
      break;
    case JSON:
      InputStream in = new ByteArrayInputStream(bytes);
      bvi = new JsonDecoder(sc, in);
    }
    Decoder vi = new ValidatingDecoder(sc, bvi);
    String msg = "Error in validating case: " + sc;
    check(msg, vi, calls, values, skipLevel);
  }

  public static void check(String msg, Decoder vi, String calls, Object[] values, final int skipLevel)
      throws IOException {
    InputScanner cs = new InputScanner(calls.toCharArray());
    int p = 0;
    int level = 0;
    long[] counts = new long[100];
    boolean[] isArray = new boolean[100];
    boolean[] isEmpty = new boolean[100];
    while (!cs.isDone()) {
      final char c = cs.cur();
      cs.next();
      try {
        switch (c) {
        case 'N':
          vi.readNull();
          break;
        case 'B':
          assertEquals(msg, values[p++], vi.readBoolean());
          break;
        case 'I':
          assertEquals(msg, values[p++], vi.readInt());
          break;
        case 'L':
          assertEquals(msg, values[p++], vi.readLong());
          break;
        case 'F':
          if (!(values[p] instanceof Float))
            fail();
          float f = (Float) values[p++];
          assertEquals(msg, f, vi.readFloat(), Math.abs(f / 1000));
          break;
        case 'D':
          if (!(values[p] instanceof Double))
            fail();
          double d = (Double) values[p++];
          assertEquals(msg, d, vi.readDouble(), Math.abs(d / 1000));
          break;
        case 'S':
          extractInt(cs);
          if (level == skipLevel) {
            vi.skipString();
            p++;
          } else {
            String s = (String) values[p++];
            assertEquals(msg, new Utf8(s), vi.readString(null));
          }
          break;
        case 'K':
          extractInt(cs);
          if (level == skipLevel) {
            vi.skipString();
            p++;
          } else {
            String s = (String) values[p++];
            assertEquals(msg, new Utf8(s), vi.readString(null));
          }
          break;
        case 'b':
          extractInt(cs);
          if (level == skipLevel) {
            vi.skipBytes();
            p++;
          } else {
            byte[] bb = (byte[]) values[p++];
            ByteBuffer bb2 = vi.readBytes(null);
            byte[] actBytes = new byte[bb2.remaining()];
            System.arraycopy(bb2.array(), bb2.position(), actBytes, 0, bb2.remaining());
            assertArrayEquals(msg, bb, actBytes);
          }
          break;
        case 'f': {
          int len = extractInt(cs);
          if (level == skipLevel) {
            vi.skipFixed(len);
            p++;
          } else {
            byte[] bb = (byte[]) values[p++];
            byte[] actBytes = new byte[len];
            vi.readFixed(actBytes);
            assertArrayEquals(msg, bb, actBytes);
          }
        }
          break;
        case 'e': {
          int e = extractInt(cs);
          if (level == skipLevel) {
            vi.readEnum();
          } else {
            assertEquals(msg, e, vi.readEnum());
          }
        }
          break;
        case '[':
          if (level == skipLevel) {
            p += skip(msg, cs, vi, true);
            break;
          } else {
            level++;
            counts[level] = vi.readArrayStart();
            isArray[level] = true;
            isEmpty[level] = counts[level] == 0;
            continue;
          }
        case '{':
          if (level == skipLevel) {
            p += skip(msg, cs, vi, false);
            break;
          } else {
            level++;
            counts[level] = vi.readMapStart();
            isArray[level] = false;
            isEmpty[level] = counts[level] == 0;
            continue;
          }
        case ']':
          assertEquals(msg, 0, counts[level]);
          if (!isEmpty[level]) {
            assertEquals(msg, 0, vi.arrayNext());
          }
          level--;
          break;
        case '}':
          assertEquals(0, counts[level]);
          if (!isEmpty[level]) {
            assertEquals(msg, 0, vi.mapNext());
          }
          level--;
          break;
        case 's':
          if (counts[level] == 0) {
            if (isArray[level]) {
              counts[level] = vi.arrayNext();
            } else {
              counts[level] = vi.mapNext();
            }
          }
          counts[level]--;
          continue;
        case 'c':
          extractInt(cs);
          continue;
        case 'U': {
          int idx = extractInt(cs);
          assertEquals(msg, idx, vi.readIndex());
          continue;
        }
        case 'R':
          ((ResolvingDecoder) vi).readFieldOrder();
          continue;
        default:
          fail(msg);
        }
      } catch (RuntimeException e) {
        throw new RuntimeException(msg, e);
      }
    }
    assertEquals(msg, values.length, p);
  }

  private static int skip(String msg, InputScanner cs, Decoder vi, boolean isArray) throws IOException {
    final char end = isArray ? ']' : '}';
    if (isArray) {
      assertEquals(msg, 0, vi.skipArray());
    } else if (end == '}') {
      assertEquals(msg, 0, vi.skipMap());
    }
    int level = 0;
    int p = 0;
    while (!cs.isDone()) {
      char c = cs.cur();
      cs.next();
      switch (c) {
      case '[':
      case '{':
        ++level;
        break;
      case ']':
      case '}':
        if (c == end && level == 0) {
          return p;
        }
        level--;
        break;
      case 'B':
      case 'I':
      case 'L':
      case 'F':
      case 'D':
      case 'S':
      case 'K':
      case 'b':
      case 'f':
      case 'e':
        p++;
        break;
      }
    }
    throw new RuntimeException("Don't know how to skip");
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(convertTo2dArray(encodings, skipLevels, testSchemas()));
  }

  private static Object[][] encodings = new Object[][] { { Encoding.BINARY }, { Encoding.BLOCKING_BINARY },
      { Encoding.JSON } };

  private static Object[][] skipLevels = new Object[][] { { -1 }, { 0 }, { 1 }, { 2 }, };

  public static Object[][] convertTo2dArray(final Object[][]... values) {
    ArrayList<Object[]> ret = new ArrayList<>();

    Iterator<Object[]> iter = cartesian(values);
    while (iter.hasNext()) {
      Object[] objects = iter.next();
      ret.add(objects);
    }
    Object[][] retArrays = new Object[ret.size()][];
    for (int i = 0; i < ret.size(); i++) {
      retArrays[i] = ret.get(i);
    }
    return retArrays;
  }

  /**
   * Returns the Cartesian product of input sequences.
   */
  public static Iterator<Object[]> cartesian(final Object[][]... values) {
    return new Iterator<Object[]>() {
      private int[] pos = new int[values.length];

      @Override
      public boolean hasNext() {
        return pos[0] < values[0].length;
      }

      @Override
      public Object[] next() {
        Object[][] v = new Object[values.length][];
        for (int i = 0; i < v.length; i++) {
          v[i] = values[i][pos[i]];
        }
        for (int i = v.length - 1; i >= 0; i--) {
          if (++pos[i] == values[i].length) {
            if (i != 0) {
              pos[i] = 0;
            }
          } else {
            break;
          }
        }
        return concat(v);
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Concatenates the input sequences in order and forms a longer sequence.
   */
  public static Object[] concat(Object[]... oo) {
    int l = 0;
    for (Object[] o : oo) {
      l += o.length;
    }
    Object[] result = new Object[l];
    l = 0;
    for (Object[] o : oo) {
      System.arraycopy(o, 0, result, l, o.length);
      l += o.length;
    }
    return result;
  }

  /**
   * Pastes incoming tables to form a wider table. All incoming tables should be
   * of same height.
   */
  static Object[][] paste(Object[][]... in) {
    Object[][] result = new Object[in[0].length][];
    Object[][] cc = new Object[in.length][];
    for (int i = 0; i < result.length; i++) {
      for (int j = 0; j < cc.length; j++) {
        cc[j] = in[j][i];
      }
      result[i] = concat(cc);
    }
    return result;
  }

  public static Object[][] testSchemas() {
    /**
     * The first argument is a schema. The second one is a sequence of (single
     * character) mnemonics: N null B boolean I int L long F float D double K
     * followed by integer - key-name (and its length) in a map S followed by
     * integer - string and its length b followed by integer - bytes and length f
     * followed by integer - fixed and length c Number of items to follow in an
     * array/map. U followed by integer - Union and its branch e followed by integer
     * - Enum and its value [ Start array ] End array { Start map } End map s start
     * item
     */
    return new Object[][] { { "\"null\"", "N" }, { "\"boolean\"", "B" }, { "\"int\"", "I" }, { "\"long\"", "L" },
        { "\"float\"", "F" }, { "\"double\"", "D" }, { "\"string\"", "S0" }, { "\"string\"", "S10" },
        { "\"bytes\"", "b0" }, { "\"bytes\"", "b10" }, { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 1}", "f1" },
        { "{\"type\":\"fixed\", \"name\":\"fi\", \"size\": 10}", "f10" },
        { "{\"type\":\"enum\", \"name\":\"en\", \"symbols\":[\"v1\", \"v2\"]}", "e1" },

        { "{\"type\":\"array\", \"items\": \"boolean\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[]", }, { "{\"type\":\"array\", \"items\": \"long\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"double\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"string\"}", "[]", },
        { "{\"type\":\"array\", \"items\": \"bytes\"}", "[]", },
        { "{\"type\":\"array\", \"items\":{\"type\":\"fixed\", " + "\"name\":\"fi\", \"size\": 10}}", "[]" },

        { "{\"type\":\"array\", \"items\": \"boolean\"}", "[c1sB]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sI]" },
        { "{\"type\":\"array\", \"items\": \"long\"}", "[c1sL]" },
        { "{\"type\":\"array\", \"items\": \"float\"}", "[c1sF]" },
        { "{\"type\":\"array\", \"items\": \"double\"}", "[c1sD]" },
        { "{\"type\":\"array\", \"items\": \"string\"}", "[c1sS10]" },
        { "{\"type\":\"array\", \"items\": \"bytes\"}", "[c1sb10]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c1sIc1sI]" },
        { "{\"type\":\"array\", \"items\": \"int\"}", "[c2sIsI]" },
        { "{\"type\":\"array\", \"items\":{\"type\":\"fixed\", " + "\"name\":\"fi\", \"size\": 10}}", "[c2sf10sf10]" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}", "{}" }, { "{\"type\":\"map\", \"values\": \"int\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{}" }, { "{\"type\":\"map\", \"values\": \"float\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"double\"}", "{}" }, { "{\"type\":\"map\", \"values\": \"string\"}", "{}" },
        { "{\"type\":\"map\", \"values\": \"bytes\"}", "{}" },
        { "{\"type\":\"map\", \"values\": " + "{\"type\":\"array\", \"items\":\"int\"}}", "{}" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}", "{c1sK5B}" },
        { "{\"type\":\"map\", \"values\": \"int\"}", "{c1sK5I}" },
        { "{\"type\":\"map\", \"values\": \"long\"}", "{c1sK5L}" },
        { "{\"type\":\"map\", \"values\": \"float\"}", "{c1sK5F}" },
        { "{\"type\":\"map\", \"values\": \"double\"}", "{c1sK5D}" },
        { "{\"type\":\"map\", \"values\": \"string\"}", "{c1sK5S10}" },
        { "{\"type\":\"map\", \"values\": \"bytes\"}", "{c1sK5b10}" },
        { "{\"type\":\"map\", \"values\": " + "{\"type\":\"array\", \"items\":\"int\"}}", "{c1sK5[c3sIsIsI]}" },

        { "{\"type\":\"map\", \"values\": \"boolean\"}", "{c1sK5Bc2sK5BsK5B}" },

        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"boolean\"}]}", "B" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"int\"}]}", "I" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"long\"}]}", "L" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"float\"}]}", "F" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"double\"}]}", "D" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"string\"}]}", "S10" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f\", \"type\":\"bytes\"}]}", "b10" },

        // multi-field records
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f1\", \"type\":\"int\"},"
            + "{\"name\":\"f2\", \"type\":\"double\"}," + "{\"name\":\"f3\", \"type\":\"string\"}]}", "IDS10" },
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f0\", \"type\":\"null\"},"
            + "{\"name\":\"f1\", \"type\":\"boolean\"}," + "{\"name\":\"f2\", \"type\":\"int\"},"
            + "{\"name\":\"f3\", \"type\":\"long\"}," + "{\"name\":\"f4\", \"type\":\"float\"},"
            + "{\"name\":\"f5\", \"type\":\"double\"}," + "{\"name\":\"f6\", \"type\":\"string\"},"
            + "{\"name\":\"f7\", \"type\":\"bytes\"}]}", "NBILFDS10b25" },

        // record of records
        { "{\"type\":\"record\",\"name\":\"outer\",\"fields\":[" + "{\"name\":\"f1\", \"type\":{\"type\":\"record\", "
            + "\"name\":\"inner\", \"fields\":[" + "{\"name\":\"g1\", \"type\":\"int\"}, {\"name\":\"g2\", "
            + "\"type\":\"double\"}]}}," + "{\"name\":\"f2\", \"type\":\"string\"},"
            + "{\"name\":\"f3\", \"type\":\"inner\"}]}", "IDS10ID" },
        // record with array
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f1\", \"type\":\"long\"},"
            + "{\"name\":\"f2\", " + "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}", "L[c1sI]" },

        // record with map
        { "{\"type\":\"record\",\"name\":\"r\",\"fields\":[" + "{\"name\":\"f1\", \"type\":\"long\"},"
            + "{\"name\":\"f2\", " + "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}", "L{c1sK5I}" },

        // array of records
        { "{\"type\":\"array\", \"items\":" + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
            + "{\"name\":\"f1\", \"type\":\"long\"}," + "{\"name\":\"f2\", \"type\":\"null\"}]}}", "[c2sLNsLN]" },

        { "{\"type\":\"array\", \"items\":" + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
            + "{\"name\":\"f1\", \"type\":\"long\"}," + "{\"name\":\"f2\", "
            + "\"type\":{\"type\":\"array\", \"items\":\"int\"}}]}}", "[c2sL[c1sI]sL[c2sIsI]]" },
        { "{\"type\":\"array\", \"items\":" + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
            + "{\"name\":\"f1\", \"type\":\"long\"}," + "{\"name\":\"f2\", "
            + "\"type\":{\"type\":\"map\", \"values\":\"int\"}}]}}", "[c2sL{c1sK5I}sL{c2sK5IsK5I}]" },
        { "{\"type\":\"array\", \"items\":" + "{\"type\":\"record\",\"name\":\"r\",\"fields\":["
            + "{\"name\":\"f1\", \"type\":\"long\"}," + "{\"name\":\"f2\", " + "\"type\":[\"null\", \"int\"]}]}}",
            "[c2sLU0NsLU1I]" },

        { "[\"boolean\"]", "U0B" }, { "[\"int\"]", "U0I" }, { "[\"long\"]", "U0L" }, { "[\"float\"]", "U0F" },
        { "[\"double\"]", "U0D" }, { "[\"string\"]", "U0S10" }, { "[\"bytes\"]", "U0b10" },

        { "[\"null\", \"int\"]", "U0N" }, { "[\"boolean\", \"int\"]", "U0B" }, { "[\"boolean\", \"int\"]", "U1I" },
        { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]", "U0B" },

        { "[\"boolean\", {\"type\":\"array\", \"items\":\"int\"} ]", "U1[c1sI]" },

        // Recursion
        { "{\"type\": \"record\", \"name\": \"Node\", \"fields\": [" + "{\"name\":\"label\", \"type\":\"string\"},"
            + "{\"name\":\"children\", \"type\":" + "{\"type\": \"array\", \"items\": \"Node\" }}]}", "S10[c1sS10[]]" },

        { "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
            + "{\"name\":\"value\", \"type\":[\"null\", \"string\","
            + "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": [" + "{\"name\":\"car\", \"type\":\"Lisp\"},"
            + "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}", "U0N" },
        { "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
            + "{\"name\":\"value\", \"type\":[\"null\", \"string\","
            + "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": [" + "{\"name\":\"car\", \"type\":\"Lisp\"},"
            + "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}", "U1S10" },
        { "{\"type\": \"record\", \"name\": \"Lisp\", \"fields\": ["
            + "{\"name\":\"value\", \"type\":[\"null\", \"string\","
            + "{\"type\": \"record\", \"name\": \"Cons\", \"fields\": [" + "{\"name\":\"car\", \"type\":\"Lisp\"},"
            + "{\"name\":\"cdr\", \"type\":\"Lisp\"}]}]}]}", "U2U1S10U0N" },

        // Deep recursion
        { "{\"type\": \"record\", \"name\": \"Node\", \"fields\": [" + "{\"name\":\"children\", \"type\":"
            + "{\"type\": \"array\", \"items\": \"Node\" }}]}",
            "[c1s[c1s[c1s[c1s[c1s[c1s[c1s[c1s[c1s[c1s[c1s[]]]]]]]]]]]]" },

    };
  }

  static void dump(byte[] bb) {
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

  static void print(Encoding encoding, int skipLevel, Schema writerSchema, Schema readerSchema, Object[] writtenValues,
      Object[] expectedValues) {
    LOG.debug("{} Skip Level {}", encoding, skipLevel);
    printSchemaAndValues("Writer", writerSchema, writtenValues);
    printSchemaAndValues("Reader", readerSchema, expectedValues);
  }

  private static void printSchemaAndValues(String schemaType, Schema schema, Object[] values) {
    LOG.debug("{} Schema {}", schemaType, schema);
    for (Object value : values) {
      LOG.debug("{} -> {}", value, value.getClass().getSimpleName());
    }
  }
}
