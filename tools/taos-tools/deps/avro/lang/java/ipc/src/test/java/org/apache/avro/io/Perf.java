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
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.FooBarSpecificRecord;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.TypeEnum;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

/**
 * Performance tests for various low level operations of Avro encoding and
 * decoding.
 *
 * @deprecated Use avro-perf module
 */
@Deprecated
public class Perf {
  private static final int COUNT // needs to be a multiple of 4
      = Integer.parseInt(System.getProperty("org.apache.avro.io.perf.count", "250000"));
  private static final int CYCLES = Integer.parseInt(System.getProperty("org.apache.avro.io.perf.cycles", "800"));

  /**
   * Use a fixed value seed for random number generation to allow for better
   * cross-run comparisons.
   */
  private static final long SEED = 19781210;

  protected static Random newRandom() {
    return new Random(SEED);
  }

  private static class TestDescriptor {
    Class<? extends Test> test;
    String param;

    TestDescriptor(Class<? extends Test> test, String param) {
      this.test = test;
      this.param = param;
    }

    void add(List<TestDescriptor> typeList) {
      ALL_TESTS.put(param, this);
      typeList.add(this);
    }
  }

  private static final List<TestDescriptor> BASIC = new ArrayList<>();
  private static final List<TestDescriptor> RECORD = new ArrayList<>();
  private static final List<TestDescriptor> GENERIC = new ArrayList<>();
  private static final List<TestDescriptor> GENERIC_ONETIME = new ArrayList<>();
  private static final List<TestDescriptor> SPECIFIC = new ArrayList<>();
  private static final List<TestDescriptor> REFLECT = new ArrayList<>();
  private static final LinkedHashMap<String, TestDescriptor> ALL_TESTS;
  private static final LinkedHashMap<String, List<TestDescriptor>> BATCHES;
  static {
    ALL_TESTS = new LinkedHashMap<>();
    BATCHES = new LinkedHashMap<>();
    BATCHES.put("-basic", BASIC);
    new TestDescriptor(IntTest.class, "-i").add(BASIC);
    new TestDescriptor(SmallLongTest.class, "-ls").add(BASIC);
    new TestDescriptor(LongTest.class, "-l").add(BASIC);
    new TestDescriptor(FloatTest.class, "-f").add(BASIC);
    new TestDescriptor(DoubleTest.class, "-d").add(BASIC);
    new TestDescriptor(BoolTest.class, "-b").add(BASIC);
    new TestDescriptor(BytesTest.class, "-by").add(BASIC);
    new TestDescriptor(StringTest.class, "-s").add(BASIC);
    new TestDescriptor(ArrayTest.class, "-a").add(BASIC);
    new TestDescriptor(MapTest.class, "-m").add(BASIC);
    new TestDescriptor(ExtendedEnumResolveTest.class, "-ee").add(BASIC);
    new TestDescriptor(UnchangedUnionResolveTest.class, "-uu").add(BASIC);
    BATCHES.put("-record", RECORD);
    new TestDescriptor(RecordTest.class, "-R").add(RECORD);
    new TestDescriptor(ValidatingRecord.class, "-Rv").add(RECORD);
    new TestDescriptor(ResolvingRecord.class, "-Rr").add(RECORD);
    new TestDescriptor(RecordWithDefault.class, "-Rd").add(RECORD);
    new TestDescriptor(RecordWithOutOfOrder.class, "-Ro").add(RECORD);
    new TestDescriptor(RecordWithPromotion.class, "-Rp").add(RECORD);
    BATCHES.put("-generic", GENERIC);
    new TestDescriptor(GenericTest.class, "-G").add(GENERIC);
    new TestDescriptor(GenericStrings.class, "-Gs").add(GENERIC);
    new TestDescriptor(GenericNested.class, "-Gn").add(GENERIC);
    new TestDescriptor(GenericNestedFake.class, "-Gf").add(GENERIC);
    new TestDescriptor(GenericWithDefault.class, "-Gd").add(GENERIC);
    new TestDescriptor(GenericWithOutOfOrder.class, "-Go").add(GENERIC);
    new TestDescriptor(GenericWithPromotion.class, "-Gp").add(GENERIC);
    BATCHES.put("-generic-onetime", GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeDecoderUse.class, "-Gotd").add(GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeReaderUse.class, "-Gotr").add(GENERIC_ONETIME);
    new TestDescriptor(GenericOneTimeUse.class, "-Got").add(GENERIC_ONETIME);
    BATCHES.put("-specific", SPECIFIC);
    new TestDescriptor(FooBarSpecificRecordTest.class, "-Sf").add(SPECIFIC);
    BATCHES.put("-reflect", REFLECT);
    new TestDescriptor(ReflectRecordTest.class, "-REFr").add(REFLECT);
    new TestDescriptor(ReflectBigRecordTest.class, "-REFbr").add(REFLECT);
    new TestDescriptor(ReflectFloatTest.class, "-REFf").add(REFLECT);
    new TestDescriptor(ReflectDoubleTest.class, "-REFd").add(REFLECT);
    new TestDescriptor(ReflectIntArrayTest.class, "-REFia").add(REFLECT);
    new TestDescriptor(ReflectLongArrayTest.class, "-REFla").add(REFLECT);
    new TestDescriptor(ReflectDoubleArrayTest.class, "-REFda").add(REFLECT);
    new TestDescriptor(ReflectFloatArrayTest.class, "-REFfa").add(REFLECT);
    new TestDescriptor(ReflectNestedFloatArrayTest.class, "-REFnf").add(REFLECT);
    new TestDescriptor(ReflectNestedObjectArrayTest.class, "-REFno").add(REFLECT);
    new TestDescriptor(ReflectNestedLargeFloatArrayTest.class, "-REFnlf").add(REFLECT);
    new TestDescriptor(ReflectNestedLargeFloatArrayBlockedTest.class, "-REFnlfb").add(REFLECT);
  }
  private static final int NAME_FIELD = 0;
  private static final int TIME_FIELD = 1;
  private static final int BYTES_PS_FIELD = 2;
  private static final int ENTRIES_PS_FIELD = 3;
  private static final int BYTES_PC_FIELD = 4;
  private static final int MIN_TIME_FIELD = 5;
  private static final int MAX_FIELD_TAG = 5;

  private static void usage() {
    StringBuilder usage = new StringBuilder("Usage: Perf [-o <file>] [-c <spec>] { -nowrite | -noread }");
    StringBuilder details = new StringBuilder();
    details.append(" -o file   (send output to a file)\n");
    details.append(
        " -c [n][t][e][b][c][m] (format as no-header CSV; include Name, Time, Entries/sec, Bytes/sec, bytes/Cycle, and/or min time/op; no spec=all fields)\n");
    details.append(" -nowrite   (do not execute write tests)\n");
    details.append(" -noread   (do not execute write tests)\n");
    for (Map.Entry<String, List<TestDescriptor>> entry : BATCHES.entrySet()) {
      List<TestDescriptor> lt = entry.getValue();
      String param = entry.getKey();
      String paramName = param.substring(1);
      usage.append(param).append(" | ");
      details.append(" ").append(param).append("   (executes all ").append(paramName).append(" tests):\n");
      for (TestDescriptor t : lt) {
        usage.append(t.param).append(" | ");
        details.append("      ").append(t.param).append("  (").append(t.test.getSimpleName()).append(")\n");
      }
    }
    usage.setLength(usage.length() - 2);
    usage.append("}\n");
    System.out.println(usage.toString());
    System.out.print(details.toString());
  }

  public static void main(String[] args) throws Exception {
    if (0 != (COUNT % 4)) {
      System.out.println("Property 'org.apache.avro.io.perf.count' must be a multiple of 4.");
      System.exit(1);
    }

    List<Test> tests = new ArrayList<>();
    boolean writeTests = true;
    boolean readTests = true;
    String outputfilename = null;
    PrintStream out = System.out;
    boolean[] csvFormat = null;
    String csvFormatString = null;

    for (int i = 0; i < args.length; i++) {
      String a = args[i];
      TestDescriptor t = ALL_TESTS.get(a);
      if (null != t) {
        tests.add(t.test.newInstance());
        continue;
      }
      List<TestDescriptor> lt = BATCHES.get(a);
      if (null != lt) {
        for (TestDescriptor td : lt) {
          tests.add(td.test.newInstance());
        }
        continue;
      }
      if (i < args.length - 1 && "-o".equals(a)) {
        outputfilename = args[++i];
        out = new PrintStream(new FileOutputStream(outputfilename));
        continue;
      }
      if ("-c".equals(a)) {
        if (i == args.length - 1 || args[i + 1].startsWith("-")) {
          csvFormatString = "ntebcm"; // For diagnostics
          csvFormat = new boolean[] { true, true, true, true, true, true };
        } else {
          csvFormatString = args[++i];
          csvFormat = new boolean[MAX_FIELD_TAG + 1];
          for (char c : csvFormatString.toCharArray())
            switch (c) {
            case 'n':
              csvFormat[NAME_FIELD] = true;
              break;
            case 't':
              csvFormat[TIME_FIELD] = true;
              break;
            case 'e':
              csvFormat[BYTES_PS_FIELD] = true;
              break;
            case 'b':
              csvFormat[ENTRIES_PS_FIELD] = true;
              break;
            case 'c':
              csvFormat[BYTES_PC_FIELD] = true;
              break;
            case 'm':
              csvFormat[MIN_TIME_FIELD] = true;
              break;
            default:
              usage();
              System.exit(1);
            }
        }
        continue;
      }
      if ("-nowrite".equals(a)) {
        writeTests = false;
        continue;
      }
      if ("-noread".equals(a)) {
        readTests = false;
        continue;
      }
      usage();
      System.exit(1);
    }
    if (tests.isEmpty()) {
      for (Map.Entry<String, TestDescriptor> entry : ALL_TESTS.entrySet()) {
        TestDescriptor t = entry.getValue();
        Test test = t.test.newInstance();
        tests.add(test);
      }
    }
    System.out.println("Executing tests: \n" + tests + "\n readTests:" + readTests + "\n writeTests:" + writeTests
        + "\n cycles=" + CYCLES + "\n count=" + (COUNT / 1000) + "K");
    if (out != System.out)
      System.out.println(" Writing to: " + outputfilename);
    if (csvFormat != null)
      System.out.println(" CSV format: " + csvFormatString);

    TestResult tr = new TestResult();

    for (Test t : tests) {
      try {
        // get everything to compile once
        t.init();
        if (t.isReadTest()) {
          t.readTest();
        }
        if (t.isWriteTest()) {
          t.writeTest();
        }
        t.reset();
      } catch (Exception e) {
        System.out.println("Failed to execute test: " + t.getClass().getSimpleName());
        throw e;
      }
    }

    if (csvFormat == null)
      printHeader();

    for (Test t : tests) {
      // warmup JVM
      t.init();
      if (t.isReadTest() && readTests) {
        for (int i = 0; i < t.cycles / 2; i++) {
          t.readTest();
        }
      }
      if (t.isWriteTest() && writeTests) {
        for (int i = 0; i < t.cycles / 2; i++) {
          t.writeTest();
        }
      }

      // test
      System.gc();
      if (t.isReadTest() && readTests) {
        tr.reset();
        for (int i = 0; i < t.cycles; i++) {
          tr.update(t.readTest());
        }
        printResult(out, csvFormat, tr, t, t.name + "Read");
      }
      if (t.isWriteTest() && writeTests) {
        tr.reset();
        for (int i = 0; i < t.cycles; i++) {
          tr.update(t.writeTest());
        }
        printResult(out, csvFormat, tr, t, t.name + "Write");
      }
      t.reset();
    }
  }

  private static class TestResult {
    public long totalTime;
    public long minTime;

    public void reset() {
      totalTime = 0L;
      minTime = Long.MAX_VALUE;
    }

    public long update(long t) {
      totalTime += t;
      minTime = Math.min(t, minTime);
      return t;
    }
  }

  private static void printHeader() {
    String header = String.format("%60s     time    M entries/sec   M bytes/sec  bytes/cycle", "test name");
    System.out.println(header.toString());
  }

  private static void printResult(PrintStream o, boolean[] csv, TestResult tr, Test t, String name) {
    long s = tr.totalTime / 1000;
    double entries = (t.cycles * (double) t.count);
    double bytes = t.cycles * (double) t.encodedSize;
    StringBuilder result = new StringBuilder();
    if (csv != null) {
      boolean commaneeded = false;
      for (int i = 0; i <= MAX_FIELD_TAG; i++) {
        if (!csv[i])
          continue;
        if (commaneeded)
          result.append(",");
        else
          commaneeded = true;
        switch (i) {
        case NAME_FIELD:
          result.append(name);
          break;
        case TIME_FIELD:
          result.append(String.format("%d", (s / 1000)));
          break;
        case BYTES_PS_FIELD:
          result.append(String.format("%.3f", (entries / s)));
          break;
        case ENTRIES_PS_FIELD:
          result.append(String.format("%.3f", (bytes / s)));
          break;
        case BYTES_PC_FIELD:
          result.append(String.format("%d", t.encodedSize));
          break;
        case MIN_TIME_FIELD:
          result.append(String.format("%d", tr.minTime));
          break;
        }
      }
    } else {
      result.append(String.format("%42s: %6d ms  ", name, (s / 1000)));
      result.append(String.format("%10.3f   %11.3f   %11d", (entries / s), (bytes / s), t.encodedSize));
    }
    o.println(result.toString());
  }

  private abstract static class Test {

    /**
     * Name of the test.
     */
    public final String name;
    public final int count;
    public final int cycles;
    public long encodedSize = 0;
    protected boolean isReadTest = true;
    protected boolean isWriteTest = true;
    static DecoderFactory decoder_factory = new DecoderFactory();
    static EncoderFactory encoder_factory = new EncoderFactory();

    public Test(String name, int cycles, int count) {
      this.name = name;
      this.cycles = cycles;
      this.count = count;
    }

    /**
     * Reads data from a Decoder and returns the time taken in nanoseconds.
     */
    abstract long readTest() throws IOException;

    /**
     * Writes data to an Encoder and returns the time taken in nanoseconds.
     */
    abstract long writeTest() throws IOException;

    final boolean isWriteTest() {
      return isWriteTest;
    }

    final boolean isReadTest() {
      return isReadTest;
    }

    /** initializes data for read and write tests **/
    abstract void init() throws IOException;

    /**
     * clears generated data arrays and other large objects created during
     * initialization
     **/
    abstract void reset();

    @Override
    public String toString() {
      return this.getClass().getSimpleName();
    }

  }

  /**
   * the basic test writes a simple schema directly to an encoder or reads from an
   * array. It does not use GenericDatumReader or any higher level constructs,
   * just manual serialization.
   */
  private static abstract class BasicTest extends Test {
    /**
     * Switch to using a DirectBinaryEncoder rather than a BufferedBinaryEncoder for
     * writing tests. DirectBinaryEncoders are noticably slower than Buffered ones,
     * but they can be more consistent in their performance, which can make it
     * easier to detect small performance improvements.
     */
    public static boolean USE_DIRECT_ENCODER = Boolean
        .parseBoolean(System.getProperty("org.apache.avro.io.perf.use-direct", "false"));

    protected final Schema schema;
    protected byte[] data;

    BasicTest(String name, String json) throws IOException {
      this(name, json, 1);
    }

    BasicTest(String name, String json, int factor) throws IOException {
      super(name, CYCLES, COUNT / factor);
      this.schema = new Schema.Parser().parse(json);
    }

    @Override
    public final long readTest() throws IOException {
      Decoder d = getDecoder();
      long t = System.nanoTime();
      readInternal(d);
      return (System.nanoTime() - t);
    }

    @Override
    public final long writeTest() throws IOException {
      Encoder e = getEncoder();
      long t = System.nanoTime();
      writeInternal(e);
      e.flush();
      return (System.nanoTime() - t);
    }

    protected Decoder getDecoder() throws IOException {
      return newDecoder();
    }

    private Encoder getEncoder() throws IOException {
      return newEncoder(getOutputStream());
    }

    protected Decoder newDecoder() {
      return decoder_factory.binaryDecoder(data, null);
    }

    protected Encoder newEncoder(ByteArrayOutputStream out) throws IOException {
      Encoder e = (USE_DIRECT_ENCODER ? encoder_factory.directBinaryEncoder(out, null)
          : encoder_factory.binaryEncoder(out, null));
//    Encoder e = encoder_factory.blockingBinaryEncoder(out, null);
//    Encoder e = new LegacyBinaryEncoder(out);
      return e;
    }

    private ByteArrayOutputStream getOutputStream() {
      return new ByteArrayOutputStream((int) (encodedSize > 0 ? encodedSize : count));
    }

    @Override
    void init() throws IOException {
      genSourceData();
      ByteArrayOutputStream baos = getOutputStream();
      Encoder e = newEncoder(baos);
      writeInternal(e);
      e.flush();
      data = baos.toByteArray();
      encodedSize = data.length;
      // System.out.println(this.getClass().getSimpleName() + " encodedSize=" +
      // encodedSize);
    }

    abstract void genSourceData();

    abstract void readInternal(Decoder d) throws IOException;

    abstract void writeInternal(Encoder e) throws IOException;
  }

  static class IntTest extends BasicTest {
    protected int[] sourceData = null;

    public IntTest() throws IOException {
      this("Int", "{ \"type\": \"int\"} ");
    }

    private IntTest(String name, String schema) throws IOException {
      super(name, schema);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new int[count];
      for (int i = 0; i < sourceData.length; i += 4) {
        sourceData[i] = r.nextInt(50); // fits in 1 byte
        sourceData[i + 1] = r.nextInt(5000); // fits in 2 bytes
        sourceData[i + 2] = r.nextInt(500000); // fits in 3 bytes
        sourceData[i + 3] = r.nextInt(150000000); // most in 4, some in 5
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count / 4; i++) {
        d.readInt();
        d.readInt();
        d.readInt();
        d.readInt();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeInt(sourceData[i]);
        e.writeInt(sourceData[i + 1]);
        e.writeInt(sourceData[i + 2]);
        e.writeInt(sourceData[i + 3]);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  // This is the same data as ReadInt, but using readLong.
  static class SmallLongTest extends IntTest {
    public SmallLongTest() throws IOException {
      super("SmallLong", "{ \"type\": \"long\"} ");
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count / 4; i++) {
        d.readLong();
        d.readLong();
        d.readLong();
        d.readLong();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeLong(sourceData[i]);
        e.writeLong(sourceData[i + 1]);
        e.writeLong(sourceData[i + 2]);
        e.writeLong(sourceData[i + 3]);
      }
    }
  }

  // this tests reading Longs that are sometimes very large
  static class LongTest extends BasicTest {
    private long[] sourceData = null;

    public LongTest() throws IOException {
      super("Long", "{ \"type\": \"long\"} ");
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new long[count];
      for (int i = 0; i < sourceData.length; i += 4) {
        sourceData[i] = r.nextLong() % 0x7FL; // half fit in 1, half in 2
        sourceData[i + 1] = r.nextLong() % 0x1FFFFFL; // half fit in <=3, half in 4
        sourceData[i + 2] = r.nextLong() % 0x3FFFFFFFFL; // half in <=5, half in 6
        sourceData[i + 3] = r.nextLong() % 0x1FFFFFFFFFFFFL; // half in <=8, half in 9
      }
      // last 16, make full size
      for (int i = sourceData.length - 16; i < sourceData.length; i++) {
        sourceData[i] = r.nextLong();
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count / 4; i++) {
        d.readLong();
        d.readLong();
        d.readLong();
        d.readLong();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeLong(sourceData[i]);
        e.writeLong(sourceData[i + 1]);
        e.writeLong(sourceData[i + 2]);
        e.writeLong(sourceData[i + 3]);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class FloatTest extends BasicTest {
    float[] sourceData = null;

    public FloatTest() throws IOException {
      this("Float", "{ \"type\": \"float\"} ");
    }

    public FloatTest(String name, String schema) throws IOException {
      super(name, schema);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new float[count];
      for (int i = 0; i < sourceData.length;) {
        sourceData[i++] = r.nextFloat();
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i += 4) {
        d.readFloat();
        d.readFloat();
        d.readFloat();
        d.readFloat();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeFloat(sourceData[i]);
        e.writeFloat(sourceData[i + 1]);
        e.writeFloat(sourceData[i + 2]);
        e.writeFloat(sourceData[i + 3]);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class DoubleTest extends BasicTest {
    double[] sourceData = null;

    public DoubleTest() throws IOException {
      super("Double", "{ \"type\": \"double\"} ");
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new double[count];
      for (int i = 0; i < sourceData.length;) {
        sourceData[i++] = r.nextDouble();
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i += 4) {
        d.readDouble();
        d.readDouble();
        d.readDouble();
        d.readDouble();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeDouble(sourceData[i]);
        e.writeDouble(sourceData[i + 1]);
        e.writeDouble(sourceData[i + 2]);
        e.writeDouble(sourceData[i + 3]);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class BoolTest extends BasicTest {
    boolean[] sourceData = null;

    public BoolTest() throws IOException {
      super("Boolean", "{ \"type\": \"boolean\"} ");
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new boolean[count];
      for (int i = 0; i < sourceData.length;) {
        sourceData[i++] = r.nextBoolean();
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count / 4; i++) {
        d.readBoolean();
        d.readBoolean();
        d.readBoolean();
        d.readBoolean();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeBoolean(sourceData[i]);
        e.writeBoolean(sourceData[i + 1]);
        e.writeBoolean(sourceData[i + 2]);
        e.writeBoolean(sourceData[i + 3]);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class BytesTest extends BasicTest {
    byte[][] sourceData = null;

    public BytesTest() throws IOException {
      super("Bytes", "{ \"type\": \"bytes\"} ", 5);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new byte[count][];
      for (int i = 0; i < sourceData.length;) {
        byte[] data = new byte[r.nextInt(70)];
        r.nextBytes(data);
        sourceData[i++] = data;
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      ByteBuffer bb = ByteBuffer.allocate(70);
      for (int i = 0; i < count / 4; i++) {
        d.readBytes(bb);
        d.readBytes(bb);
        d.readBytes(bb);
        d.readBytes(bb);
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeBytes(sourceData[i]);
        e.writeBytes(sourceData[i + 1]);
        e.writeBytes(sourceData[i + 2]);
        e.writeBytes(sourceData[i + 3]);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  private static String randomString(Random r) {
    char[] data = new char[r.nextInt(70)];
    for (int j = 0; j < data.length; j++) {
      data[j] = (char) ('a' + r.nextInt('z' - 'a'));
    }
    return new String(data);
  }

  static class StringTest extends BasicTest {
    String[] sourceData = null;

    public StringTest() throws IOException {
      super("String", "{ \"type\": \"string\"} ", 5);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new String[count];
      for (int i = 0; i < sourceData.length;)
        sourceData[i++] = randomString(r);
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      Utf8 utf = new Utf8();
      for (int i = 0; i < count / 4; i++) {
        d.readString(utf).toString();
        d.readString(utf).toString();
        d.readString(utf).toString();
        d.readString(utf).toString();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (int i = 0; i < sourceData.length; i += 4) {
        e.writeString(sourceData[i]);
        e.writeString(sourceData[i + 1]);
        e.writeString(sourceData[i + 2]);
        e.writeString(sourceData[i + 3]);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class ArrayTest extends FloatTest {
    public ArrayTest() throws IOException {
      super("Array",
          "{ \"type\": \"array\", \"items\": " + " { \"type\": \"record\", \"name\":\"Foo\", \"fields\": "
              + "  [{\"name\":\"bar\", \"type\":" + "    {\"type\": \"array\", \"items\": "
              + "     { \"type\": \"record\", \"name\":\"Vals\", \"fields\": ["
              + "      {\"name\":\"f1\", \"type\":\"float\"}," + "      {\"name\":\"f2\", \"type\":\"float\"},"
              + "      {\"name\":\"f3\", \"type\":\"float\"}," + "      {\"name\":\"f4\", \"type\":\"float\"}]"
              + "     }" + "    }" + "   }]}}");
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      d.readArrayStart();
      for (long i = d.readArrayStart(); i != 0; i = d.arrayNext()) {
        for (long j = 0; j < i; j++) {
          d.readFloat();
          d.readFloat();
          d.readFloat();
          d.readFloat();
        }
      }
      d.arrayNext();
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      int items = sourceData.length / 4;
      e.writeArrayStart();
      e.setItemCount(1);
      e.startItem();
      e.writeArrayStart();
      e.setItemCount(items);
      for (int i = 0; i < sourceData.length; i += 4) {
        e.startItem();
        e.writeFloat(sourceData[i]);
        e.writeFloat(sourceData[i + 1]);
        e.writeFloat(sourceData[i + 2]);
        e.writeFloat(sourceData[i + 3]);
      }
      e.writeArrayEnd();
      e.writeArrayEnd();
    }
  }

  static class MapTest extends FloatTest {
    public MapTest() throws IOException {
      super("Map",
          "{ \"type\": \"map\", \"values\": " + "  { \"type\": \"record\", \"name\":\"Vals\", \"fields\": ["
              + "   {\"name\":\"f1\", \"type\":\"float\"}," + "   {\"name\":\"f2\", \"type\":\"float\"},"
              + "   {\"name\":\"f3\", \"type\":\"float\"}," + "   {\"name\":\"f4\", \"type\":\"float\"}]" + "  }} ");
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      Utf8 key = new Utf8();
      for (long i = d.readMapStart(); i != 0; i = d.mapNext()) {
        for (long j = 0; j < i; j++) {
          key = d.readString(key);
          d.readFloat();
          d.readFloat();
          d.readFloat();
          d.readFloat();
        }
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      int items = sourceData.length / 4;
      e.writeMapStart();
      e.setItemCount(items);
      Utf8 foo = new Utf8("foo");
      for (int i = 0; i < sourceData.length; i += 4) {
        e.startItem();
        e.writeString(foo);
        e.writeFloat(sourceData[i]);
        e.writeFloat(sourceData[i + 1]);
        e.writeFloat(sourceData[i + 2]);
        e.writeFloat(sourceData[i + 3]);
      }
      e.writeMapEnd();
    }
  }

  private static final String RECORD_SCHEMA = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n" + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n" + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n" + "{ \"name\": \"f6\", \"type\": \"int\" }\n" + "] }";

  private static final String NESTED_RECORD_SCHEMA = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \n" + "{ \"type\": \"record\", \"name\": \"D\", \"fields\": [\n"
      + "{\"name\": \"dbl\", \"type\": \"double\" }]\n" + "} },\n" + "{ \"name\": \"f2\", \"type\": \"D\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"D\" },\n" + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n" + "{ \"name\": \"f6\", \"type\": \"int\" }\n" + "] }";

  private static class Rec {
    double f1;
    double f2;
    double f3;
    int f4;
    int f5;
    int f6;

    Rec() {

    }

    Rec(Random r) {
      f1 = r.nextDouble();
      f2 = r.nextDouble();
      f3 = r.nextDouble();
      f4 = r.nextInt();
      f5 = r.nextInt();
      f6 = r.nextInt();
    }
  }

  static class RecordTest extends BasicTest {
    Rec[] sourceData = null;

    public RecordTest() throws IOException {
      this("Record");
    }

    public RecordTest(String name) throws IOException {
      super(name, RECORD_SCHEMA, 6);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new Rec[count];
      for (int i = 0; i < sourceData.length; i++) {
        sourceData[i] = new Rec(r);
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        d.readDouble();
        d.readDouble();
        d.readDouble();
        d.readInt();
        d.readInt();
        d.readInt();
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (Rec r : sourceData) {
        e.writeDouble(r.f1);
        e.writeDouble(r.f2);
        e.writeDouble(r.f3);
        e.writeInt(r.f4);
        e.writeInt(r.f5);
        e.writeInt(r.f6);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class ValidatingRecord extends RecordTest {
    ValidatingRecord() throws IOException {
      super("ValidatingRecord");
    }

    @Override
    protected Decoder getDecoder() throws IOException {
      return new ValidatingDecoder(schema, super.getDecoder());
    }

    @Override
    protected Encoder newEncoder(ByteArrayOutputStream out) throws IOException {
      return encoder_factory.validatingEncoder(schema, super.newEncoder(out));
    }
  }

  static class ResolvingRecord extends RecordTest {
    public ResolvingRecord() throws IOException {
      super("ResolvingRecord");
      isWriteTest = false;
    }

    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, schema, super.getDecoder());
    }
  }

  private static final String RECORD_SCHEMA_WITH_DEFAULT = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n" + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n" + "{ \"name\": \"f4\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n" + "{ \"name\": \"f6\", \"type\": \"int\" },\n"
      + "{ \"name\": \"f7\", \"type\": \"string\", " + "\"default\": \"undefined\" },\n"
      + "{ \"name\": \"f8\", \"type\": \"string\"," + "\"default\": \"undefined\" }\n" + "] }";

  private static final String RECORD_SCHEMA_WITH_OUT_OF_ORDER = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n" + "{ \"name\": \"f3\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"int\" },\n" + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f4\", \"type\": \"int\" },\n" + "{ \"name\": \"f6\", \"type\": \"int\" }\n" + "] }";

  private static final String RECORD_SCHEMA_WITH_PROMOTION = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"double\" },\n" + "{ \"name\": \"f2\", \"type\": \"double\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"double\" },\n" + "{ \"name\": \"f4\", \"type\": \"long\" },\n"
      + "{ \"name\": \"f5\", \"type\": \"long\" },\n" + "{ \"name\": \"f6\", \"type\": \"long\" }\n" + "] }";

  /**
   * Tests the performance of introducing default values.
   */
  static class RecordWithDefault extends RecordTest {
    private final Schema readerSchema;

    public RecordWithDefault() throws IOException {
      super("RecordWithDefault");
      readerSchema = new Schema.Parser().parse(RECORD_SCHEMA_WITH_DEFAULT);
      isWriteTest = false;
    }

    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, readerSchema, super.getDecoder());
    }

    @Override
    protected void readInternal(Decoder d) throws IOException {
      ResolvingDecoder r = (ResolvingDecoder) d;
      Field[] ff = r.readFieldOrder();
      for (int i = 0; i < count; i++) {
        for (Field f : ff) {
          switch (f.pos()) {
          case 0:
          case 1:
          case 2:
            r.readDouble();
            break;
          case 3:
          case 4:
          case 5:
            r.readInt();
            break;
          case 6:
          case 7:
            r.readString(null);
            break;
          }
        }
      }
    }
  }

  /**
   * Tests the performance of resolving a change in field order.
   */
  static class RecordWithOutOfOrder extends RecordTest {
    private final Schema readerSchema;

    public RecordWithOutOfOrder() throws IOException {
      super("RecordWithOutOfOrder");
      readerSchema = new Schema.Parser().parse(RECORD_SCHEMA_WITH_OUT_OF_ORDER);
      isWriteTest = false;
    }

    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, readerSchema, super.getDecoder());
    }

    @Override
    protected void readInternal(Decoder d) throws IOException {
      ResolvingDecoder r = (ResolvingDecoder) d;
      Field[] ff = r.readFieldOrder();
      for (int i = 0; i < count; i++) {
        for (Field f : ff) {
          switch (f.pos()) {
          case 0:
          case 1:
          case 3:
            r.readDouble();
            break;
          case 2:
          case 4:
          case 5:
            r.readInt();
            break;
          }
        }
      }
    }
  }

  /**
   * Tests the performance of resolving a type promotion.
   */
  static class RecordWithPromotion extends RecordTest {
    private final Schema readerSchema;

    public RecordWithPromotion() throws IOException {
      super("RecordWithPromotion");
      readerSchema = new Schema.Parser().parse(RECORD_SCHEMA_WITH_PROMOTION);
      isWriteTest = false;
    }

    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(schema, readerSchema, super.getDecoder());
    }

    @Override
    protected void readInternal(Decoder d) throws IOException {
      ResolvingDecoder r = (ResolvingDecoder) d;
      Field[] ff = r.readFieldOrder();
      for (int i = 0; i < count; i++) {
        for (Field f : ff) {
          switch (f.pos()) {
          case 0:
          case 1:
          case 2:
            r.readDouble();
            break;
          case 3:
          case 4:
          case 5:
            r.readLong();
            break;
          }
        }
      }
    }
  }

  static class GenericTest extends BasicTest {
    GenericRecord[] sourceData = null;
    protected final GenericDatumReader<Object> reader;

    public GenericTest() throws IOException {
      this("Generic");
    }

    protected GenericTest(String name) throws IOException {
      this(name, RECORD_SCHEMA);
    }

    protected GenericTest(String name, String writerSchema) throws IOException {
      super(name, writerSchema, 12);
      reader = newReader();
    }

    protected GenericDatumReader<Object> getReader() {
      return reader;
    }

    protected GenericDatumReader<Object> newReader() {
      return new GenericDatumReader<>(schema);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new GenericRecord[count];
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put(0, r.nextDouble());
        rec.put(1, r.nextDouble());
        rec.put(2, r.nextDouble());
        rec.put(3, r.nextInt());
        rec.put(4, r.nextInt());
        rec.put(5, r.nextInt());
        sourceData[i] = rec;
      }
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        getReader().read(null, d);
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      GenericDatumWriter<Object> writer = new GenericDatumWriter<>(schema);
      for (GenericRecord rec : sourceData) {
        writer.write(rec, e);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  private static final String GENERIC_STRINGS = "{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n"
      + "{ \"name\": \"f1\", \"type\": \"string\" },\n" + "{ \"name\": \"f2\", \"type\": \"string\" },\n"
      + "{ \"name\": \"f3\", \"type\": \"string\" }\n" + "] }";

  static class GenericStrings extends GenericTest {
    public GenericStrings() throws IOException {
      super("GenericStrings", GENERIC_STRINGS);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new GenericRecord[count];
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = new GenericData.Record(schema);
        rec.put(0, randomString(r));
        rec.put(1, randomString(r));
        rec.put(2, randomString(r));
        sourceData[i] = rec;
      }
    }
  }

  static class GenericNested extends GenericTest {
    public GenericNested() throws IOException {
      super("GenericNested_", NESTED_RECORD_SCHEMA);
    }

    @Override
    void genSourceData() {
      sourceData = generateGenericNested(schema, count);
    }
  }

  static GenericRecord[] generateGenericNested(Schema schema, int count) {
    Random r = newRandom();
    GenericRecord[] sourceData = new GenericRecord[count];
    Schema doubleSchema = schema.getFields().get(0).schema();
    for (int i = 0; i < sourceData.length; i++) {
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
      sourceData[i] = rec;
    }
    return sourceData;
  }

  static class GenericNestedFake extends BasicTest {
    // reads and writes generic data, but not using
    // GenericDatumReader or GenericDatumWriter
    GenericRecord[] sourceData = null;

    public GenericNestedFake() throws IOException {
      super("GenericNestedFake_", NESTED_RECORD_SCHEMA, 12);
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      Schema doubleSchema = schema.getFields().get(0).schema();
      for (int i = 0; i < count; i++) {
        GenericRecord rec = new GenericData.Record(schema);
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

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (GenericRecord rec : sourceData) {
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

    @Override
    void genSourceData() {
      sourceData = generateGenericNested(schema, count);
    }

    @Override
    void reset() {
      data = null;
      sourceData = null;
    }

  }

  private static abstract class GenericResolving extends GenericTest {
    protected GenericResolving(String name) throws IOException {
      super(name);
      isWriteTest = false;
    }

    @Override
    protected GenericDatumReader<Object> newReader() {
      return new GenericDatumReader<>(schema, getReaderSchema());
    }

    protected abstract Schema getReaderSchema();
  }

  static class GenericWithDefault extends GenericResolving {
    GenericWithDefault() throws IOException {
      super("GenericWithDefault_");
    }

    @Override
    protected Schema getReaderSchema() {
      return new Schema.Parser().parse(RECORD_SCHEMA_WITH_DEFAULT);
    }
  }

  static class GenericWithOutOfOrder extends GenericResolving {
    GenericWithOutOfOrder() throws IOException {
      super("GenericWithOutOfOrder_");
    }

    @Override
    protected Schema getReaderSchema() {
      return new Schema.Parser().parse(RECORD_SCHEMA_WITH_OUT_OF_ORDER);
    }
  }

  static class GenericWithPromotion extends GenericResolving {
    GenericWithPromotion() throws IOException {
      super("GenericWithPromotion_");
    }

    @Override
    protected Schema getReaderSchema() {
      return new Schema.Parser().parse(RECORD_SCHEMA_WITH_PROMOTION);
    }
  }

  static class GenericOneTimeDecoderUse extends GenericTest {
    public GenericOneTimeDecoderUse() throws IOException {
      super("GenericOneTimeDecoderUse_");
      isWriteTest = false;
    }

    @Override
    protected Decoder getDecoder() {
      return newDecoder();
    }
  }

  static class GenericOneTimeReaderUse extends GenericTest {
    public GenericOneTimeReaderUse() throws IOException {
      super("GenericOneTimeReaderUse_");
      isWriteTest = false;
    }

    @Override
    protected GenericDatumReader<Object> getReader() {
      return newReader();
    }
  }

  static class GenericOneTimeUse extends GenericTest {
    public GenericOneTimeUse() throws IOException {
      super("GenericOneTimeUse_");
      isWriteTest = false;
    }

    @Override
    protected GenericDatumReader<Object> getReader() {
      return newReader();
    }

    @Override
    protected Decoder getDecoder() {
      return newDecoder();
    }
  }

  static abstract class SpecificTest<T extends SpecificRecordBase> extends BasicTest {
    protected final SpecificDatumReader<T> reader;
    protected final SpecificDatumWriter<T> writer;
    private Object[] sourceData;
    private T reuse;

    protected SpecificTest(String name, String writerSchema) throws IOException {
      super(name, writerSchema, 48);
      reader = newReader();
      writer = newWriter();
    }

    protected SpecificDatumReader<T> newReader() {
      return new SpecificDatumReader<>(schema);
    }

    protected SpecificDatumWriter<T> newWriter() {
      return new SpecificDatumWriter<>(schema);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      sourceData = new Object[count];
      for (int i = 0; i < sourceData.length; i++) {
        sourceData[i] = genSingleRecord(r);
      }
      reuse = genSingleRecord(r);
    }

    protected abstract T genSingleRecord(Random r);

    @Override
    void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        reader.read(reuse, d);
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      for (Object sourceDatum : sourceData) {
        @SuppressWarnings("unchecked")
        T rec = (T) sourceDatum;
        writer.write(rec, e);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class FooBarSpecificRecordTest extends SpecificTest<FooBarSpecificRecord> {
    public FooBarSpecificRecordTest() throws IOException {
      super("FooBarSpecificRecordTest", FooBarSpecificRecord.SCHEMA$.toString());
    }

    @Override
    protected FooBarSpecificRecord genSingleRecord(Random r) {
      TypeEnum[] typeEnums = TypeEnum.values();
      List<Integer> relatedIds = new ArrayList<>(10);
      for (int i = 0; i < 10; i++) {
        relatedIds.add(r.nextInt());
      }

      try {
        String[] nicknames = { randomString(r), randomString(r) };
        return FooBarSpecificRecord.newBuilder().setId(r.nextInt()).setName(randomString(r))
            .setNicknames(new ArrayList<>(Arrays.asList(nicknames))).setTypeEnum(typeEnums[r.nextInt(typeEnums.length)])
            .setRelatedids(relatedIds).build();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  static abstract class ReflectTest<T> extends BasicTest {
    T[] sourceData = null;
    ReflectDatumReader<T> reader;
    ReflectDatumWriter<T> writer;
    Class<T> clazz;

    @SuppressWarnings("unchecked")
    ReflectTest(String name, T sample, int factor) throws IOException {
      super(name, ReflectData.get().getSchema(sample.getClass()).toString(), factor);
      clazz = (Class<T>) sample.getClass();
      reader = new ReflectDatumReader<>(schema);
      writer = new ReflectDatumWriter<>(schema);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected final void genSourceData() {
      Random r = newRandom();
      sourceData = (T[]) Array.newInstance(clazz, count);
      for (int i = 0; i < sourceData.length; i++) {
        sourceData[i] = createDatum(r);
      }
    }

    protected abstract T createDatum(Random r);

    @Override
    protected final void readInternal(Decoder d) throws IOException {
      for (int i = 0; i < count; i++) {
        reader.read(null, d);
      }
    }

    @Override
    protected final void writeInternal(Encoder e) throws IOException {
      for (T sourceDatum : sourceData) {
        writer.write(sourceDatum, e);
      }
    }

    @Override
    protected final void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class ReflectRecordTest extends ReflectTest<Rec> {
    ReflectRecordTest() throws IOException {
      super("ReflectRecord", new Rec(), 12);
    }

    @Override
    protected Rec createDatum(Random r) {
      return new Rec(r);
    }
  }

  static class ReflectFloatTest extends ReflectTest<float[]> {
    ReflectFloatTest() throws IOException {
      super("ReflectFloat", new float[0], COUNT);
    }

    @Override
    protected float[] createDatum(Random r) {
      return populateFloatArray(r, COUNT / count);
    }
  }

  static class ReflectDoubleTest extends ReflectTest<double[]> {
    ReflectDoubleTest() throws IOException {
      super("ReflectDouble", new double[0], COUNT);
    }

    @Override
    protected double[] createDatum(Random r) {
      return populateDoubleArray(r, COUNT / count);
    }
  }

  static class ReflectFloatArrayTest extends ReflectTest<float[]> {
    ReflectFloatArrayTest() throws IOException {
      super("ReflectFloatArray", new float[0], 10);
    }

    @Override
    protected float[] createDatum(Random r) {
      return populateFloatArray(r, false);
    }
  }

  static class ReflectDoubleArrayTest extends ReflectTest<double[]> {
    ReflectDoubleArrayTest() throws IOException {
      super("ReflectDoubleArray", new double[0], 20);
    }

    @Override
    protected double[] createDatum(Random r) {
      return populateDoubleArray(r);
    }
  }

  static class ReflectIntArrayTest extends ReflectTest<int[]> {
    ReflectIntArrayTest() throws IOException {
      super("ReflectIntArray", new int[0], 12);
    }

    @Override
    protected int[] createDatum(Random r) {
      return populateIntArray(r);
    }
  }

  static class ReflectLongArrayTest extends ReflectTest<long[]> {
    ReflectLongArrayTest() throws IOException {
      super("ReflectLongArray", new long[0], 24);
    }

    @Override
    protected long[] createDatum(Random r) {
      return populateLongArray(r);
    }
  }

  static class ReflectNestedObjectArrayTest extends ReflectTest<ReflectNestedObjectArrayTest.Foo> {
    ReflectNestedObjectArrayTest() throws IOException {
      super("ReflectNestedObjectArray", new Foo(new Random()), 50);
    }

    @Override
    protected Foo createDatum(Random r) {
      return new Foo(r);
    }

    static public class Foo {
      Vals[] bar;

      Foo() {
      }

      Foo(Random r) {
        bar = new Vals[smallArraySize(r)];
        for (int i = 0; i < bar.length; i++) {
          bar[i] = new Vals(r);
        }
      }
    }

    static class Vals {
      float f1;
      float f2;
      float f3;
      float f4;

      Vals() {
      }

      Vals(Random r) {
        this.f1 = r.nextFloat();
        this.f2 = r.nextFloat();
        this.f3 = r.nextFloat();
        this.f4 = r.nextFloat();
      }
    }

  }

  static public class FloatFoo {
    float[] floatBar;

    FloatFoo() {
    }

    FloatFoo(Random r, boolean large) {
      floatBar = populateFloatArray(r, large);
    }
  }

  // average of 8, between 1 and 15
  private static int smallArraySize(Random r) {
    return r.nextInt(15) + 1;
  }

  // average of 64, between 16 and 112
  private static int largeArraySize(Random r) {
    return r.nextInt(97) + 16;
  }

  static float[] populateFloatArray(Random r, boolean large) {
    int size = large ? largeArraySize(r) : smallArraySize(r);
    return populateFloatArray(r, size);
  }

  static float[] populateFloatArray(Random r, int size) {
    float[] result = new float[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextFloat();
    }
    return result;
  }

  static double[] populateDoubleArray(Random r) {
    return populateDoubleArray(r, smallArraySize(r));
  }

  static double[] populateDoubleArray(Random r, int size) {
    double[] result = new double[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextDouble();
    }
    return result;
  }

  static int[] populateIntArray(Random r) {
    int size = smallArraySize(r);
    int[] result = new int[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextInt();
    }
    return result;
  }

  static long[] populateLongArray(Random r) {
    int size = smallArraySize(r);
    long[] result = new long[size];
    for (int i = 0; i < result.length; i++) {
      result[i] = r.nextLong();
    }
    return result;
  }

  static class ReflectNestedFloatArrayTest extends ReflectTest<FloatFoo> {
    public ReflectNestedFloatArrayTest() throws IOException {
      super("ReflectNestedFloatArray", new FloatFoo(new Random(), false), 10);
    }

    @Override
    protected FloatFoo createDatum(Random r) {
      return new FloatFoo(r, false);
    }
  }

  static class ReflectNestedLargeFloatArrayTest extends ReflectTest<FloatFoo> {
    public ReflectNestedLargeFloatArrayTest() throws IOException {
      super("ReflectNestedLargeFloatArray", new FloatFoo(new Random(), true), 60);
    }

    @Override
    protected FloatFoo createDatum(Random r) {
      return new FloatFoo(r, true);
    }

  }

  static class ReflectNestedLargeFloatArrayBlockedTest extends ReflectTest<FloatFoo> {
    public ReflectNestedLargeFloatArrayBlockedTest() throws IOException {
      super("ReflectNestedLargeFloatArrayBlocked", new FloatFoo(new Random(), true), 60);
    }

    @Override
    protected FloatFoo createDatum(Random r) {
      return new FloatFoo(r, true);
    }

    @Override
    protected Encoder newEncoder(ByteArrayOutputStream out) throws IOException {
      return new EncoderFactory().configureBlockSize(254).blockingBinaryEncoder(out, null);
    }

  }

  @SuppressWarnings("unused")
  private static class Rec1 {
    double d1;
    double d11;
    float f2;
    float f22;
    int f3;
    int f33;
    long f4;
    long f44;
    byte f5;
    byte f55;
    short f6;
    short f66;

    Rec1() {
    }

    Rec1(Random r) {
      d1 = r.nextDouble();
      d11 = r.nextDouble();
      f2 = r.nextFloat();
      f22 = r.nextFloat();
      f3 = r.nextInt();
      f33 = r.nextInt();
      f4 = r.nextLong();
      f44 = r.nextLong();
      f5 = (byte) r.nextInt();
      f55 = (byte) r.nextInt();
      f6 = (short) r.nextInt();
      f66 = (short) r.nextInt();
    }
  }

  static class ReflectBigRecordTest extends ReflectTest<Rec1> {
    public ReflectBigRecordTest() throws IOException {
      super("ReflectBigRecord", new Rec1(new Random()), 20);
    }

    @Override
    protected Rec1 createDatum(Random r) {
      return new Rec1(r);
    }
  }

  static abstract class ResolvingTest extends BasicTest {
    GenericRecord[] sourceData = null;
    Schema writeSchema;

    private static String mkSchema(String subschema) {
      return ("{ \"type\": \"record\", \"name\": \"R\", \"fields\": [\n" + "{ \"name\": \"f\", \"type\": " + subschema
          + "}\n" + "] }");
    }

    public ResolvingTest(String name, String r, String w) throws IOException {
      super(name, mkSchema(r));
      isWriteTest = false;
      this.writeSchema = new Schema.Parser().parse(mkSchema(w));
    }

    @Override
    protected Decoder getDecoder() throws IOException {
      return new ResolvingDecoder(writeSchema, schema, super.getDecoder());
    }

    @Override
    void readInternal(Decoder d) throws IOException {
      GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
      for (int i = 0; i < count; i++) {
        reader.read(null, d);
      }
    }

    @Override
    void writeInternal(Encoder e) throws IOException {
      GenericDatumWriter<Object> writer = new GenericDatumWriter<>(writeSchema);
      for (GenericRecord sourceDatum : sourceData) {
        writer.write(sourceDatum, e);
      }
    }

    @Override
    void reset() {
      sourceData = null;
      data = null;
    }
  }

  static class ExtendedEnumResolveTest extends ResolvingTest {
    private static final String ENUM_WRITER = "{ \"type\": \"enum\", \"name\":\"E\", \"symbols\": [\"A\", \"B\"] }";
    private static final String ENUM_READER = "{ \"type\": \"enum\", \"name\":\"E\", \"symbols\": [\"A\",\"B\",\"C\",\"D\",\"E\"] }";

    public ExtendedEnumResolveTest() throws IOException {
      super("ExtendedEnum", ENUM_READER, ENUM_WRITER);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      Schema eSchema = writeSchema.getField("f").schema();
      sourceData = new GenericRecord[count];
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = new GenericData.Record(writeSchema);
        int tag = r.nextInt(2);
        rec.put("f", GenericData.get().createEnum(eSchema.getEnumSymbols().get(tag), eSchema));
        sourceData[i] = rec;
      }
    }
  }

  static class UnchangedUnionResolveTest extends ResolvingTest {
    private static final String UNCHANGED_UNION = "[ \"null\", \"int\" ]";

    public UnchangedUnionResolveTest() throws IOException {
      super("UnchangedUnion", UNCHANGED_UNION, UNCHANGED_UNION);
    }

    @Override
    void genSourceData() {
      Random r = newRandom();
      Schema uSchema = writeSchema.getField("f").schema();
      sourceData = new GenericRecord[count];
      for (int i = 0; i < sourceData.length; i++) {
        GenericRecord rec = new GenericData.Record(writeSchema);
        int val = r.nextInt(1000000);
        Integer v = (val < 750000 ? val : null);
        rec.put("f", v);
        sourceData[i] = rec;
      }
    }
  }
}
