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
package org.apache.avro.tool;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class TestCatTool {

  @Rule
  public TestName name = new TestName();

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  private static final int ROWS_IN_INPUT_FILES = 100000;
  private static final int OFFSET = 1000;
  private static final int LIMIT_WITHIN_INPUT_BOUNDS = 100;
  private static final int LIMIT_OUT_OF_INPUT_BOUNDS = 100001;
  private static final double SAMPLERATE = .01;
  private static final double SAMPLERATE_TOO_SMALL = .00000001;

  private final Schema INTSCHEMA = new Schema.Parser().parse("{\"type\":\"record\", " + "\"name\":\"myRecord\", "
      + "\"fields\":[ " + "{\"name\":\"value\",\"type\":\"int\"} " + "]}");
  private final Schema STRINGSCHEMA = new Schema.Parser().parse("{\"type\":\"record\", " + "\"name\":\"myRecord\", "
      + "\"fields\":[ {\"name\":\"value\",\"type\":\"string\"} " + "]}");
  private static final CodecFactory DEFLATE = CodecFactory.deflateCodec(9);
  private static final CodecFactory SNAPPY = CodecFactory.snappyCodec();

  private GenericRecord aDatum(Type ofType, int forRow) {
    GenericRecord record;
    switch (ofType) {
    case STRING:
      record = new GenericData.Record(STRINGSCHEMA);
      record.put("value", String.valueOf(forRow % 100));
      return record;
    case INT:
      record = new GenericData.Record(INTSCHEMA);
      record.put("value", forRow);
      return record;
    default:
      throw new AssertionError("I can't generate data for this type");
    }
  }

  private File generateData(String file, Type type, Map<String, String> metadata, CodecFactory codec) throws Exception {
    File inputFile = new File(DIR.getRoot(), file);
    inputFile.deleteOnExit();

    Schema schema = null;
    if (type.equals(Schema.Type.INT)) {
      schema = INTSCHEMA;
    }
    if (type.equals(Schema.Type.STRING)) {
      schema = STRINGSCHEMA;
    }

    DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema));
    for (Entry<String, String> metadatum : metadata.entrySet()) {
      writer.setMeta(metadatum.getKey(), metadatum.getValue());
    }
    writer.setCodec(codec);
    writer.create(schema, inputFile);

    for (int i = 0; i < ROWS_IN_INPUT_FILES; i++) {
      writer.append(aDatum(type, i));
    }
    writer.close();

    return inputFile;
  }

  private int getFirstIntDatum(File file) throws Exception {
    DataFileStream<GenericRecord> reader = new DataFileStream<>(new FileInputStream(file), new GenericDatumReader<>());

    int result = (Integer) reader.next().get(0);
    System.out.println(result);
    reader.close();
    return result;
  }

  private int numRowsInFile(File output) throws Exception {
    DataFileStream<GenericRecord> reader = new DataFileStream<>(new FileInputStream(output),
        new GenericDatumReader<>());
    Iterator<GenericRecord> rows = reader.iterator();
    int rowcount = 0;
    while (rows.hasNext()) {
      ++rowcount;
      rows.next();
    }
    reader.close();
    return rowcount;
  }

  @Test
  public void testCat() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData("input1.avro", Type.INT, metadata, DEFLATE);
    File input2 = generateData("input2.avro", Type.INT, metadata, SNAPPY);
    File input3 = generateData("input3.avro", Type.INT, metadata, DEFLATE);

    File output = new File(DIR.getRoot(), name.getMethodName() + ".avro");
    output.deleteOnExit();

//    file input
    List<String> args = asList(input1.getAbsolutePath(), input2.getAbsolutePath(), input3.getAbsolutePath(), "--offset",
        String.valueOf(OFFSET), "--limit", String.valueOf(LIMIT_WITHIN_INPUT_BOUNDS), "--samplerate",
        String.valueOf(SAMPLERATE), output.getAbsolutePath());
    int returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);

    assertEquals(LIMIT_WITHIN_INPUT_BOUNDS, numRowsInFile(output));

//    folder input
    args = asList(input1.getParentFile().getAbsolutePath(), output.getAbsolutePath(), "--offset",
        String.valueOf(OFFSET), "--limit", String.valueOf(LIMIT_WITHIN_INPUT_BOUNDS));
    returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);
    assertEquals(LIMIT_WITHIN_INPUT_BOUNDS, numRowsInFile(output));

//    glob input
    args = asList(new File(input1.getParentFile(), "/*").getAbsolutePath(), output.getAbsolutePath(), "--offset",
        String.valueOf(OFFSET), "--limit", String.valueOf(LIMIT_WITHIN_INPUT_BOUNDS));
    returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);
    assertEquals(LIMIT_WITHIN_INPUT_BOUNDS, numRowsInFile(output));
  }

  @Test
  public void testLimitOutOfBounds() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData("input1.avro", Type.INT, metadata, DEFLATE);
    File output = new File(DIR.getRoot(), name.getMethodName() + ".avro");
    output.deleteOnExit();

    List<String> args = asList(input1.getAbsolutePath(), "--offset=" + String.valueOf(OFFSET),
        "--limit=" + String.valueOf(LIMIT_OUT_OF_INPUT_BOUNDS), output.getAbsolutePath());
    int returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);
    assertEquals(ROWS_IN_INPUT_FILES - OFFSET, numRowsInFile(output));
  }

  @Test
  public void testSamplerateAccuracy() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData("input1.avro", Type.INT, metadata, DEFLATE);
    File output = new File(DIR.getRoot(), name.getMethodName() + ".avro");
    output.deleteOnExit();

    List<String> args = asList(input1.getAbsolutePath(), output.getAbsolutePath(), "--offset", String.valueOf(OFFSET),
        "--samplerate", String.valueOf(SAMPLERATE));
    int returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);

    assertTrue("Outputsize is not roughly (Inputsize - Offset) * samplerate",
        (ROWS_IN_INPUT_FILES - OFFSET) * SAMPLERATE - numRowsInFile(output) < 2);
    assertTrue("", (ROWS_IN_INPUT_FILES - OFFSET) * SAMPLERATE - numRowsInFile(output) > -2);
  }

  @Test
  public void testOffSetAccuracy() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData("input1.avro", Type.INT, metadata, DEFLATE);
    File output = new File(DIR.getRoot(), name.getMethodName() + ".avro");
    output.deleteOnExit();

    List<String> args = asList(input1.getAbsolutePath(), "--offset", String.valueOf(OFFSET), "--limit",
        String.valueOf(LIMIT_WITHIN_INPUT_BOUNDS), "--samplerate", String.valueOf(SAMPLERATE),
        output.getAbsolutePath());
    int returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);
    assertEquals("output does not start at offset", OFFSET, getFirstIntDatum(output));
  }

  @Test
  public void testOffsetBiggerThanInput() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData("input1.avro", Type.INT, metadata, DEFLATE);
    File output = new File(DIR.getRoot(), name.getMethodName() + ".avro");
    output.deleteOnExit();

    List<String> args = asList(input1.getAbsolutePath(), "--offset", String.valueOf(ROWS_IN_INPUT_FILES + 1),
        output.getAbsolutePath());
    int returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);
    assertEquals("output is not empty", 0, numRowsInFile(output));
  }

  @Test
  public void testSamplerateSmallerThanInput() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData("input1.avro", Type.INT, metadata, DEFLATE);
    File output = new File(DIR.getRoot(), name.getMethodName() + ".avro");
    output.deleteOnExit();

    List<String> args = asList(input1.getAbsolutePath(), output.getAbsolutePath(),
        "--offset=" + Integer.toString(OFFSET), "--samplerate=" + Double.toString(SAMPLERATE_TOO_SMALL));
    int returnCode = new CatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);

    assertEquals("output should only contain the record at offset", OFFSET, getFirstIntDatum(output));
  }

  @Test(expected = IOException.class)
  public void testDifferentSchemasFail() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData("input1.avro", Type.STRING, metadata, DEFLATE);
    File input2 = generateData("input2.avro", Type.INT, metadata, DEFLATE);

    File output = new File(DIR.getRoot(), name.getMethodName() + ".avro");
    output.deleteOnExit();

    List<String> args = asList(input1.getAbsolutePath(), input2.getAbsolutePath(), output.getAbsolutePath());
    new CatTool().run(System.in, System.out, System.err, args);
  }

  @Test
  public void testHelpfulMessageWhenNoArgsGiven() throws Exception {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(1024);
    int returnCode;
    try (PrintStream out = new PrintStream(buffer)) {
      returnCode = new CatTool().run(System.in, out, System.err, Collections.emptyList());
    }

    assertEquals(0, returnCode);
    assertTrue("should have lots of help", buffer.toString().trim().length() > 200);
  }
}
