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
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class TestConcatTool {
  private static final int ROWS_IN_INPUT_FILES = 100000;
  private static final CodecFactory DEFLATE = CodecFactory.deflateCodec(9);

  @Rule
  public TestName name = new TestName();

  @Rule
  public TemporaryFolder INPUT_DIR = new TemporaryFolder();

  @Rule
  public TemporaryFolder OUTPUT_DIR = new TemporaryFolder();

  private Object aDatum(Type ofType, int forRow) {
    switch (ofType) {
    case STRING:
      return String.valueOf(forRow % 100);
    case INT:
      return forRow;
    default:
      throw new AssertionError("I can't generate data for this type");
    }
  }

  private File generateData(String file, Type type, Map<String, String> metadata, CodecFactory codec) throws Exception {
    File inputFile = new File(INPUT_DIR.getRoot(), file);
    Schema schema = Schema.create(type);
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
      for (Entry<String, String> metadatum : metadata.entrySet()) {
        writer.setMeta(metadatum.getKey(), metadatum.getValue());
      }
      writer.setCodec(codec);
      writer.create(schema, inputFile);

      for (int i = 0; i < ROWS_IN_INPUT_FILES; i++) {
        writer.append(aDatum(type, i));
      }
    }
    return inputFile;
  }

  private CodecFactory getCodec(File output) throws Exception {
    try (DataFileStream<GenericRecord> reader = new DataFileStream<>(new FileInputStream(output),
        new GenericDatumReader<>())) {
      String codec = reader.getMetaString(DataFileConstants.CODEC);

      return codec == null ? CodecFactory.nullCodec() : CodecFactory.fromString(codec);
    }
  }

  private int numRowsInFile(File output) throws Exception {
    int rowcount = 0;
    try (DataFileStream<Utf8> reader = new DataFileStream<>(new FileInputStream(output), new GenericDatumReader<>())) {
      for (Utf8 ignored : reader) {
        ++rowcount;
      }
    }
    return rowcount;
  }

  @Test
  public void testDirConcat() throws Exception {
    Map<String, String> metadata = new HashMap<>();

    for (int i = 0; i < 3; i++) {
      generateData(name.getMethodName() + "-" + i + ".avro", Type.STRING, metadata, DEFLATE);
    }

    File output = new File(OUTPUT_DIR.getRoot(), name.getMethodName() + ".avro");

    List<String> args = asList(INPUT_DIR.getRoot().getAbsolutePath(), output.getAbsolutePath());
    int returnCode = new ConcatTool().run(System.in, System.out, System.err, args);

    assertEquals(0, returnCode);
    assertEquals(ROWS_IN_INPUT_FILES * 3, numRowsInFile(output));
  }

  @Test
  public void testGlobPatternConcat() throws Exception {
    Map<String, String> metadata = new HashMap<>();

    for (int i = 0; i < 3; i++) {
      generateData(name.getMethodName() + "-" + i + ".avro", Type.STRING, metadata, DEFLATE);
    }

    File output = new File(OUTPUT_DIR.getRoot(), name.getMethodName() + ".avro");

    List<String> args = asList(new File(INPUT_DIR.getRoot(), "/*").getAbsolutePath(), output.getAbsolutePath());
    int returnCode = new ConcatTool().run(System.in, System.out, System.err, args);

    assertEquals(0, returnCode);
    assertEquals(ROWS_IN_INPUT_FILES * 3, numRowsInFile(output));
  }

  @Test(expected = FileNotFoundException.class)
  public void testFileDoesNotExist() throws Exception {
    File output = new File(INPUT_DIR.getRoot(), name.getMethodName() + ".avro");

    List<String> args = asList(new File(INPUT_DIR.getRoot(), "/doNotExist").getAbsolutePath(),
        output.getAbsolutePath());
    new ConcatTool().run(System.in, System.out, System.err, args);
  }

  @Test
  public void testConcat() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData(name.getMethodName() + "-1.avro", Type.STRING, metadata, DEFLATE);
    File input2 = generateData(name.getMethodName() + "-2.avro", Type.STRING, metadata, DEFLATE);
    File input3 = generateData(name.getMethodName() + "-3.avro", Type.STRING, metadata, DEFLATE);

    File output = new File(OUTPUT_DIR.getRoot(), name.getMethodName() + ".avro");

    List<String> args = asList(input1.getAbsolutePath(), input2.getAbsolutePath(), input3.getAbsolutePath(),
        output.getAbsolutePath());
    int returnCode = new ConcatTool().run(System.in, System.out, System.err, args);
    assertEquals(0, returnCode);

    assertEquals(ROWS_IN_INPUT_FILES * 3, numRowsInFile(output));
    assertEquals(getCodec(input1).getClass(), getCodec(output).getClass());
  }

  @Test
  public void testDifferentSchemasFail() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData(name.getMethodName() + "-1.avro", Type.STRING, metadata, DEFLATE);
    File input2 = generateData(name.getMethodName() + "-2.avro", Type.INT, metadata, DEFLATE);

    File output = new File(OUTPUT_DIR.getRoot(), name.getMethodName() + ".avro");

    List<String> args = asList(input1.getAbsolutePath(), input2.getAbsolutePath(), output.getAbsolutePath());
    int returnCode = new ConcatTool().run(System.in, System.out, System.err, args);
    assertEquals(1, returnCode);
  }

  @Test
  public void testDifferentMetadataFail() throws Exception {
    Map<String, String> metadata1 = new HashMap<>();
    metadata1.put("myMetaKey", "myMetaValue");
    Map<String, String> metadata2 = new HashMap<>();
    metadata2.put("myOtherMetaKey", "myOtherMetaValue");

    File input1 = generateData(name.getMethodName() + "-1.avro", Type.STRING, metadata1, DEFLATE);
    File input2 = generateData(name.getMethodName() + "-2.avro", Type.STRING, metadata2, DEFLATE);

    File output = new File(OUTPUT_DIR.getRoot(), name.getMethodName() + ".avro");

    List<String> args = asList(input1.getAbsolutePath(), input2.getAbsolutePath(), output.getAbsolutePath());
    int returnCode = new ConcatTool().run(System.in, System.out, System.err, args);
    assertEquals(2, returnCode);
  }

  @Test
  public void testDifferentCodecFail() throws Exception {
    Map<String, String> metadata = new HashMap<>();
    metadata.put("myMetaKey", "myMetaValue");

    File input1 = generateData(name.getMethodName() + "-1.avro", Type.STRING, metadata, DEFLATE);
    File input2 = generateData(name.getMethodName() + "-2.avro", Type.STRING, metadata, CodecFactory.nullCodec());

    File output = new File(OUTPUT_DIR.getRoot(), name.getMethodName() + ".avro");

    List<String> args = asList(input1.getAbsolutePath(), input2.getAbsolutePath(), output.getAbsolutePath());
    int returnCode = new ConcatTool().run(System.in, System.out, System.err, args);
    assertEquals(3, returnCode);
  }

  @Test
  public void testHelpfulMessageWhenNoArgsGiven() throws Exception {
    int returnCode;
    try (ByteArrayOutputStream buffer = new ByteArrayOutputStream(1024)) {
      try (PrintStream out = new PrintStream(buffer)) {
        returnCode = new ConcatTool().run(System.in, out, System.err, Collections.emptyList());
      }
      assertTrue("should have lots of help", buffer.toString().trim().length() > 200);
    }
    assertEquals(0, returnCode);
  }
}
