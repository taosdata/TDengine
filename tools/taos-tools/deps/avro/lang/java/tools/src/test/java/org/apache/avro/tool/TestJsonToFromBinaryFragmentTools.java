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

import static org.junit.Assert.assertEquals;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests both {@link JsonToBinaryFragmentTool} and
 * {@link BinaryFragmentToJsonTool}.
 */
public class TestJsonToFromBinaryFragmentTools {
  private static final String STRING_SCHEMA = Schema.create(Type.STRING).toString();
  private static final String UTF8 = "utf-8";
  private static final String AVRO = "ZLong string implies readable length encoding.";
  private static final String JSON = "\"Long string implies readable length encoding.\"\n";

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  @Test
  public void testBinaryToJson() throws Exception {
    binaryToJson(AVRO, JSON, STRING_SCHEMA);
  }

  @Test
  public void testJsonToBinary() throws Exception {
    jsonToBinary(JSON, AVRO, STRING_SCHEMA);
  }

  @Test
  public void testMultiBinaryToJson() throws Exception {
    binaryToJson(AVRO + AVRO + AVRO, JSON + JSON + JSON, STRING_SCHEMA);
  }

  @Test
  public void testMultiJsonToBinary() throws Exception {
    jsonToBinary(JSON + JSON + JSON, AVRO + AVRO + AVRO, STRING_SCHEMA);
  }

  @Test
  public void testBinaryToNoPrettyJson() throws Exception {
    binaryToJson(AVRO, JSON, "--no-pretty", STRING_SCHEMA);
  }

  @Test
  public void testMultiBinaryToNoPrettyJson() throws Exception {
    binaryToJson(AVRO + AVRO + AVRO, JSON + JSON + JSON, "--no-pretty", STRING_SCHEMA);
  }

  @Test
  public void testBinaryToJsonSchemaFile() throws Exception {
    binaryToJson(AVRO, JSON, "--schema-file", schemaFile(DIR.getRoot()));
  }

  @Test
  public void testJsonToBinarySchemaFile() throws Exception {
    jsonToBinary(JSON, AVRO, "--schema-file", schemaFile(DIR.getRoot()));
  }

  private void binaryToJson(String avro, String json, String... options) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(new BufferedOutputStream(baos));

    List<String> args = new ArrayList<>(Arrays.asList(options));
    args.add("-");
    new BinaryFragmentToJsonTool().run(new ByteArrayInputStream(avro.getBytes(StandardCharsets.UTF_8)), // stdin
        p, // stdout
        null, // stderr
        args);
    System.out.println(baos.toString(UTF8).replace("\r", ""));
    assertEquals(json, baos.toString(UTF8).replace("\r", ""));
  }

  private void jsonToBinary(String json, String avro, String... options) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream p = new PrintStream(new BufferedOutputStream(baos));

    List<String> args = new ArrayList<>(Arrays.asList(options));
    args.add("-");
    new JsonToBinaryFragmentTool().run(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), // stdin
        p, // stdout
        null, // stderr
        args);
    assertEquals(avro, baos.toString(UTF8));
  }

  private static String schemaFile(File dir) throws IOException {
    File schemaFile = new File(dir, "String.avsc");
    try (FileWriter fw = new FileWriter(schemaFile)) {
      fw.append(STRING_SCHEMA);
    }
    return schemaFile.toString();
  }
}
