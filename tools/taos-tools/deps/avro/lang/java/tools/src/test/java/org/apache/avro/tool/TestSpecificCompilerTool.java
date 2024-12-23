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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Verifies that the SpecificCompilerTool generates Java source properly
 */
public class TestSpecificCompilerTool {

  // where test input/expected output comes from
  private static final File TEST_DIR = new File(System.getProperty("test.compile.schema.dir", "src/test/compiler"));

  // where test input comes from
  private static final File TEST_INPUT_DIR = new File(TEST_DIR, "input");

  // where test expected output comes from
  private static final File TEST_EXPECTED_OUTPUT_DIR = new File(TEST_DIR, "output");
  private static final File TEST_EXPECTED_POSITION = new File(TEST_EXPECTED_OUTPUT_DIR, "Position.java");
  private static final File TEST_EXPECTED_PLAYER = new File(TEST_EXPECTED_OUTPUT_DIR, "Player.java");
  private static final File TEST_EXPECTED_NO_SETTERS = new File(TEST_EXPECTED_OUTPUT_DIR, "NoSettersTest.java");
  private static final File TEST_EXPECTED_OPTIONAL_GETTERS_FOR_NULLABLE_FIELDS = new File(TEST_EXPECTED_OUTPUT_DIR,
      "OptionalGettersNullableFieldsTest.java");
  private static final File TEST_EXPECTED_OPTIONAL_GETTERS_FOR_ALL_FIELDS = new File(TEST_EXPECTED_OUTPUT_DIR,
      "OptionalGettersAllFieldsTest.java");
  private static final File TEST_EXPECTED_ADD_EXTRA_OPTIONAL_GETTERS = new File(TEST_EXPECTED_OUTPUT_DIR,
      "AddExtraOptionalGettersTest.java");

  private static final File TEST_EXPECTED_STRING_OUTPUT_DIR = new File(TEST_DIR, "output-string");
  private static final File TEST_EXPECTED_STRING_POSITION = new File(TEST_EXPECTED_STRING_OUTPUT_DIR,
      "avro/examples/baseball/Position.java");
  private static final File TEST_EXPECTED_STRING_PLAYER = new File(TEST_EXPECTED_STRING_OUTPUT_DIR,
      "avro/examples/baseball/Player.java");
  private static final File TEST_EXPECTED_STRING_FIELDTEST = new File(TEST_EXPECTED_STRING_OUTPUT_DIR,
      "avro/examples/baseball/FieldTest.java");
  private static final File TEST_EXPECTED_STRING_PROTO = new File(TEST_EXPECTED_STRING_OUTPUT_DIR,
      "avro/examples/baseball/Proto.java");

  // where test output goes
  private static final File TEST_OUTPUT_DIR = new File("target/compiler/output");
  private static final File TEST_OUTPUT_PLAYER = new File(TEST_OUTPUT_DIR, "avro/examples/baseball/Player.java");
  private static final File TEST_OUTPUT_POSITION = new File(TEST_OUTPUT_DIR, "avro/examples/baseball/Position.java");
  private static final File TEST_OUTPUT_NO_SETTERS = new File(TEST_OUTPUT_DIR,
      "avro/examples/baseball/NoSettersTest.java");
  private static final File TEST_OUTPUT_OPTIONAL_GETTERS_NULLABLE_FIELDS = new File(TEST_OUTPUT_DIR,
      "avro/examples/baseball/OptionalGettersNullableFieldsTest.java");
  private static final File TEST_OUTPUT_OPTIONAL_GETTERS_ALL_FIELDS = new File(TEST_OUTPUT_DIR,
      "avro/examples/baseball/OptionalGettersAllFieldsTest.java");
  private static final File TEST_OUTPUT_ADD_EXTRA_OPTIONAL_GETTERS = new File(TEST_OUTPUT_DIR,
      "avro/examples/baseball/AddExtraOptionalGettersTest.java");

  private static final File TEST_OUTPUT_STRING_DIR = new File("target/compiler/output-string");
  private static final File TEST_OUTPUT_STRING_PLAYER = new File(TEST_OUTPUT_STRING_DIR,
      "avro/examples/baseball/Player.java");
  private static final File TEST_OUTPUT_STRING_POSITION = new File(TEST_OUTPUT_STRING_DIR,
      "avro/examples/baseball/Position.java");
  private static final File TEST_OUTPUT_STRING_FIELDTEST = new File(TEST_OUTPUT_STRING_DIR,
      "avro/examples/baseball/FieldTest.java");
  private static final File TEST_OUTPUT_STRING_PROTO = new File(TEST_OUTPUT_STRING_DIR,
      "avro/examples/baseball/Proto.java");

  @Before
  public void setUp() {
    TEST_OUTPUT_DIR.delete();
  }

  @Test
  public void testCompileSchemaWithExcludedSetters() throws Exception {

    TEST_OUTPUT_NO_SETTERS.delete();
    doCompile(new String[] { "-encoding", "UTF-8", "-noSetters", "schema",
        TEST_INPUT_DIR.toString() + "/nosetterstest.avsc", TEST_OUTPUT_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_NO_SETTERS, TEST_OUTPUT_NO_SETTERS);
  }

  @Test
  public void testCompileSchemaWithOptionalGettersForNullableFieldsOnly() throws Exception {

    TEST_OUTPUT_OPTIONAL_GETTERS_NULLABLE_FIELDS.delete();
    doCompile(new String[] { "-encoding", "UTF-8", "-optionalGetters", "only_nullable_fields", "schema",
        TEST_INPUT_DIR.toString() + "/optionalgettersnullablefieldstest.avsc", TEST_OUTPUT_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_OPTIONAL_GETTERS_FOR_NULLABLE_FIELDS, TEST_OUTPUT_OPTIONAL_GETTERS_NULLABLE_FIELDS);
  }

  @Test
  public void testCompileSchemaWithOptionalGettersForAllFields() throws Exception {

    TEST_OUTPUT_OPTIONAL_GETTERS_ALL_FIELDS.delete();
    doCompile(new String[] { "-encoding", "UTF-8", "-optionalGetters", "all_fields", "schema",
        TEST_INPUT_DIR.toString() + "/optionalgettersallfieldstest.avsc", TEST_OUTPUT_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_OPTIONAL_GETTERS_FOR_ALL_FIELDS, TEST_OUTPUT_OPTIONAL_GETTERS_ALL_FIELDS);
  }

  @Test
  public void testCompileSchemaWithAddExtraOptionalGetters() throws Exception {

    TEST_OUTPUT_ADD_EXTRA_OPTIONAL_GETTERS.delete();
    doCompile(new String[] { "-encoding", "UTF-8", "-addExtraOptionalGetters", "schema",
        TEST_INPUT_DIR.toString() + "/addextraoptionalgetterstest.avsc", TEST_OUTPUT_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_ADD_EXTRA_OPTIONAL_GETTERS, TEST_OUTPUT_ADD_EXTRA_OPTIONAL_GETTERS);
  }

  @Test
  public void testCompileSchemaSingleFile() throws Exception {

    doCompile(new String[] { "-encoding", "UTF-8", "schema", TEST_INPUT_DIR.toString() + "/position.avsc",
        TEST_OUTPUT_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_POSITION, TEST_OUTPUT_POSITION);
  }

  @Test
  public void testCompileSchemaTwoFiles() throws Exception {

    doCompile(new String[] { "-encoding", "UTF-8", "schema", TEST_INPUT_DIR.toString() + "/position.avsc",
        TEST_INPUT_DIR.toString() + "/player.avsc", TEST_OUTPUT_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_POSITION, TEST_OUTPUT_POSITION);
    assertFileMatch(TEST_EXPECTED_PLAYER, TEST_OUTPUT_PLAYER);
  }

  @Test
  public void testCompileSchemaFileAndDirectory() throws Exception {

    doCompile(new String[] { "-encoding", "UTF-8", "schema", TEST_INPUT_DIR.toString() + "/position.avsc",
        TEST_INPUT_DIR.toString(), TEST_OUTPUT_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_POSITION, TEST_OUTPUT_POSITION);
    assertFileMatch(TEST_EXPECTED_PLAYER, TEST_OUTPUT_PLAYER);
  }

  @Test
  public void testCompileSchemasUsingString() throws Exception {

    doCompile(new String[] { "-encoding", "UTF-8", "-string", "schema", TEST_INPUT_DIR.toString() + "/position.avsc",
        TEST_INPUT_DIR.toString() + "/player.avsc", TEST_OUTPUT_STRING_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_STRING_POSITION, TEST_OUTPUT_STRING_POSITION);
    assertFileMatch(TEST_EXPECTED_STRING_PLAYER, TEST_OUTPUT_STRING_PLAYER);
  }

  @Test
  public void testCompileSchemasWithVariousFieldTypes() throws Exception {

    doCompile(new String[] { "-encoding", "UTF-8", "-string", "schema", TEST_INPUT_DIR.toString() + "/fieldtest.avsc",
        TEST_INPUT_DIR.toString() + "/fieldtest.avsc", TEST_OUTPUT_STRING_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_STRING_FIELDTEST, TEST_OUTPUT_STRING_FIELDTEST);
  }

  @Test
  public void testOrderingOfFlags() throws Exception {

    // Order of Flags as per initial implementation
    doCompile(new String[] { "-encoding", "UTF-8", "-string", "-bigDecimal", "schema",
        TEST_INPUT_DIR.toString() + "/fieldtest.avsc", TEST_INPUT_DIR.toString() + "/fieldtest.avsc",
        TEST_OUTPUT_STRING_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_STRING_FIELDTEST, TEST_OUTPUT_STRING_FIELDTEST);

    // Change order of encoding and string
    doCompile(new String[] { "-string", "-encoding", "UTF-8", "-bigDecimal", "schema",
        TEST_INPUT_DIR.toString() + "/fieldtest.avsc", TEST_INPUT_DIR.toString() + "/fieldtest.avsc",
        TEST_OUTPUT_STRING_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_STRING_FIELDTEST, TEST_OUTPUT_STRING_FIELDTEST);

    // Change order of -string and -bigDecimal
    doCompile(new String[] { "-bigDecimal", "-encoding", "UTF-8", "-string", "schema",
        TEST_INPUT_DIR.toString() + "/fieldtest.avsc", TEST_INPUT_DIR.toString() + "/fieldtest.avsc",
        TEST_OUTPUT_STRING_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_STRING_FIELDTEST, TEST_OUTPUT_STRING_FIELDTEST);

    // Keep encoding at the end
    doCompile(new String[] { "-bigDecimal", "-string", "-encoding", "UTF-8", "schema",
        TEST_INPUT_DIR.toString() + "/fieldtest.avsc", TEST_INPUT_DIR.toString() + "/fieldtest.avsc",
        TEST_OUTPUT_STRING_DIR.getPath() });
    assertFileMatch(TEST_EXPECTED_STRING_FIELDTEST, TEST_OUTPUT_STRING_FIELDTEST);
  }

  @Test
  public void testCompileProtocol() throws Exception {

    doCompile(new String[] { "-encoding", "UTF-8", "protocol", TEST_INPUT_DIR.toString() + "/proto.avpr",
        TEST_OUTPUT_STRING_DIR.getPath() });

    assertFileMatch(TEST_EXPECTED_STRING_PROTO, TEST_OUTPUT_STRING_PROTO);
  }

  // Runs the actual compiler tool with the given input args
  private void doCompile(String[] args) throws Exception {
    SpecificCompilerTool tool = new SpecificCompilerTool();
    tool.run(null, null, null, Arrays.asList((args)));
  }

  /**
   * Verify that the generated Java files match the expected. This approach has
   * room for improvement, since we're currently just verify that the text
   * matches, which can be brittle if the code generation formatting or method
   * ordering changes for example. A better approach would be to compile the
   * sources and do a deeper comparison.
   *
   * See
   * https://download.oracle.com/javase/6/docs/api/javax/tools/JavaCompiler.html
   */
  private static void assertFileMatch(File expected, File found) throws IOException {
    Assert.assertEquals("Found file: " + found + " does not match expected file: " + expected, readFile(expected),
        readFile(found));
  }

  /**
   * Not the best implementation, but does the job. Building full strings of the
   * file content and comparing provides nice diffs via JUnit when failures occur.
   */
  private static String readFile(File file) throws IOException {
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
    StringBuilder sb = new StringBuilder();
    String line = null;
    boolean first = true;
    while ((line = reader.readLine()) != null) {
      if (!first) {
        sb.append("\n");
        first = false;
      }
      sb.append(line);
    }
    reader.close();
    return sb.toString();
  }
}
