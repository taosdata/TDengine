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

package org.apache.avro.compiler.idl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Protocol;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple test harness for Idl. This relies on an input/ and output/ directory.
 * Inside the input/ directory are .avdl files. Each file should have a
 * corresponding .avpr file in output/. When the test is run, it generates and
 * stringifies each .avdl file and compares it to the expected output, failing
 * if the two differ.
 *
 * To make it simpler to write these tests, you can run ant -Dtestcase=TestIdl
 * -Dtest.idl.mode=write which will *replace* all expected output.
 */
public class TestIdl {
  private static final File TEST_DIR = new File(System.getProperty("test.idl.dir", "src/test/idl"));

  private static final File TEST_INPUT_DIR = new File(TEST_DIR, "input");

  private static final File TEST_OUTPUT_DIR = new File(TEST_DIR, "output");

  private static final String TEST_MODE = System.getProperty("test.idl.mode", "run");

  private List<GenTest> tests;

  @Before
  public void loadTests() {
    assertTrue(TEST_DIR.exists());
    assertTrue(TEST_INPUT_DIR.exists());
    assertTrue(TEST_OUTPUT_DIR.exists());

    tests = new ArrayList<>();
    for (File inF : TEST_INPUT_DIR.listFiles()) {
      if (!inF.getName().endsWith(".avdl"))
        continue;
      if (inF.getName().startsWith("."))
        continue;

      File outF = new File(TEST_OUTPUT_DIR, inF.getName().replaceFirst("\\.avdl$", ".avpr"));
      tests.add(new GenTest(inF, outF));
    }
  }

  @Test
  public void runTests() throws Exception {
    if (!"run".equals(TEST_MODE))
      return;

    int failed = 0;

    for (GenTest t : tests) {
      try {
        t.run();
      } catch (Exception e) {
        failed++;
        System.err.println("Failed: " + t.testName());
        e.printStackTrace(System.err);
      }
    }

    if (failed > 0) {
      fail(String.valueOf(failed) + " tests failed");
    }
  }

  @Test
  public void writeTests() throws Exception {
    if (!"write".equals(TEST_MODE))
      return;

    for (GenTest t : tests) {
      t.write();
    }
  }

  /**
   * An individual comparison test
   */
  private static class GenTest {
    private final File in, expectedOut;

    public GenTest(File in, File expectedOut) {
      this.in = in;
      this.expectedOut = expectedOut;
    }

    private String generate() throws Exception {
      ClassLoader cl = Thread.currentThread().getContextClassLoader();

      URL[] newPathURL = new URL[] { cl.getResource("putOnClassPath-test-resource.jar") };
      URLClassLoader ucl = new URLClassLoader(newPathURL, cl);

      Idl parser = new Idl(in, ucl);
      Protocol p = parser.CompilationUnit();
      parser.close();
      return p.toString();
    }

    public String testName() {
      return this.in.getName();
    }

    public void run() throws Exception {
      String output = generate();
      String slurped = slurp(expectedOut);
      assertEquals(slurped.trim(), output.replace("\\r", "").trim());
    }

    public void write() throws Exception {
      writeFile(expectedOut, generate());
    }

    private static String slurp(File f) throws IOException {
      BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(f), "UTF-8"));

      String line = null;
      StringBuilder builder = new StringBuilder();
      while ((line = in.readLine()) != null) {
        builder.append(line);
      }
      in.close();
      ObjectMapper mapper = new ObjectMapper();
      JsonNode json = mapper.readTree(builder.toString());
      return mapper.writer().writeValueAsString(json);
    }

    private static void writeFile(File f, String s) throws IOException {
      FileWriter w = new FileWriter(f);
      w.write(s);
      w.close();
    }
  }
}
