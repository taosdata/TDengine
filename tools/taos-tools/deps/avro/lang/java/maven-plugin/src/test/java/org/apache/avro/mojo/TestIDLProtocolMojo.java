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
package org.apache.avro.mojo;

import org.codehaus.plexus.util.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Test the IDL Protocol Mojo.
 */
public class TestIDLProtocolMojo extends AbstractAvroMojoTest {

  private File testPom = new File(getBasedir(), "src/test/resources/unit/idl/pom.xml");
  private File injectingVelocityToolsTestPom = new File(getBasedir(),
      "src/test/resources/unit/idl/pom-injecting-velocity-tools.xml");

  @Test
  public void testIdlProtocolMojo() throws Exception {
    final IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", testPom);

    assertNotNull(mojo);
    mojo.execute();

    final File outputDir = new File(getBasedir(), "target/test-harness/idl/test/");
    final Set<String> generatedFiles = new HashSet<>(Arrays.asList("IdlPrivacy.java", "IdlTest.java", "IdlUser.java",
        "IdlUserWrapper.java", "IdlClasspathImportTest.java"));
    assertFilesExist(outputDir, generatedFiles);

    final String idlUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(idlUserContent.contains("java.time.Instant"));
  }

  @Test
  public void testSetCompilerVelocityAdditionalTools() throws Exception {
    final IDLProtocolMojo mojo = (IDLProtocolMojo) lookupMojo("idl-protocol", injectingVelocityToolsTestPom);

    assertNotNull(mojo);
    mojo.execute();

    final File outputDir = new File(getBasedir(), "target/test-harness/idl-inject/test");
    final Set<String> generatedFiles = new HashSet<>(Arrays.asList("IdlPrivacy.java", "IdlTest.java", "IdlUser.java",
        "IdlUserWrapper.java", "IdlClasspathImportTest.java"));

    assertFilesExist(outputDir, generatedFiles);

    final String schemaUserContent = FileUtils.fileRead(new File(outputDir, "IdlUser.java"));
    assertTrue(schemaUserContent.contains("It works!"));
  }
}
