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

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.maven.plugin.testing.AbstractMojoTestCase;

/**
 * Base class for all Avro mojo test classes.
 */
public abstract class AbstractAvroMojoTest extends AbstractMojoTestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Assert the existence files in the given given directory.
   *
   * @param directory     the directory being checked
   * @param expectedFiles the files whose existence is being checked.
   */
  void assertFilesExist(File directory, Set<String> expectedFiles) {
    assertNotNull(directory);
    assertTrue("Directory " + directory.toString() + " does not exists", directory.exists());
    assertNotNull(expectedFiles);
    assertTrue(expectedFiles.size() > 0);

    final Set<String> filesInDirectory = new HashSet<>(Arrays.asList(directory.list()));

    assertEquals(expectedFiles, filesInDirectory);

  }
}
