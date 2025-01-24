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

import static org.junit.Assert.fail;

import org.junit.Test;

public class TestMain {
  /** Make sure that tool descriptions fit in 80 characters. */
  @Test
  public void testToolDescriptionLength() {
    Main m = new Main();
    for (Tool t : m.tools.values()) {
      // System.out.println(t.getName() + ": " + t.getShortDescription().length());
      if (m.maxLen + 2 + t.getShortDescription().length() > 80) {
        fail("Tool description too long: " + t.getName());
      }
    }
  }

  /**
   * Make sure that the tool name is not too long, otherwise space for description
   * is too short because they are rebalanced in the CLI.
   */
  @Test
  public void testToolNameLength() {
    // 13 chosen for backwards compatibility
    final int MAX_NAME_LENGTH = 13;

    Main m = new Main();
    for (Tool t : m.tools.values()) {
      if (t.getName().length() > MAX_NAME_LENGTH) {
        fail("Tool name too long (" + t.getName().length() + "): " + t.getName() + ". Max length is: "
            + MAX_NAME_LENGTH);
      }
    }
  }
}
