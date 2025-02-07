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

package org.apache.avro.codegentest;

import org.apache.avro.codegentest.testdata.StringLogicalType;
import org.apache.avro.generic.GenericData;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestLogicalTypeForStringType {

  /**
   * See AVRO-2548: StringType of "String" causes logicalType converters to be
   * ignored for field
   */
  @Test
  public void shouldUseUUIDAsType() {
    StringLogicalType stringLogicalType = new StringLogicalType();
    stringLogicalType.setSomeIdentifier(UUID.randomUUID());
    assertThat(stringLogicalType.getSomeIdentifier(), instanceOf(UUID.class));
    assertThat(StringLogicalType.getClassSchema().getField("someJavaString").schema().getProp(GenericData.STRING_PROP),
        equalTo("String"));
  }

}
