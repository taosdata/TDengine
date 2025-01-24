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

package org.apache.avro.generic;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;

import org.junit.Test;

import test.StringablesRecord;

import static org.junit.Assert.assertEquals;

/** Unit test for performing a builder copy of an object with a schema */
public class TestBuilderCopy {
  @Test
  public void testBuilderCopy() {
    StringablesRecord.Builder builder = StringablesRecord.newBuilder();
    builder.setValue(new BigDecimal("1314.11"));

    HashMap<String, BigDecimal> mapWithBigDecimalElements = new HashMap<>();
    mapWithBigDecimalElements.put("testElement", new BigDecimal("220.11"));
    builder.setMapWithBigDecimalElements(mapWithBigDecimalElements);

    HashMap<BigInteger, String> mapWithBigIntKeys = new HashMap<>();
    mapWithBigIntKeys.put(BigInteger.ONE, "testKey");
    builder.setMapWithBigIntKeys(mapWithBigIntKeys);

    StringablesRecord original = builder.build();

    StringablesRecord duplicate = StringablesRecord.newBuilder(original).build();

    assertEquals(duplicate, original);
  }
}
