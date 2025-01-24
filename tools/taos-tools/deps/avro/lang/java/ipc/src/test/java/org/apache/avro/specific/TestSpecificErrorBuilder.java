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
package org.apache.avro.specific;

import org.apache.avro.test.errors.TestError;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the SpecificErrorBuilderBase class.
 */
public class TestSpecificErrorBuilder {
  @Test
  public void testSpecificErrorBuilder() {
    TestError.Builder testErrorBuilder = TestError.newBuilder().setValue("value").setCause(new NullPointerException())
        .setMessage$("message$");

    // Test has methods
    Assert.assertTrue(testErrorBuilder.hasValue());
    Assert.assertNotNull(testErrorBuilder.getValue());
    Assert.assertTrue(testErrorBuilder.hasCause());
    Assert.assertNotNull(testErrorBuilder.getCause());
    Assert.assertTrue(testErrorBuilder.hasMessage$());
    Assert.assertNotNull(testErrorBuilder.getMessage$());

    TestError testError = testErrorBuilder.build();
    Assert.assertEquals("value", testError.getValue());
    Assert.assertEquals("value", testError.getMessage());
    Assert.assertEquals("message$", testError.getMessage$());

    // Test copy constructor
    Assert.assertEquals(testErrorBuilder, TestError.newBuilder(testErrorBuilder));
    Assert.assertEquals(testErrorBuilder, TestError.newBuilder(testError));

    TestError error = new TestError("value", new NullPointerException());
    error.setMessage$("message");
    Assert.assertEquals(error,
        TestError.newBuilder().setValue("value").setCause(new NullPointerException()).setMessage$("message").build());

    // Test clear
    testErrorBuilder.clearValue();
    Assert.assertFalse(testErrorBuilder.hasValue());
    Assert.assertNull(testErrorBuilder.getValue());
    testErrorBuilder.clearCause();
    Assert.assertFalse(testErrorBuilder.hasCause());
    Assert.assertNull(testErrorBuilder.getCause());
    testErrorBuilder.clearMessage$();
    Assert.assertFalse(testErrorBuilder.hasMessage$());
    Assert.assertNull(testErrorBuilder.getMessage$());
  }

  @Test(expected = org.apache.avro.AvroRuntimeException.class)
  public void attemptToSetNonNullableFieldToNull() {
    TestError.newBuilder().setMessage$(null);
  }
}
