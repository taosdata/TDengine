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

import org.apache.avro.codegentest.testdata.NestedLogicalTypesArray;
import org.apache.avro.codegentest.testdata.NestedLogicalTypesMap;
import org.apache.avro.codegentest.testdata.NestedLogicalTypesRecord;
import org.apache.avro.codegentest.testdata.NestedLogicalTypesUnion;
import org.apache.avro.codegentest.testdata.NestedLogicalTypesUnionFixedDecimal;
import org.apache.avro.codegentest.testdata.NestedRecord;
import org.apache.avro.codegentest.testdata.NullableLogicalTypesArray;
import org.apache.avro.codegentest.testdata.RecordInArray;
import org.apache.avro.codegentest.testdata.RecordInMap;
import org.apache.avro.codegentest.testdata.RecordInUnion;
import org.junit.Test;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Collections;

public class TestNestedLogicalTypes extends AbstractSpecificRecordTest {

  @Test
  public void testNullableLogicalTypeInNestedRecord() {
    final NestedLogicalTypesRecord nestedLogicalTypesRecord = NestedLogicalTypesRecord.newBuilder()
        .setNestedRecord(NestedRecord.newBuilder().setNullableDateField(LocalDate.now()).build()).build();
    verifySerDeAndStandardMethods(nestedLogicalTypesRecord);
  }

  @Test
  public void testNullableLogicalTypeInArray() {
    final NullableLogicalTypesArray logicalTypesArray = NullableLogicalTypesArray.newBuilder()
        .setArrayOfLogicalType(Collections.singletonList(LocalDate.now())).build();
    verifySerDeAndStandardMethods(logicalTypesArray);
  }

  @Test
  public void testNullableLogicalTypeInRecordInArray() {
    final NestedLogicalTypesArray nestedLogicalTypesArray = NestedLogicalTypesArray.newBuilder()
        .setArrayOfRecords(
            Collections.singletonList(RecordInArray.newBuilder().setNullableDateField(LocalDate.now()).build()))
        .build();
    verifySerDeAndStandardMethods(nestedLogicalTypesArray);
  }

  @Test
  public void testNullableLogicalTypeInRecordInUnion() {
    final NestedLogicalTypesUnion nestedLogicalTypesUnion = NestedLogicalTypesUnion.newBuilder()
        .setUnionOfRecords(RecordInUnion.newBuilder().setNullableDateField(LocalDate.now()).build()).build();
    verifySerDeAndStandardMethods(nestedLogicalTypesUnion);
  }

  @Test
  public void testNullableLogicalTypeInRecordInMap() {
    final NestedLogicalTypesMap nestedLogicalTypesMap = NestedLogicalTypesMap.newBuilder()
        .setMapOfRecords(
            Collections.singletonMap("key", RecordInMap.newBuilder().setNullableDateField(LocalDate.now()).build()))
        .build();
    verifySerDeAndStandardMethods(nestedLogicalTypesMap);
  }

  @Test
  public void testNullableLogicalTypeInRecordInFixedDecimal() {
    final NestedLogicalTypesUnionFixedDecimal nestedLogicalTypesUnionFixedDecimal = NestedLogicalTypesUnionFixedDecimal
        .newBuilder().setUnionOfFixedDecimal(new CustomDecimal(BigInteger.TEN, 15)).build();
    verifySerDeAndStandardMethods(nestedLogicalTypesUnionFixedDecimal);
  }

}
