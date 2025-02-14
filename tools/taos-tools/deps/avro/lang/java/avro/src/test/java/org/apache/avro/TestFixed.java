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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro;

import org.junit.Assert;
import org.junit.Test;

public class TestFixed {

  @Test
  public void testFixedDefaultValueDrop() {
    Schema md5 = SchemaBuilder.builder().fixed("MD5").size(16);
    Schema frec = SchemaBuilder.builder().record("test").fields().name("hash").type(md5).withDefault(new byte[16])
        .endRecord();
    Schema.Field field = frec.getField("hash");
    Assert.assertNotNull(field.defaultVal());
    Assert.assertArrayEquals(new byte[16], (byte[]) field.defaultVal());
  }

}
