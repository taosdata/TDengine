/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.avro.message;

import org.apache.avro.Schema;
import org.apache.avro.compiler.schema.evolve.NestedEvolve1;
import org.apache.avro.compiler.schema.evolve.NestedEvolve2;
import org.apache.avro.compiler.schema.evolve.NestedEvolve3;
import org.apache.avro.compiler.schema.evolve.TestRecord2;
import org.apache.avro.compiler.schema.evolve.TestRecord3;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TestCustomSchemaStore {

  static class CustomSchemaStore implements SchemaStore {
    Cache cache;

    CustomSchemaStore() {
      cache = new Cache();
      cache.addSchema(NestedEvolve1.getClassSchema());
      cache.addSchema(NestedEvolve2.getClassSchema());
    }

    @Override
    public Schema findByFingerprint(long fingerprint) {
      return cache.findByFingerprint(fingerprint);
    }
  }

  private BinaryMessageDecoder<NestedEvolve1> decoder = NestedEvolve1.createDecoder(new CustomSchemaStore());

  @Test
  public void testCompatibleReadWithSchemaFromSchemaStore() throws Exception {
    // Create and encode a NestedEvolve2 record.
    NestedEvolve2.Builder rootBuilder = NestedEvolve2.newBuilder().setRootName("RootName");
    rootBuilder.setNested(TestRecord2.newBuilder().setName("Name").setValue(1).setData("Data").build());
    ByteBuffer nestedEvolve2Buffer = rootBuilder.build().toByteBuffer();

    // Decode it
    NestedEvolve1 nestedEvolve1 = decoder.decode(nestedEvolve2Buffer);

    // Should work
    assertEquals(nestedEvolve1.getRootName(), "RootName");
    assertEquals(nestedEvolve1.getNested().getName(), "Name");
    assertEquals(nestedEvolve1.getNested().getValue(), 1);
  }

  @Test(expected = MissingSchemaException.class)
  public void testIncompatibleReadWithSchemaFromSchemaStore() throws Exception {
    // Create and encode a NestedEvolve3 record.
    NestedEvolve3.Builder rootBuilder = NestedEvolve3.newBuilder().setRootName("RootName");
    rootBuilder.setNested(TestRecord3.newBuilder().setName("Name").setData("Data").build());
    ByteBuffer nestedEvolve3Buffer = rootBuilder.build().toByteBuffer();

    // Decode it ... should fail because schema for 'NestedEvolve3' is not available
    // in the SchemaStore
    decoder.decode(nestedEvolve3Buffer);
  }

}
