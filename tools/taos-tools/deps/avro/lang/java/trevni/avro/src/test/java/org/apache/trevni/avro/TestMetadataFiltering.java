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
package org.apache.trevni.avro;

import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.mapred.JobConf;
import org.apache.trevni.ColumnFileMetaData;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestMetadataFiltering {

  @Test
  public void testMetadataFiltering() throws Exception {
    JobConf job = new JobConf();

    job.set(AvroTrevniOutputFormat.META_PREFIX + "test1", "1");
    job.set(AvroTrevniOutputFormat.META_PREFIX + "test2", "2");
    job.set("test3", "3");
    job.set(AvroJob.TEXT_PREFIX + "test4", "4");
    job.set(AvroTrevniOutputFormat.META_PREFIX + "test5", "5");

    ColumnFileMetaData metadata = AvroTrevniOutputFormat.filterMetadata(job);

    assertTrue(metadata.get("test1") != null);
    assertTrue(new String(metadata.get("test1")).equals("1"));
    assertTrue(metadata.get("test2") != null);
    assertTrue(new String(metadata.get("test2")).equals("2"));
    assertTrue(metadata.get("test5") != null);
    assertTrue(new String(metadata.get("test5")).equals("5"));
    assertTrue(metadata.get("test3") == null);
    assertTrue(metadata.get("test4") == null);

  }

}
