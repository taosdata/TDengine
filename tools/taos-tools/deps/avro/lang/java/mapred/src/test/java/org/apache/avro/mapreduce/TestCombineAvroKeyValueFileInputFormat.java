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

package org.apache.avro.mapreduce;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestCombineAvroKeyValueFileInputFormat {

  /** A temporary directory for test data. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  /**
   * Verifies that avro records can be read in multi files.
   */
  @Test
  public void testReadRecords() throws IOException, InterruptedException, ClassNotFoundException {

    Schema keyValueSchema = AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));

    AvroKeyValue<Integer, CharSequence> record1 = new AvroKeyValue<>(new GenericData.Record(keyValueSchema));
    record1.setKey(1);
    record1.setValue("apple banana carrot");
    AvroFiles.createFile(new File(mTempDir.getRoot(), "combineSplit00.avro"), keyValueSchema, record1.get());

    AvroKeyValue<Integer, CharSequence> record2 = new AvroKeyValue<>(new GenericData.Record(keyValueSchema));
    record2.setKey(2);
    record2.setValue("apple banana");

    AvroFiles.createFile(new File(mTempDir.getRoot(), "combineSplit01.avro"), keyValueSchema, record2.get());

    // Configure the job input.
    Job job = Job.getInstance();
    FileInputFormat.setInputPaths(job, new Path(mTempDir.getRoot().getAbsolutePath()));
    job.setInputFormatClass(CombineAvroKeyValueFileInputFormat.class);
    AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.INT));
    AvroJob.setInputValueSchema(job, Schema.create(Schema.Type.STRING));

    // Configure the identity mapper.
    AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.INT));
    AvroJob.setMapOutputValueSchema(job, Schema.create(Schema.Type.STRING));

    // Configure zero reducers.
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(AvroValue.class);

    // Configure the output format.
    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    Path outputPath = new Path(mTempDir.getRoot().getPath(), "out");
    FileOutputFormat.setOutputPath(job, outputPath);

    // Run the job.
    assertTrue(job.waitForCompletion(true));

    // Verify that the output Avro container file has the expected data.
    File avroFile = new File(outputPath.toString(), "part-m-00000.avro");
    DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(
        AvroKeyValue.getSchema(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING)));
    DataFileReader<GenericRecord> avroFileReader = new DataFileReader<>(avroFile, datumReader);
    assertTrue(avroFileReader.hasNext());

    while (avroFileReader.hasNext()) {
      AvroKeyValue<Integer, CharSequence> mapRecord1 = new AvroKeyValue<>(avroFileReader.next());
      assertNotNull(mapRecord1.get());
      if (mapRecord1.getKey().intValue() == 1) {
        assertEquals("apple banana carrot", mapRecord1.getValue().toString());
      } else if (mapRecord1.getKey().intValue() == 2) {
        assertEquals("apple banana", mapRecord1.getValue().toString());
      } else {
        fail("Unknown key " + mapRecord1.getKey().intValue());
      }
    }
    avroFileReader.close();
  }
}
