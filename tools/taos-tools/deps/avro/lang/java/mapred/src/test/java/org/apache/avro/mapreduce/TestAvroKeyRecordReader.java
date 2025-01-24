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

package org.apache.avro.mapreduce;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroKeyRecordReader {
  /** A temporary directory for test data. */
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  /**
   * Verifies that avro records can be read and progress is reported correctly.
   */
  @Test
  public void testReadRecords() throws IOException, InterruptedException {
    // Create the test avro file input with two records:
    // 1. "first"
    // 2. "second"
    final SeekableInput avroFileInput = new SeekableFileInput(AvroFiles.createFile(
        new File(mTempDir.getRoot(), "myStringfile.avro"), Schema.create(Schema.Type.STRING), "first", "second"));

    // Create the record reader.
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    RecordReader<AvroKey<CharSequence>, NullWritable> recordReader = new AvroKeyRecordReader<CharSequence>(
        readerSchema) {
      @Override
      protected SeekableInput createSeekableInput(Configuration conf, Path path) throws IOException {
        return avroFileInput;
      }
    };

    // Set up the job configuration.
    Configuration conf = new Configuration();

    // Create a mock input split for this record reader.
    FileSplit inputSplit = createMock(FileSplit.class);
    expect(inputSplit.getPath()).andReturn(new Path("/path/to/an/avro/file")).anyTimes();
    expect(inputSplit.getStart()).andReturn(0L).anyTimes();
    expect(inputSplit.getLength()).andReturn(avroFileInput.length()).anyTimes();

    // Create a mock task attempt context for this record reader.
    TaskAttemptContext context = createMock(TaskAttemptContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();

    // Initialize the record reader.
    replay(inputSplit);
    replay(context);
    recordReader.initialize(inputSplit, context);

    assertEquals("Progress should be zero before any records are read", 0.0f, recordReader.getProgress(), 0.0f);

    // Some variables to hold the records.
    AvroKey<CharSequence> key;
    NullWritable value;

    // Read the first record.
    assertTrue("Expected at least one record", recordReader.nextKeyValue());
    key = recordReader.getCurrentKey();
    value = recordReader.getCurrentValue();

    assertNotNull("First record had null key", key);
    assertNotNull("First record had null value", value);

    CharSequence firstString = key.datum();
    assertEquals("first", firstString.toString());

    assertTrue("getCurrentKey() returned different keys for the same record", key == recordReader.getCurrentKey());
    assertTrue("getCurrentValue() returned different values for the same record",
        value == recordReader.getCurrentValue());

    // Read the second record.
    assertTrue("Expected to read a second record", recordReader.nextKeyValue());
    key = recordReader.getCurrentKey();
    value = recordReader.getCurrentValue();

    assertNotNull("Second record had null key", key);
    assertNotNull("Second record had null value", value);

    CharSequence secondString = key.datum();
    assertEquals("second", secondString.toString());

    assertEquals("Progress should be complete (2 out of 2 records processed)", 1.0f, recordReader.getProgress(), 0.0f);

    // There should be no more records.
    assertFalse("Expected only 2 records", recordReader.nextKeyValue());

    // Close the record reader.
    recordReader.close();

    // Verify the expected calls on the mocks.
    verify(inputSplit);
    verify(context);
  }
}
