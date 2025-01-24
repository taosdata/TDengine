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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestKeyValueWordCount {
  @Rule
  public TemporaryFolder mTempDir = new TemporaryFolder();

  public static class LineCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable mOne;

    @Override
    protected void setup(Context context) {
      mOne = new IntWritable(1);
    }

    @Override
    protected void map(LongWritable fileByteOffset, Text line, Context context)
        throws IOException, InterruptedException {
      context.write(line, mOne);
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text word, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(word, new IntWritable(sum));
    }
  }

  @Test
  public void testKeyValueMapReduce()
      throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
    // Configure a word count job over our test input file.
    Job job = Job.getInstance();
    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.txt").toURI().toString()));
    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
    Path outputPath = new Path(mTempDir.getRoot().getPath() + "/out-wordcount");
    FileOutputFormat.setOutputPath(job, outputPath);

    // Run the job.
    assertTrue(job.waitForCompletion(true));

    // Verify that the Avro container file generated had the right KeyValuePair
    // generic records.
    File avroFile = new File(outputPath.toString(), "part-r-00000.avro");
    DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(
        AvroKeyValue.getSchema(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT)));
    DataFileReader<GenericRecord> avroFileReader = new DataFileReader<>(avroFile, datumReader);

    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, Integer> appleRecord = new AvroKeyValue<>(avroFileReader.next());
    assertNotNull(appleRecord.get());
    assertEquals("apple", appleRecord.getKey().toString());
    assertEquals(3, appleRecord.getValue().intValue());

    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, Integer> bananaRecord = new AvroKeyValue<>(avroFileReader.next());
    assertNotNull(bananaRecord.get());
    assertEquals("banana", bananaRecord.getKey().toString());
    assertEquals(2, bananaRecord.getValue().intValue());

    assertTrue(avroFileReader.hasNext());
    AvroKeyValue<CharSequence, Integer> carrotRecord = new AvroKeyValue<>(avroFileReader.next());
    assertEquals("carrot", carrotRecord.getKey().toString());
    assertEquals(1, carrotRecord.getValue().intValue());

    assertFalse(avroFileReader.hasNext());
    avroFileReader.close();
  }
}
