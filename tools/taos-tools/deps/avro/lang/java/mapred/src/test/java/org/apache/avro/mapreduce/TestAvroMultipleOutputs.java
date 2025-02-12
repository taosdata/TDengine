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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroMultipleOutputs {
  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  public static final Schema STATS_SCHEMA = new Schema.Parser().parse("{\"name\":\"stats\",\"type\":\"record\","
      + "\"fields\":[{\"name\":\"count\",\"type\":\"int\"}," + "{\"name\":\"name\",\"type\":\"string\"}]}");
  public static final Schema STATS_SCHEMA_2 = new Schema.Parser().parse("{\"name\":\"stats\",\"type\":\"record\","
      + "\"fields\":[{\"name\":\"count1\",\"type\":\"int\"}," + "{\"name\":\"name1\",\"type\":\"string\"}]}");

  private static class LineCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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

  private static class StatCountMapper extends Mapper<AvroKey<TextStats>, NullWritable, Text, IntWritable> {
    private IntWritable mCount;
    private Text mText;

    @Override
    protected void setup(Context context) {
      mCount = new IntWritable(0);
      mText = new Text("");
    }

    @Override
    protected void map(AvroKey<TextStats> record, NullWritable ignore, Context context)
        throws IOException, InterruptedException {
      mCount.set(record.datum().getCount());
      mText.set(record.datum().getName().toString());
      context.write(mText, mCount);
    }
  }

  private static class GenericStatsReducer
      extends Reducer<Text, IntWritable, AvroKey<GenericData.Record>, NullWritable> {
    private AvroKey<GenericData.Record> mStats;
    private AvroMultipleOutputs amos;

    @Override
    protected void setup(Context context) {
      mStats = new AvroKey<>(null);
      amos = new AvroMultipleOutputs(context);
    }

    @Override
    protected void reduce(Text line, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      GenericData.Record record = new GenericData.Record(STATS_SCHEMA);
      GenericData.Record record2 = new GenericData.Record(STATS_SCHEMA_2);
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      record.put("name", new Utf8(line.toString()));
      record.put("count", sum);
      mStats.datum(record);
      context.write(mStats, NullWritable.get());
      amos.write("myavro", mStats, NullWritable.get());
      record2.put("name1", new Utf8(line.toString()));
      record2.put("count1", sum);
      mStats.datum(record2);
      amos.write(mStats, NullWritable.get(), STATS_SCHEMA_2, null, "testnewwrite2");
      amos.write("myavro1", mStats);
      amos.write(mStats, NullWritable.get(), STATS_SCHEMA, null, "testnewwrite");
      amos.write(mStats, NullWritable.get(), "testwritenonschema");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      amos.close();
    }
  }

  private static class SpecificStatsReducer extends Reducer<Text, IntWritable, AvroKey<TextStats>, NullWritable> {
    private AvroKey<TextStats> mStats;
    private AvroMultipleOutputs amos;

    @Override
    protected void setup(Context context) {
      mStats = new AvroKey<>(null);
      amos = new AvroMultipleOutputs(context);
    }

    @Override
    protected void reduce(Text line, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      TextStats record = new TextStats();
      record.setCount(0);
      for (IntWritable count : counts) {
        record.setCount(record.getCount() + count.get());
      }
      record.setName(line.toString());
      mStats.datum(record);
      context.write(mStats, NullWritable.get());
      amos.write("myavro3", mStats, NullWritable.get());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      amos.close();
    }
  }

  private static class SortMapper extends Mapper<AvroKey<TextStats>, NullWritable, AvroKey<TextStats>, NullWritable> {
    @Override
    protected void map(AvroKey<TextStats> key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(key, value);
    }
  }

  private static class SortReducer extends Reducer<AvroKey<TextStats>, NullWritable, AvroKey<TextStats>, NullWritable> {
    @Override
    protected void reduce(AvroKey<TextStats> key, Iterable<NullWritable> ignore, Context context)
        throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  @Test
  public void testAvroGenericOutput() throws Exception {
    Job job = Job.getInstance();

    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.txt").toURI().toString()));
    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(GenericStatsReducer.class);
    AvroJob.setOutputKeySchema(job, STATS_SCHEMA);
    AvroMultipleOutputs.addNamedOutput(job, "myavro", AvroKeyOutputFormat.class, STATS_SCHEMA, null);
    AvroMultipleOutputs.addNamedOutput(job, "myavro1", AvroKeyOutputFormat.class, STATS_SCHEMA_2);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    Path outputPath = new Path(DIR.getRoot().getPath() + "/testAvroGenericOutput");
    outputPath.getFileSystem(job.getConfiguration()).delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/myavro-r-00000.avro"));
    Assert.assertEquals(1, outputFiles.length);

    Map<String, Integer> counts = new HashMap<>();

    try (DataFileReader<GenericData.Record> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new GenericDatumReader<>(STATS_SCHEMA))) {
      for (GenericData.Record record : reader) {
        counts.put(((Utf8) record.get("name")).toString(), (Integer) record.get("count"));
      }
    }

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());

    outputFiles = fileSystem.globStatus(outputPath.suffix("/myavro1-r-00000.avro"));
    Assert.assertEquals(1, outputFiles.length);
    counts.clear();
    try (DataFileReader<GenericData.Record> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new GenericDatumReader<>(STATS_SCHEMA_2))) {
      for (GenericData.Record record : reader) {
        counts.put(((Utf8) record.get("name1")).toString(), (Integer) record.get("count1"));
      }
    }

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());

    outputFiles = fileSystem.globStatus(outputPath.suffix("/testnewwrite-r-00000.avro"));
    Assert.assertEquals(1, outputFiles.length);
    counts.clear();
    try (DataFileReader<GenericData.Record> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new GenericDatumReader<>(STATS_SCHEMA))) {
      for (GenericData.Record record : reader) {
        counts.put(((Utf8) record.get("name")).toString(), (Integer) record.get("count"));
      }
    }

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());

    outputFiles = fileSystem.globStatus(outputPath.suffix("/testnewwrite2-r-00000.avro"));
    Assert.assertEquals(1, outputFiles.length);
    counts.clear();
    try (DataFileReader<GenericData.Record> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new GenericDatumReader<>(STATS_SCHEMA_2))) {
      for (GenericData.Record record : reader) {
        counts.put(((Utf8) record.get("name1")).toString(), (Integer) record.get("count1"));
      }
    }
    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());

    outputFiles = fileSystem.globStatus(outputPath.suffix("/testwritenonschema-r-00000.avro"));
    Assert.assertEquals(1, outputFiles.length);
    counts.clear();
    try (DataFileReader<GenericData.Record> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new GenericDatumReader<>(STATS_SCHEMA))) {
      for (GenericData.Record record : reader) {
        counts.put(((Utf8) record.get("name")).toString(), (Integer) record.get("count"));
      }
    }

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }

  @Test
  public void testAvroSpecificOutput() throws Exception {
    Job job = Job.getInstance();

    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.txt").toURI().toString()));
    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    AvroMultipleOutputs.addNamedOutput(job, "myavro3", AvroKeyOutputFormat.class, TextStats.SCHEMA$, null);

    job.setReducerClass(SpecificStatsReducer.class);
    AvroJob.setOutputKeySchema(job, TextStats.SCHEMA$);

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(DIR.getRoot().getPath() + "/testAvroSpecificOutput");
    outputPath.getFileSystem(job.getConfiguration()).delete(outputPath, true);
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/myavro3-*"));
    Assert.assertEquals(1, outputFiles.length);
    Map<String, Integer> counts = new HashMap<>();
    try (DataFileReader<TextStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new SpecificDatumReader<>())) {
      for (TextStats record : reader) {
        counts.put(record.getName().toString(), record.getCount());
      }
    }

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }

  @Test
  public void testAvroInput() throws Exception {
    Job job = Job.getInstance();

    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.avro").toURI().toString()));
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setInputKeySchema(job, TextStats.SCHEMA$);
    AvroMultipleOutputs.addNamedOutput(job, "myavro3", AvroKeyOutputFormat.class, TextStats.SCHEMA$, null);

    job.setMapperClass(StatCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(SpecificStatsReducer.class);
    AvroJob.setOutputKeySchema(job, TextStats.SCHEMA$);

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(DIR.getRoot().getPath() + "/testAvroInput");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/myavro3-*"));
    Assert.assertEquals(1, outputFiles.length);
    Map<String, Integer> counts = new HashMap<>();
    try (DataFileReader<TextStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new SpecificDatumReader<>())) {
      for (TextStats record : reader) {
        counts.put(record.getName().toString(), record.getCount());
      }
    }

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }

  @Test
  public void testAvroMapOutput() throws Exception {
    Job job = Job.getInstance();

    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.avro").toURI().toString()));
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setInputKeySchema(job, TextStats.SCHEMA$);

    job.setMapperClass(SortMapper.class);
    AvroJob.setMapOutputKeySchema(job, TextStats.SCHEMA$);
    job.setMapOutputValueClass(NullWritable.class);

    job.setReducerClass(SortReducer.class);
    AvroJob.setOutputKeySchema(job, TextStats.SCHEMA$);

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(DIR.getRoot().getPath() + "/testAvroMapOutput");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    Map<String, Integer> counts = new HashMap<>();
    try (DataFileReader<TextStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new SpecificDatumReader<>())) {
      for (TextStats record : reader) {
        counts.put(record.getName().toString(), record.getCount());
      }
    }

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }
}
