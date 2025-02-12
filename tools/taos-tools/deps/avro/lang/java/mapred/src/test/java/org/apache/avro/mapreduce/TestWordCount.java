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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestWordCount {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  public static final Schema STATS_SCHEMA = new Schema.Parser().parse("{\"name\":\"stats\",\"type\":\"record\","
      + "\"fields\":[{\"name\":\"count\",\"type\":\"int\"}," + "{\"name\":\"name\",\"type\":\"string\"}]}");

  public static class ReflectStats {
    String name;
    int count;
  }

  // permit data written as SpecficStats to be read as ReflectStats
  private static Schema REFLECT_STATS_SCHEMA = ReflectData.get().getSchema(ReflectStats.class);
  static {
    REFLECT_STATS_SCHEMA.addAlias(TextStats.SCHEMA$.getFullName());
  }

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

  private static class ReflectCountMapper extends Mapper<AvroKey<ReflectStats>, NullWritable, Text, IntWritable> {
    private IntWritable mCount;
    private Text mText;

    @Override
    protected void setup(Context context) {
      mCount = new IntWritable(0);
      mText = new Text("");
    }

    @Override
    protected void map(AvroKey<ReflectStats> record, NullWritable ignore, Context context)
        throws IOException, InterruptedException {
      mCount.set(record.datum().count);
      mText.set(record.datum().name);
      context.write(mText, mCount);
    }
  }

  private static class AvroSumReducer extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      context.write(new AvroKey<>(key.toString()), new AvroValue<>(sum));
    }
  }

  private static class GenericStatsReducer
      extends Reducer<Text, IntWritable, AvroKey<GenericData.Record>, NullWritable> {
    private AvroKey<GenericData.Record> mStats;

    @Override
    protected void setup(Context context) {
      mStats = new AvroKey<>(null);
    }

    @Override
    protected void reduce(Text line, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      GenericData.Record record = new GenericData.Record(STATS_SCHEMA);
      int sum = 0;
      for (IntWritable count : counts) {
        sum += count.get();
      }
      record.put("name", new Utf8(line.toString()));
      record.put("count", sum);
      mStats.datum(record);
      context.write(mStats, NullWritable.get());
    }
  }

  private static class SpecificStatsReducer extends Reducer<Text, IntWritable, AvroKey<TextStats>, NullWritable> {
    private AvroKey<TextStats> mStats;

    @Override
    protected void setup(Context context) {
      mStats = new AvroKey<>(null);
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
    }
  }

  private static class ReflectStatsReducer extends Reducer<Text, IntWritable, AvroKey<ReflectStats>, NullWritable> {
    private AvroKey<ReflectStats> mStats;

    @Override
    protected void setup(Context context) {
      mStats = new AvroKey<>(null);
    }

    @Override
    protected void reduce(Text line, Iterable<IntWritable> counts, Context context)
        throws IOException, InterruptedException {
      ReflectStats record = new ReflectStats();
      record.count = 0;
      for (IntWritable count : counts) {
        record.count += count.get();
      }
      record.name = line.toString();
      mStats.datum(record);
      context.write(mStats, NullWritable.get());
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

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-generic");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    DataFileReader<GenericData.Record> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new GenericDatumReader<>(STATS_SCHEMA));
    Map<String, Integer> counts = new HashMap<>();
    for (GenericData.Record record : reader) {
      counts.put(((Utf8) record.get("name")).toString(), (Integer) record.get("count"));
    }
    reader.close();

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

    job.setReducerClass(SpecificStatsReducer.class);
    AvroJob.setOutputKeySchema(job, TextStats.SCHEMA$);

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-specific");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    DataFileReader<TextStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new SpecificDatumReader<>());
    Map<String, Integer> counts = new HashMap<>();
    for (TextStats record : reader) {
      counts.put(record.getName().toString(), record.getCount());
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }

  @Test
  public void testAvroReflectOutput() throws Exception {
    Job job = Job.getInstance();

    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.txt").toURI().toString()));
    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(ReflectStatsReducer.class);
    AvroJob.setOutputKeySchema(job, REFLECT_STATS_SCHEMA);

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-reflect");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    DataFileReader<ReflectStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new ReflectDatumReader<>());
    Map<String, Integer> counts = new HashMap<>();
    for (ReflectStats record : reader) {
      counts.put(record.name, record.count);
    }
    reader.close();

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

    job.setMapperClass(StatCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(SpecificStatsReducer.class);
    AvroJob.setOutputKeySchema(job, TextStats.SCHEMA$);

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-specific-input");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    DataFileReader<TextStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new SpecificDatumReader<>());
    Map<String, Integer> counts = new HashMap<>();
    for (TextStats record : reader) {
      counts.put(record.getName().toString(), record.getCount());
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }

  @Test
  public void testReflectInput() throws Exception {
    Job job = Job.getInstance();
    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.avro").toURI().toString()));
    job.setInputFormatClass(AvroKeyInputFormat.class);
    AvroJob.setInputKeySchema(job, REFLECT_STATS_SCHEMA);

    job.setMapperClass(ReflectCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(ReflectStatsReducer.class);
    AvroJob.setOutputKeySchema(job, REFLECT_STATS_SCHEMA);

    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-reflect-input");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    DataFileReader<ReflectStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new ReflectDatumReader<>());
    Map<String, Integer> counts = new HashMap<>();
    for (ReflectStats record : reader) {
      counts.put(record.name, record.count);
    }
    reader.close();

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
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-specific-input");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    DataFileReader<TextStats> reader = new DataFileReader<>(
        new FsInput(outputFiles[0].getPath(), job.getConfiguration()), new SpecificDatumReader<>());
    Map<String, Integer> counts = new HashMap<>();
    for (TextStats record : reader) {
      counts.put(record.getName().toString(), record.getCount());
    }
    reader.close();

    Assert.assertEquals(3, counts.get("apple").intValue());
    Assert.assertEquals(2, counts.get("banana").intValue());
    Assert.assertEquals(1, counts.get("carrot").intValue());
  }

  /**
   * Tests the MR output to text files when using AvroKey and AvroValue records.
   */
  @Test
  public void testAvroUsingTextFileOutput() throws Exception {
    Job job = Job.getInstance();

    FileInputFormat.setInputPaths(job,
        new Path(getClass().getResource("/org/apache/avro/mapreduce/mapreduce-test-input.txt").toURI().toString()));
    job.setInputFormatClass(TextInputFormat.class);

    job.setMapperClass(LineCountMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(AvroSumReducer.class);
    AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

    job.setOutputFormatClass(TextOutputFormat.class);
    Path outputPath = new Path(tmpFolder.getRoot().getPath() + "/out-text");
    FileOutputFormat.setOutputPath(job, outputPath);

    Assert.assertTrue(job.waitForCompletion(true));

    // Check that the results from the MapReduce were as expected.
    FileSystem fileSystem = FileSystem.get(job.getConfiguration());
    FileStatus[] outputFiles = fileSystem.globStatus(outputPath.suffix("/part-*"));
    Assert.assertEquals(1, outputFiles.length);
    Path filePath = outputFiles[0].getPath();
    InputStream inputStream = filePath.getFileSystem(job.getConfiguration()).open(filePath);
    Assert.assertNotNull(inputStream);
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      Assert.assertTrue(reader.ready());
      Assert.assertEquals("apple\t3", reader.readLine());
      Assert.assertEquals("banana\t2", reader.readLine());
      Assert.assertEquals("carrot\t1", reader.readLine());
      Assert.assertFalse(reader.ready());
    }
  }
}
