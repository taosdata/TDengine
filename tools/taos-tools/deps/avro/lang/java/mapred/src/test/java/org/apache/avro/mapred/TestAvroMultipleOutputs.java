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

package org.apache.avro.mapred;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroMultipleOutputs {

  @Rule
  public TemporaryFolder INPUT_DIR = new TemporaryFolder();

  @Rule
  public TemporaryFolder OUTPUT_DIR = new TemporaryFolder();

  public static class MapImpl extends AvroMapper<Utf8, Pair<Utf8, Long>> {
    private AvroMultipleOutputs amos;

    public void configure(JobConf Job) {
      this.amos = new AvroMultipleOutputs(Job);
    }

    @Override
    public void map(Utf8 text, AvroCollector<Pair<Utf8, Long>> collector, Reporter reporter) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens()) {
        String tok = tokens.nextToken();
        collector.collect(new Pair<>(new Utf8(tok), 1L));
        amos.getCollector("myavro2", reporter).collect(new Pair<Utf8, Long>(new Utf8(tok), 1L).toString());
      }
    }

    public void close() throws IOException {
      amos.close();
    }
  }

  public static class ReduceImpl extends AvroReducer<Utf8, Long, Pair<Utf8, Long>> {
    private AvroMultipleOutputs amos;

    public void configure(JobConf Job) {
      amos = new AvroMultipleOutputs(Job);
    }

    @Override
    public void reduce(Utf8 word, Iterable<Long> counts, AvroCollector<Pair<Utf8, Long>> collector, Reporter reporter)
        throws IOException {
      long sum = 0;
      for (long count : counts)
        sum += count;
      Pair<Utf8, Long> outputvalue = new Pair<>(word, sum);
      amos.getCollector("myavro", reporter).collect(outputvalue);
      amos.collect("myavro1", reporter, outputvalue.toString());
      amos.collect("myavro", reporter, new Pair<Utf8, Long>(new Utf8(""), 0L).getSchema(), outputvalue, "testavrofile");
      amos.collect("myavro", reporter, Schema.create(Schema.Type.STRING), outputvalue.toString(), "testavrofile1");
      collector.collect(new Pair<>(word, sum));
    }

    public void close() throws IOException {
      amos.close();
    }
  }

  @Test
  public void runTestsInOrder() throws Exception {
    String avroPath = OUTPUT_DIR.getRoot().getPath();
    testJob(avroPath);
    testProjection(avroPath);
    testProjectionNewMethodsOne(avroPath);
    testProjectionNewMethodsTwo(avroPath);
    testProjection1(avroPath);
    testJobNoreducer();
    testProjectionNoreducer(avroPath);
  }

  @SuppressWarnings("deprecation")
  public void testJob(String pathOut) throws Exception {
    JobConf job = new JobConf();

    String pathIn = INPUT_DIR.getRoot().getPath();

    File fileIn = new File(pathIn, "lines.avro");
    Path outputPath = new Path(pathOut);

    outputPath.getFileSystem(job).delete(outputPath);

    WordCountUtil.writeLinesFile(fileIn);

    job.setJobName("AvroMultipleOutputs");

    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputSchema(job, new Pair<Utf8, Long>(new Utf8(""), 0L).getSchema());

    AvroJob.setMapperClass(job, MapImpl.class);
    AvroJob.setReducerClass(job, ReduceImpl.class);

    FileInputFormat.setInputPaths(job, pathIn);
    FileOutputFormat.setOutputPath(job, outputPath);
    FileOutputFormat.setCompressOutput(job, false);
    AvroMultipleOutputs.addNamedOutput(job, "myavro", AvroOutputFormat.class,
        new Pair<Utf8, Long>(new Utf8(""), 0L).getSchema());
    AvroMultipleOutputs.addNamedOutput(job, "myavro1", AvroOutputFormat.class, Schema.create(Schema.Type.STRING));
    AvroMultipleOutputs.addNamedOutput(job, "myavro2", AvroOutputFormat.class, Schema.create(Schema.Type.STRING));
    WordCountUtil.setMeta(job);

    JobClient.runJob(job);

    WordCountUtil.validateCountsFile(new File(outputPath.toString(), "/part-00000.avro"));
  }

  @SuppressWarnings("deprecation")
  public void testProjection(String inputDirectory) throws Exception {
    JobConf job = new JobConf();

    Integer defaultRank = -1;

    String jsonSchema = "{\"type\":\"record\"," + "\"name\":\"org.apache.avro.mapred.Pair\"," + "\"fields\": [ "
        + "{\"name\":\"rank\", \"type\":\"int\", \"default\": -1}," + "{\"name\":\"value\", \"type\":\"long\"}" + "]}";

    Schema readerSchema = Schema.parse(jsonSchema);

    AvroJob.setInputSchema(job, readerSchema);

    Path inputPath = new Path(inputDirectory + "/myavro-r-00000.avro");
    FileStatus fileStatus = FileSystem.get(job).getFileStatus(inputPath);
    FileSplit fileSplit = new FileSplit(inputPath, 0, fileStatus.getLen(), job);

    AvroRecordReader<Pair<Integer, Long>> recordReader = new AvroRecordReader<>(job, fileSplit);

    AvroWrapper<Pair<Integer, Long>> inputPair = new AvroWrapper<>(null);
    NullWritable ignore = NullWritable.get();

    long sumOfCounts = 0;
    long numOfCounts = 0;
    while (recordReader.next(inputPair, ignore)) {
      Assert.assertEquals(inputPair.datum().get(0), defaultRank);
      sumOfCounts += (Long) inputPair.datum().get(1);
      numOfCounts++;
    }

    Assert.assertEquals(numOfCounts, WordCountUtil.COUNTS.size());

    long actualSumOfCounts = 0;
    for (Long count : WordCountUtil.COUNTS.values()) {
      actualSumOfCounts += count;
    }

    Assert.assertEquals(sumOfCounts, actualSumOfCounts);
  }

  @SuppressWarnings("deprecation")
  public void testProjectionNewMethodsOne(String inputDirectory) throws Exception {
    JobConf job = new JobConf();

    Integer defaultRank = -1;

    String jsonSchema = "{\"type\":\"record\"," + "\"name\":\"org.apache.avro.mapred.Pair\"," + "\"fields\": [ "
        + "{\"name\":\"rank\", \"type\":\"int\", \"default\": -1}," + "{\"name\":\"value\", \"type\":\"long\"}" + "]}";

    Schema readerSchema = Schema.parse(jsonSchema);

    AvroJob.setInputSchema(job, readerSchema);

    Path inputPath = new Path(inputDirectory + "/testavrofile-r-00000.avro");
    FileStatus fileStatus = FileSystem.get(job).getFileStatus(inputPath);
    FileSplit fileSplit = new FileSplit(inputPath, 0, fileStatus.getLen(), job);

    AvroRecordReader<Pair<Integer, Long>> recordReader = new AvroRecordReader<>(job, fileSplit);

    AvroWrapper<Pair<Integer, Long>> inputPair = new AvroWrapper<>(null);
    NullWritable ignore = NullWritable.get();

    long sumOfCounts = 0;
    long numOfCounts = 0;
    while (recordReader.next(inputPair, ignore)) {
      Assert.assertEquals(inputPair.datum().get(0), defaultRank);
      sumOfCounts += (Long) inputPair.datum().get(1);
      numOfCounts++;
    }

    Assert.assertEquals(numOfCounts, WordCountUtil.COUNTS.size());

    long actualSumOfCounts = 0;
    for (Long count : WordCountUtil.COUNTS.values()) {
      actualSumOfCounts += count;
    }

    Assert.assertEquals(sumOfCounts, actualSumOfCounts);

  }

  @SuppressWarnings("deprecation")
  // Test for a different schema output
  public void testProjection1(String inputDirectory) throws Exception {
    JobConf job = new JobConf();
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    AvroJob.setInputSchema(job, readerSchema);

    Path inputPath = new Path(inputDirectory + "/myavro1-r-00000.avro");
    FileStatus fileStatus = FileSystem.get(job).getFileStatus(inputPath);
    FileSplit fileSplit = new FileSplit(inputPath, 0, fileStatus.getLen(), job);
    AvroWrapper<Utf8> inputPair = new AvroWrapper<>(null);
    NullWritable ignore = NullWritable.get();
    AvroRecordReader<Utf8> recordReader = new AvroRecordReader<>(job, fileSplit);
    long sumOfCounts = 0;
    long numOfCounts = 0;
    while (recordReader.next(inputPair, ignore)) {
      sumOfCounts += Long.parseLong(inputPair.datum().toString().split(":")[2].replace("}", "").trim());
      numOfCounts++;
    }
    Assert.assertEquals(numOfCounts, WordCountUtil.COUNTS.size());
    long actualSumOfCounts = 0;
    for (Long count : WordCountUtil.COUNTS.values()) {
      actualSumOfCounts += count;
    }
    Assert.assertEquals(sumOfCounts, actualSumOfCounts);
  }

  @SuppressWarnings("deprecation")
  // Test for a different schema output
  public void testProjectionNewMethodsTwo(String inputDirectory) throws Exception {
    JobConf job = new JobConf();
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    AvroJob.setInputSchema(job, readerSchema);

    Path inputPath = new Path(inputDirectory + "/testavrofile1-r-00000.avro");
    FileStatus fileStatus = FileSystem.get(job).getFileStatus(inputPath);
    FileSplit fileSplit = new FileSplit(inputPath, 0, fileStatus.getLen(), job);
    AvroWrapper<Utf8> inputPair = new AvroWrapper<>(null);
    NullWritable ignore = NullWritable.get();
    AvroRecordReader<Utf8> recordReader = new AvroRecordReader<>(job, fileSplit);
    long sumOfCounts = 0;
    long numOfCounts = 0;
    while (recordReader.next(inputPair, ignore)) {
      sumOfCounts += Long.parseLong(inputPair.datum().toString().split(":")[2].replace("}", "").trim());
      numOfCounts++;
    }
    Assert.assertEquals(numOfCounts, WordCountUtil.COUNTS.size());
    long actualSumOfCounts = 0;
    for (Long count : WordCountUtil.COUNTS.values()) {
      actualSumOfCounts += count;
    }
    Assert.assertEquals(sumOfCounts, actualSumOfCounts);
  }

  @SuppressWarnings("deprecation")
  public void testJobNoreducer() throws Exception {
    JobConf job = new JobConf();
    job.setNumReduceTasks(0);

    Path outputPath = new Path(OUTPUT_DIR.getRoot().getPath());
    outputPath.getFileSystem(job).delete(outputPath);

    WordCountUtil.writeLinesFile(new File(INPUT_DIR.getRoot(), "lines.avro"));

    job.setJobName("AvroMultipleOutputs_noreducer");

    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputSchema(job, new Pair<Utf8, Long>(new Utf8(""), 0L).getSchema());

    AvroJob.setMapperClass(job, MapImpl.class);

    FileInputFormat.setInputPaths(job, new Path(INPUT_DIR.getRoot().toString()));
    FileOutputFormat.setOutputPath(job, outputPath);
    FileOutputFormat.setCompressOutput(job, false);
    AvroMultipleOutputs.addNamedOutput(job, "myavro2", AvroOutputFormat.class, Schema.create(Schema.Type.STRING));
    JobClient.runJob(job);
  }

  public void testProjectionNoreducer(String inputDirectory) throws Exception {
    JobConf job = new JobConf();
    long onel = 1;
    Schema readerSchema = Schema.create(Schema.Type.STRING);
    AvroJob.setInputSchema(job, readerSchema);
    Path inputPath = new Path(inputDirectory + "/myavro2-m-00000.avro");
    FileStatus fileStatus = FileSystem.get(job).getFileStatus(inputPath);
    FileSplit fileSplit = new FileSplit(inputPath, 0, fileStatus.getLen(), (String[]) null);
    AvroRecordReader<Utf8> recordReader = new AvroRecordReader<>(job, fileSplit);
    AvroWrapper<Utf8> inputPair = new AvroWrapper<>(null);
    NullWritable ignore = NullWritable.get();
    while (recordReader.next(inputPair, ignore)) {
      long testl = Long.parseLong(inputPair.datum().toString().split(":")[2].replace("}", "").trim());
      Assert.assertEquals(onel, testl);
    }
  }
}
