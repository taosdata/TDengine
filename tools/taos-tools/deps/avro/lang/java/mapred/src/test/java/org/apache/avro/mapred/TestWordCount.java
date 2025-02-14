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

import static org.junit.Assert.assertEquals;

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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestWordCount {

  @ClassRule
  public static TemporaryFolder INPUT_DIR = new TemporaryFolder();

  @ClassRule
  public static TemporaryFolder OUTPUT_DIR = new TemporaryFolder();

  public static class MapImpl extends AvroMapper<Utf8, Pair<Utf8, Long>> {
    @Override
    public void map(Utf8 text, AvroCollector<Pair<Utf8, Long>> collector, Reporter reporter) throws IOException {
      StringTokenizer tokens = new StringTokenizer(text.toString());
      while (tokens.hasMoreTokens())
        collector.collect(new Pair<>(new Utf8(tokens.nextToken()), 1L));
    }
  }

  public static class ReduceImpl extends AvroReducer<Utf8, Long, Pair<Utf8, Long>> {
    @Override
    public void reduce(Utf8 word, Iterable<Long> counts, AvroCollector<Pair<Utf8, Long>> collector, Reporter reporter)
        throws IOException {
      long sum = 0;
      for (long count : counts)
        sum += count;
      collector.collect(new Pair<>(word, sum));
    }
  }

  @Test
  public void runTestsInOrder() throws Exception {
    String pathOut = OUTPUT_DIR.getRoot().getPath();
    testJob(pathOut);
    testProjection(pathOut);
  }

  @SuppressWarnings("deprecation")
  public void testJob(String pathOut) throws Exception {
    JobConf job = new JobConf();
    String pathIn = INPUT_DIR.getRoot().getPath();

    WordCountUtil.writeLinesFile(pathIn + "/lines.avro");

    Path outputPath = new Path(pathOut);
    outputPath.getFileSystem(job).delete(outputPath);

    job.setJobName("wordcount");

    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    AvroJob.setOutputSchema(job, new Pair<Utf8, Long>(new Utf8(""), 0L).getSchema());

    AvroJob.setMapperClass(job, MapImpl.class);
    AvroJob.setCombinerClass(job, ReduceImpl.class);
    AvroJob.setReducerClass(job, ReduceImpl.class);

    FileInputFormat.setInputPaths(job, new Path(pathIn));
    FileOutputFormat.setOutputPath(job, new Path(pathOut));
    FileOutputFormat.setCompressOutput(job, true);

    WordCountUtil.setMeta(job);

    JobClient.runJob(job);

    WordCountUtil.validateCountsFile(new File(pathOut, "part-00000.avro"));
  }

  @SuppressWarnings("deprecation")
  public void testProjection(String inputPathString) throws Exception {
    JobConf job = new JobConf();

    Integer defaultRank = -1;

    String jsonSchema = "{\"type\":\"record\"," + "\"name\":\"org.apache.avro.mapred.Pair\"," + "\"fields\": [ "
        + "{\"name\":\"rank\", \"type\":\"int\", \"default\": -1}," + "{\"name\":\"value\", \"type\":\"long\"}" + "]}";

    Schema readerSchema = Schema.parse(jsonSchema);

    AvroJob.setInputSchema(job, readerSchema);

    Path inputPath = new Path(inputPathString + "/part-00000.avro");
    FileStatus fileStatus = FileSystem.get(job).getFileStatus(inputPath);
    FileSplit fileSplit = new FileSplit(inputPath, 0, fileStatus.getLen(), job);

    AvroRecordReader<Pair<Integer, Long>> recordReader = new AvroRecordReader<>(job, fileSplit);

    AvroWrapper<Pair<Integer, Long>> inputPair = new AvroWrapper<>(null);
    NullWritable ignore = NullWritable.get();

    long sumOfCounts = 0;
    long numOfCounts = 0;
    while (recordReader.next(inputPair, ignore)) {
      assertEquals(inputPair.datum().get(0), defaultRank);
      sumOfCounts += (Long) inputPair.datum().get(1);
      numOfCounts++;
    }

    assertEquals(numOfCounts, WordCountUtil.COUNTS.size());

    long actualSumOfCounts = 0;
    for (Long count : WordCountUtil.COUNTS.values()) {
      actualSumOfCounts += count;
    }

    assertEquals(sumOfCounts, actualSumOfCounts);
  }

}
