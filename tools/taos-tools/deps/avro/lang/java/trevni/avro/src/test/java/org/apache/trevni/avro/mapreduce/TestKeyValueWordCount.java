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

package org.apache.trevni.avro.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.trevni.avro.WordCountUtil;
import org.junit.Test;

public class TestKeyValueWordCount {

  private static long total = 0;

  static final Schema STRING = Schema.create(Schema.Type.STRING);
  static {
    GenericData.setStringType(STRING, GenericData.StringType.String);
  }
  static final Schema LONG = Schema.create(Schema.Type.LONG);

  private static class WordCountMapper extends Mapper<AvroKey<String>, NullWritable, Text, LongWritable> {
    private LongWritable mCount = new LongWritable();
    private Text mText = new Text();

    @Override
    protected void setup(Context context) {
      mCount.set(1);
    }

    @Override
    protected void map(AvroKey<String> key, NullWritable value, Context context)
        throws IOException, InterruptedException {

      try {
        StringTokenizer tokens = new StringTokenizer(key.datum());
        while (tokens.hasMoreTokens()) {
          mText.set(tokens.nextToken());
          context.write(mText, mCount);
        }
      } catch (Exception e) {
        throw new RuntimeException(key + " " + key.datum(), e);
      }

    }
  }

  private static class WordCountReducer extends Reducer<Text, LongWritable, AvroKey<String>, AvroValue<Long>> {

    AvroKey<String> resultKey = new AvroKey<>();
    AvroValue<Long> resultValue = new AvroValue<>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable value : values) {
        sum += value.get();
      }
      resultKey.datum(key.toString());
      resultValue.datum(sum);

      context.write(resultKey, resultValue);
    }
  }

  public static class Counter extends Mapper<AvroKey<String>, AvroValue<Long>, NullWritable, NullWritable> {
    @Override
    protected void map(AvroKey<String> key, AvroValue<Long> value, Context context)
        throws IOException, InterruptedException {
      total += value.datum();
    }
  }

  @Test
  public void testIOFormat() throws Exception {
    checkOutputFormat();
    checkInputFormat();
  }

  public void checkOutputFormat() throws Exception {
    Job job = Job.getInstance();

    WordCountUtil wordCountUtil = new WordCountUtil("trevniMapReduceKeyValueTest", "part-r-00000");

    wordCountUtil.writeLinesFile();

    AvroJob.setInputKeySchema(job, STRING);
    AvroJob.setOutputKeySchema(job, STRING);
    AvroJob.setOutputValueSchema(job, LONG);

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    FileInputFormat.setInputPaths(job, new Path(wordCountUtil.getDir().toString() + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(wordCountUtil.getDir().toString() + "/out"));
    FileOutputFormat.setCompressOutput(job, true);

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setOutputFormatClass(AvroTrevniKeyValueOutputFormat.class);

    job.waitForCompletion(true);

    wordCountUtil.validateCountsFileGenericRecord();
  }

  public void checkInputFormat() throws Exception {
    Job job = Job.getInstance();

    WordCountUtil wordCountUtil = new WordCountUtil("trevniMapReduceKeyValueTest");

    job.setMapperClass(Counter.class);

    FileInputFormat.setInputPaths(job, new Path(wordCountUtil.getDir().toString() + "/out/*"));
    job.setInputFormatClass(AvroTrevniKeyValueInputFormat.class);

    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);

    total = 0;
    job.waitForCompletion(true);
    assertEquals(WordCountUtil.TOTAL, total);

  }
}
