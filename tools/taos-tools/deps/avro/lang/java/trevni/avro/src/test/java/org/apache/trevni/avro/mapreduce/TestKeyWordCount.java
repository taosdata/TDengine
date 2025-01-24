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
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.Pair;
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

public class TestKeyWordCount {

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

  private static class WordCountReducer extends Reducer<Text, LongWritable, AvroKey<GenericData.Record>, NullWritable> {

    private AvroKey<GenericData.Record> result;

    @Override
    protected void setup(Context context) {
      result = new AvroKey<>();
      result.datum(new Record(Pair.getPairSchema(STRING, LONG)));
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long count = 0;
      for (LongWritable value : values) {
        count += value.get();
      }

      result.datum().put("key", key.toString());
      result.datum().put("value", count);

      context.write(result, NullWritable.get());
    }
  }

  public static class Counter extends Mapper<AvroKey<GenericData.Record>, NullWritable, NullWritable, NullWritable> {
    @Override
    protected void map(AvroKey<GenericData.Record> key, NullWritable value, Context context)
        throws IOException, InterruptedException {
      total += (Long) key.datum().get("value");
    }
  }

  @Test
  public void testIOFormat() throws Exception {
    checkOutputFormat();
    checkInputFormat();
  }

  public void checkOutputFormat() throws Exception {
    Job job = Job.getInstance();

    WordCountUtil wordCountUtil = new WordCountUtil("trevniMapReduceKeyTest", "part-r-00000");

    wordCountUtil.writeLinesFile();

    AvroJob.setInputKeySchema(job, STRING);
    AvroJob.setOutputKeySchema(job, Pair.getPairSchema(STRING, LONG));

    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    FileInputFormat.setInputPaths(job, new Path(wordCountUtil.getDir().toString() + "/in"));
    FileOutputFormat.setOutputPath(job, new Path(wordCountUtil.getDir().toString() + "/out"));
    FileOutputFormat.setCompressOutput(job, true);

    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setOutputFormatClass(AvroTrevniKeyOutputFormat.class);

    job.waitForCompletion(true);

    wordCountUtil.validateCountsFile();
  }

  public void checkInputFormat() throws Exception {
    Job job = Job.getInstance();

    WordCountUtil wordCountUtil = new WordCountUtil("trevniMapReduceKeyTest");

    job.setMapperClass(Counter.class);

    Schema subSchema = new Schema.Parser().parse("{\"type\":\"record\"," + "\"name\":\"PairValue\"," + "\"fields\": [ "
        + "{\"name\":\"value\", \"type\":\"long\"}" + "]}");
    AvroJob.setInputKeySchema(job, subSchema);

    FileInputFormat.setInputPaths(job, new Path(wordCountUtil.getDir().toString() + "/out/*"));
    job.setInputFormatClass(AvroTrevniKeyInputFormat.class);

    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);

    total = 0;
    job.waitForCompletion(true);
    assertEquals(WordCountUtil.TOTAL, total);

  }

}
