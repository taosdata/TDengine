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

import java.io.IOException;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.file.DataFileReader;
import static org.apache.avro.file.DataFileConstants.SNAPPY_CODEC;
import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Test;

import test.Weather;

/** Tests mapred API with a specific record. */
public class TestWeather {

  private static final AtomicInteger mapCloseCalls = new AtomicInteger();
  private static final AtomicInteger mapConfigureCalls = new AtomicInteger();
  private static final AtomicInteger reducerCloseCalls = new AtomicInteger();
  private static final AtomicInteger reducerConfigureCalls = new AtomicInteger();

  @After
  public void tearDown() {
    mapCloseCalls.set(0);
    mapConfigureCalls.set(0);
    reducerCloseCalls.set(0);
    reducerConfigureCalls.set(0);
  }

  /** Uses default mapper with no reduces for a map-only identity job. */
  @Test
  @SuppressWarnings("deprecation")
  public void testMapOnly() throws Exception {
    JobConf job = new JobConf();
    String inDir = System.getProperty("share.dir", "../../../share") + "/test/data";
    Path input = new Path(inDir + "/weather.avro");
    Path output = new Path("target/test/weather-ident");

    output.getFileSystem(job).delete(output);

    job.setJobName("identity map weather");

    AvroJob.setInputSchema(job, Weather.SCHEMA$);
    AvroJob.setOutputSchema(job, Weather.SCHEMA$);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);
    FileOutputFormat.setCompressOutput(job, true);

    job.setNumReduceTasks(0); // map-only

    JobClient.runJob(job);

    // check output is correct
    DatumReader<Weather> reader = new SpecificDatumReader<>();
    DataFileReader<Weather> check = new DataFileReader<>(new File(inDir + "/weather.avro"), reader);
    DataFileReader<Weather> sorted = new DataFileReader<>(new File(output.toString() + "/part-00000.avro"), reader);

    for (Weather w : sorted)
      assertEquals(check.next(), w);

    check.close();
    sorted.close();
  }

  // maps input Weather to Pair<Weather,Void>, to sort by Weather
  public static class SortMapper extends AvroMapper<Weather, Pair<Weather, Void>> {
    @Override
    public void map(Weather w, AvroCollector<Pair<Weather, Void>> collector, Reporter reporter) throws IOException {
      collector.collect(new Pair<>(w, (Void) null));
    }

    @Override
    public void close() throws IOException {
      mapCloseCalls.incrementAndGet();
    }

    @Override
    public void configure(JobConf jobConf) {
      mapConfigureCalls.incrementAndGet();
    }
  }

  // output keys only, since values are empty
  public static class SortReducer extends AvroReducer<Weather, Void, Weather> {
    @Override
    public void reduce(Weather w, Iterable<Void> ignore, AvroCollector<Weather> collector, Reporter reporter)
        throws IOException {
      collector.collect(w);
    }

    @Override
    public void close() throws IOException {
      reducerCloseCalls.incrementAndGet();
    }

    @Override
    public void configure(JobConf jobConf) {
      reducerConfigureCalls.incrementAndGet();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSort() throws Exception {
    JobConf job = new JobConf();
    String inDir = "../../../share/test/data";
    Path input = new Path(inDir + "/weather.avro");
    Path output = new Path("target/test/weather-sort");

    output.getFileSystem(job).delete(output);

    job.setJobName("sort weather");

    AvroJob.setInputSchema(job, Weather.SCHEMA$);
    AvroJob.setMapOutputSchema(job, Pair.getPairSchema(Weather.SCHEMA$, Schema.create(Type.NULL)));
    AvroJob.setOutputSchema(job, Weather.SCHEMA$);

    AvroJob.setMapperClass(job, SortMapper.class);
    AvroJob.setReducerClass(job, SortReducer.class);

    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);
    FileOutputFormat.setCompressOutput(job, true);
    AvroJob.setOutputCodec(job, SNAPPY_CODEC);

    JobClient.runJob(job);

    // check output is correct
    DatumReader<Weather> reader = new SpecificDatumReader<>();
    DataFileReader<Weather> check = new DataFileReader<>(new File(inDir + "/weather-sorted.avro"), reader);
    DataFileReader<Weather> sorted = new DataFileReader<>(new File(output.toString() + "/part-00000.avro"), reader);

    for (Weather w : sorted)
      assertEquals(check.next(), w);

    check.close();
    sorted.close();

    // check that AvroMapper and AvroReducer get close() and configure() called
    assertEquals(1, mapCloseCalls.get());
    assertEquals(1, reducerCloseCalls.get());
    assertEquals(1, mapConfigureCalls.get());
    assertEquals(1, reducerConfigureCalls.get());

  }

}
