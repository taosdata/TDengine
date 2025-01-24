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

package org.apache.avro.mapred.tether;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.junit.Rule;
import org.junit.Test;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.WordCountUtil;
import org.apache.avro.mapred.Pair;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.rules.TemporaryFolder;

/**
 * See also TestTetherTool for an example of how to submit jobs using the
 * thether tool.
 */
public class TestWordCountTether {

  @Rule
  public TemporaryFolder INPUT_DIR = new TemporaryFolder();

  @Rule
  public TemporaryFolder OUTPUT_DIR = new TemporaryFolder();

  /**
   * Run a job using the given transport protocol
   *
   * @param proto
   */
  private void _runjob(String proto) throws Exception {
    String outputPathStr = OUTPUT_DIR.getRoot().getPath();
    File inputPath = new File(INPUT_DIR.getRoot(), "lines.avro");

    JobConf job = new JobConf();
    Path outputPath = new Path(outputPathStr);

    outputPath.getFileSystem(job).delete(outputPath, true);

    // create the input file
    WordCountUtil.writeLinesFile(inputPath);

    File exec = new File(System.getProperty("java.home") + "/bin/java");

    // create a string of the arguments
    List<String> execargs = new ArrayList<>();
    execargs.add("-classpath");
    execargs.add(System.getProperty("java.class.path"));
    execargs.add("org.apache.avro.mapred.tether.WordCountTask");

    FileInputFormat.addInputPaths(job, inputPath.toString());
    FileOutputFormat.setOutputPath(job, outputPath);
    TetherJob.setExecutable(job, exec, execargs, false);

    Schema outscheme = new Pair<Utf8, Long>(new Utf8(""), 0L).getSchema();
    AvroJob.setInputSchema(job, Schema.create(Schema.Type.STRING));
    job.set(AvroJob.OUTPUT_SCHEMA, outscheme.toString());

    TetherJob.setProtocol(job, proto);
    TetherJob.runJob(job);

    // validate the output
    DatumReader<Pair<Utf8, Long>> reader = new SpecificDatumReader<>();
    DataFileStream<Pair<Utf8, Long>> counts = new DataFileStream<>(
        new BufferedInputStream(new FileInputStream(outputPath + "/part-00000.avro")), reader);
    int numWords = 0;
    for (Pair<Utf8, Long> wc : counts) {
      assertEquals(wc.key().toString(), WordCountUtil.COUNTS.get(wc.key().toString()), wc.value());
      numWords++;
    }

    counts.close();
    assertEquals(WordCountUtil.COUNTS.size(), numWords);

  }

  /**
   * Test the job using the sasl protocol
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testJob() throws Exception {
    _runjob("sasl");
  }

  /**
   * Test the job using the http protocol
   *
   * @throws Exception
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testhtp() throws Exception {
    _runjob("http");
  }
}
