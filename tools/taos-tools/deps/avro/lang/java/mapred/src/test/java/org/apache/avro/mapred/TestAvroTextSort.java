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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroTextSort {

  @Rule
  public TemporaryFolder INPUT_DIR = new TemporaryFolder();

  @Rule
  public TemporaryFolder OUTPUT_DIR = new TemporaryFolder();

  @Test
  /**
   * Run the identity job on a "bytes" Avro file using AvroAsTextInputFormat and
   * AvroTextOutputFormat to produce a sorted "bytes" Avro file.
   */
  public void testSort() throws Exception {
    JobConf job = new JobConf();
    String inputPath = INPUT_DIR.getRoot().getPath();
    Path outputPath = new Path(OUTPUT_DIR.getRoot().getPath());
    outputPath.getFileSystem(job).delete(outputPath, true);

    WordCountUtil.writeLinesBytesFile(inputPath);

    job.setInputFormat(AvroAsTextInputFormat.class);
    job.setOutputFormat(AvroTextOutputFormat.class);
    job.setOutputKeyClass(Text.class);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, outputPath);

    JobClient.runJob(job);

    WordCountUtil.validateSortedFile(outputPath.toString() + "/part-00000.avro");
  }

}
