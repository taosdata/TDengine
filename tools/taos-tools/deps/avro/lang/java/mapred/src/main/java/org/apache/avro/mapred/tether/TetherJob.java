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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

/**
 * Constructs and submits tether jobs. This may be used as an API-based method
 * to launch tether jobs.
 */
@SuppressWarnings("deprecation")
public class TetherJob extends Configured {

  public static final String TETHER_EXEC = "avro.tether.executable";
  public static final String TETHER_EXEC_ARGS = "avro.tether.executable_args";
  public static final String TETHER_EXEC_CACHED = "avro.tether.executable_cached";
  public static final String TETHER_PROTOCOL = "avro.tether.protocol";

  /** Get the URI of the application's executable. */
  public static URI getExecutable(JobConf job) {
    try {
      return new URI(job.get("avro.tether.executable"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /** Set the URI for the application's executable. Normally this in HDFS. */
  public static void setExecutable(JobConf job, File executable) {
    setExecutable(job, executable, Collections.emptyList(), false);
  }

  /**
   * Set the URI for the application's executable (i.e the program to run in a
   * subprocess and provides the mapper/reducer).
   *
   * @param job        - Job
   * @param executable - The URI of the executable
   * @param args       - List of additional arguments; Null if no arguments
   * @param cached     - If true, the executable URI is cached using
   *                   DistributedCache - if false its not cached. I.e if the file
   *                   is already stored on each local file system or if its on a
   *                   NFS share
   */
  public static void setExecutable(JobConf job, File executable, List<String> args, boolean cached) {
    job.set(TETHER_EXEC, executable.toString());
    if (args != null) {
      StringBuilder sb = new StringBuilder();
      for (String a : args) {
        sb.append(a);
        sb.append('\n');
      }
      job.set(TETHER_EXEC_ARGS, sb.toString());
    }
    job.set(TETHER_EXEC_CACHED, (Boolean.valueOf(cached)).toString());
  }

  /**
   * Extract from the job configuration file an instance of the TRANSPROTO
   * enumeration to represent the protocol to use for the communication
   *
   * @param job
   * @return - Get the currently used protocol
   */
  public static TetheredProcess.Protocol getProtocol(JobConf job) {

    if (job.get(TetherJob.TETHER_PROTOCOL) == null) {
      return TetheredProcess.Protocol.NONE;
    } else if (job.get(TetherJob.TETHER_PROTOCOL).equals("http")) {
      return TetheredProcess.Protocol.HTTP;
    } else if (job.get(TetherJob.TETHER_PROTOCOL).equals("sasl")) {
      return TetheredProcess.Protocol.SASL;
    } else {
      throw new RuntimeException("Unknown value for protocol: " + job.get(TetherJob.TETHER_PROTOCOL));
    }

  }

  /**
   * Submit a job to the map/reduce cluster. All of the necessary modifications to
   * the job to run under tether are made to the configuration.
   */
  public static RunningJob runJob(JobConf job) throws IOException {
    setupTetherJob(job);
    return JobClient.runJob(job);
  }

  /** Submit a job to the Map-Reduce framework. */
  public static RunningJob submitJob(JobConf conf) throws IOException {
    setupTetherJob(conf);
    return new JobClient(conf).submitJob(conf);
  }

  /**
   * Determines which transport protocol (e.g http or sasl) used to communicate
   * between the parent and subprocess
   *
   * @param job   - job configuration
   * @param proto - String identifying the protocol currently http or sasl
   */
  public static void setProtocol(JobConf job, String proto) throws IOException {
    proto = proto.trim().toLowerCase();

    if (!(proto.equals("http") || proto.equals("sasl"))) {
      throw new IOException("protocol must be 'http' or 'sasl'");
    }

    job.set(TETHER_PROTOCOL, proto);

  }

  private static void setupTetherJob(JobConf job) throws IOException {
    job.setMapRunnerClass(TetherMapRunner.class);
    job.setPartitionerClass(TetherPartitioner.class);
    job.setReducerClass(TetherReducer.class);

    job.setInputFormat(TetherInputFormat.class);
    job.setOutputFormat(TetherOutputFormat.class);

    job.setOutputKeyClass(TetherData.class);
    job.setOutputKeyComparatorClass(TetherKeyComparator.class);
    job.setMapOutputValueClass(NullWritable.class);

    // set the map output key class to TetherData
    job.setMapOutputKeyClass(TetherData.class);

    // if protocol isn't set
    if (job.getStrings(TETHER_PROTOCOL) == null) {
      job.set(TETHER_PROTOCOL, "sasl");
    }

    // add TetherKeySerialization to io.serializations
    Collection<String> serializations = job.getStringCollection("io.serializations");
    if (!serializations.contains(TetherKeySerialization.class.getName())) {
      serializations.add(TetherKeySerialization.class.getName());
      job.setStrings("io.serializations", serializations.toArray(new String[0]));
    }

    // determine whether the executable should be added to the cache.
    if (job.getBoolean(TETHER_EXEC_CACHED, false)) {
      DistributedCache.addCacheFile(getExecutable(job), job);
    }
  }

}
