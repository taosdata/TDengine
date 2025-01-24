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

import java.io.IOException;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskLog;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;

import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.SaslSocketServer;
import org.apache.avro.ipc.SaslSocketTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.ipc.jetty.HttpServer;
import org.apache.avro.ipc.HttpTransceiver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TetheredProcess {

  static final Logger LOG = LoggerFactory.getLogger(TetheredProcess.class);

  private JobConf job;

  TetherOutputService outputService;
  Server outputServer;
  Process subprocess;
  Transceiver clientTransceiver;
  InputProtocol inputClient;

  /**
   * Enumeration defines which transport protocol to use to communicate between
   * the map/reduce java daemons and the tethered proce
   */
  public enum Protocol {
    HTTP, SASL, NONE
  };

  // which protocol we are using
  Protocol proto;

  public TetheredProcess(JobConf job, OutputCollector<TetherData, NullWritable> collector, Reporter reporter)
      throws Exception {
    try {
      // start server
      this.outputService = new TetherOutputService(collector, reporter);

      proto = TetherJob.getProtocol(job);

      InetSocketAddress iaddress;
      switch (proto) {
      case SASL:
        iaddress = new InetSocketAddress(0);
        this.outputServer = new SaslSocketServer(new SpecificResponder(OutputProtocol.class, outputService), iaddress);
        break;
      case HTTP:
        iaddress = new InetSocketAddress(0);
        // set it up for http
        this.outputServer = new HttpServer(new SpecificResponder(OutputProtocol.class, outputService),
            iaddress.getPort());
        break;
      case NONE:
      default:
        throw new RuntimeException("No transport protocol was specified in the job configuration");
      }

      outputServer.start();

      // start sub-process, connecting back to server
      this.subprocess = startSubprocess(job);

      // check if the process has exited -- is there a better way to do this?
      boolean hasexited = false;
      try {
        // exitValue throws an exception if process hasn't exited
        this.subprocess.exitValue();
        hasexited = true;
      } catch (IllegalThreadStateException e) {
      }
      if (hasexited) {
        LOG.error("Could not start subprocess");
        throw new RuntimeException("Could not start subprocess");
      }
      // open client, connecting to sub-process
      switch (proto) {
      case SASL:
        this.clientTransceiver = new SaslSocketTransceiver(new InetSocketAddress(outputService.inputPort()));
        break;
      case HTTP:
        this.clientTransceiver = new HttpTransceiver(new URL("http://127.0.0.1:" + outputService.inputPort()));
        break;
      default:
        throw new RuntimeException("Error: code to handle this protocol is not implemented");
      }

      this.inputClient = SpecificRequestor.getClient(InputProtocol.class, clientTransceiver);

    } catch (Exception t) {
      close();
      throw t;
    }
  }

  public void close() {
    if (clientTransceiver != null)
      try {
        clientTransceiver.close();
      } catch (IOException e) {
      } // ignore
    if (subprocess != null)
      subprocess.destroy();
    if (outputServer != null)
      outputServer.close();
  }

  private Process startSubprocess(JobConf job) throws IOException, InterruptedException {
    // get the executable command
    List<String> command = new ArrayList<>();

    String executable = "";
    if (job.getBoolean(TetherJob.TETHER_EXEC_CACHED, false)) {
      // we want to use the cached executable
      Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
      if (localFiles == null) { // until MAPREDUCE-476
        URI[] files = DistributedCache.getCacheFiles(job);
        localFiles = new Path[] { new Path(files[0].toString()) };
      }
      executable = localFiles[0].toString();
      FileUtil.chmod(executable.toString(), "a+x");
    } else {
      executable = job.get(TetherJob.TETHER_EXEC);
    }

    command.add(executable);

    // Add the executable arguments. We assume the arguments are separated by
    // newlines so we split the argument string based on newlines and add each
    // token to command We need to do it this way because
    // TaskLog.captureOutAndError will put quote marks around each argument so
    // if we pass a single string containing all arguments we get quoted
    // incorrectly
    String args = job.get(TetherJob.TETHER_EXEC_ARGS);

    // args might be null if TETHER_EXEC_ARGS wasn't set.
    if (args != null) {
      String[] aparams = args.split("\n");
      for (int i = 0; i < aparams.length; i++) {
        aparams[i] = aparams[i].trim();
        if (aparams[i].length() > 0) {
          command.add(aparams[i]);
        }
      }
    }

    if (System.getProperty("hadoop.log.dir") == null && System.getenv("HADOOP_LOG_DIR") != null)
      System.setProperty("hadoop.log.dir", System.getenv("HADOOP_LOG_DIR"));

    // wrap the command in a stdout/stderr capture
    TaskAttemptID taskid = TaskAttemptID.forName(job.get("mapred.task.id"));
    File stdout = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDOUT);
    File stderr = TaskLog.getTaskLogFile(taskid, false, TaskLog.LogName.STDERR);
    long logLength = TaskLog.getTaskLogLength(job);
    command = TaskLog.captureOutAndError(null, command, stdout, stderr, logLength, false);
    stdout.getParentFile().mkdirs();
    stderr.getParentFile().mkdirs();

    // add output server's port to env
    Map<String, String> env = new HashMap<>();
    env.put("AVRO_TETHER_OUTPUT_PORT", Integer.toString(outputServer.getPort()));

    // add an environment variable to specify what protocol to use for communication
    env.put("AVRO_TETHER_PROTOCOL", job.get(TetherJob.TETHER_PROTOCOL));

    // print an info message about the command
    String imsg = "";
    for (String s : command) {
      imsg = s + " ";
    }
    LOG.info("TetheredProcess.startSubprocess: command: " + imsg);
    LOG.info("Tetheredprocess.startSubprocess: stdout logged to: " + stdout.toString());
    LOG.info("Tetheredprocess.startSubprocess: stderr logged to: " + stderr.toString());

    // start child process
    ProcessBuilder builder = new ProcessBuilder(command);

    builder.environment().putAll(env);
    return builder.start();
  }

}
