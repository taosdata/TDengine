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
package org.apache.avro.tool;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.tether.TetherJob;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;

@SuppressWarnings("deprecation")
public class TetherTool implements Tool {
  public TetherJob job;

  @Override
  public String getName() {
    return "tether";
  }

  @Override
  public String getShortDescription() {
    return "Run a tethered mapreduce job.";
  }

  @Override
  public int run(InputStream ins, PrintStream outs, PrintStream err, List<String> args) throws Exception {

    String[] argarry = args.toArray(new String[0]);
    Options opts = new Options();

    Option helpopt = OptionBuilder.hasArg(false).withDescription("print this message").create("help");

    Option inopt = OptionBuilder.hasArg().isRequired().withDescription("comma-separated input paths").create("in");

    Option outopt = OptionBuilder.hasArg().isRequired().withDescription("The output path.").create("out");

    Option pargs = OptionBuilder.hasArg().withDescription(
        "A string containing the command line arguments to pass to the tethered process. String should be enclosed in quotes")
        .create("exec_args");

    Option popt = OptionBuilder.hasArg().isRequired().withDescription("executable program, usually in HDFS")
        .create("program");

    Option outscopt = OptionBuilder.withType(File.class).hasArg().isRequired()
        .withDescription("schema file for output of reducer").create("outschema");

    Option outscmapopt = OptionBuilder.withType(File.class).hasArg()
        .withDescription("(optional) map output schema file,  if different from outschema").create("outschemamap");

    Option redopt = OptionBuilder.withType(Integer.class).hasArg().withDescription("(optional) number of reducers")
        .create("reduces");

    Option cacheopt = OptionBuilder.withType(Boolean.class).hasArg()
        .withDescription(
            "(optional) boolean indicating whether or not the executable should be distributed via distributed cache")
        .create("exec_cached");

    Option protoopt = OptionBuilder.hasArg()
        .withDescription("(optional) specifies the transport protocol 'http' or 'sasl'").create("protocol");

    opts.addOption(redopt);
    opts.addOption(outscopt);
    opts.addOption(popt);
    opts.addOption(pargs);
    opts.addOption(inopt);
    opts.addOption(outopt);
    opts.addOption(helpopt);
    opts.addOption(outscmapopt);
    opts.addOption(cacheopt);
    opts.addOption(protoopt);

    CommandLineParser parser = new GnuParser();

    CommandLine line = null;
    HelpFormatter formatter = new HelpFormatter();

    JobConf job = new JobConf();

    try {
      line = parser.parse(opts, argarry);

      if (line.hasOption("help")) {
        formatter.printHelp("tether", opts);
        return 0;
      }

      FileInputFormat.addInputPaths(job, line.getOptionValue("in"));
      FileOutputFormat.setOutputPath(job, new Path(line.getOptionValue("out")));

      List<String> exargs = null;
      Boolean cached = false;

      if (line.hasOption("exec_args")) {
        String[] splitargs = line.getOptionValue("exec_args").split(" ");
        exargs = new ArrayList<>(Arrays.asList(splitargs));
      }
      if (line.hasOption("exec_cached")) {
        cached = Boolean.parseBoolean(line.getOptionValue("exec_cached"));
      }
      TetherJob.setExecutable(job, new File(line.getOptionValue("program")), exargs, cached);

      File outschema = (File) line.getParsedOptionValue("outschema");
      job.set(AvroJob.OUTPUT_SCHEMA, Schema.parse(outschema).toString());
      if (line.hasOption("outschemamap")) {
        job.set(AvroJob.MAP_OUTPUT_SCHEMA,
            new Schema.Parser().parse((File) line.getParsedOptionValue("outschemamap")).toString());
      }
      if (line.hasOption("reduces")) {
        job.setNumReduceTasks((Integer) line.getParsedOptionValue("reduces"));
      }
      if (line.hasOption("protocol")) {
        TetherJob.setProtocol(job, line.getOptionValue("protocol"));
      }
    } catch (Exception exp) {
      System.out.println("Unexpected exception: " + exp.getMessage());
      formatter.printHelp("tether", opts);
      return -1;
    }

    TetherJob.runJob(job);
    return 0;
  }

}
