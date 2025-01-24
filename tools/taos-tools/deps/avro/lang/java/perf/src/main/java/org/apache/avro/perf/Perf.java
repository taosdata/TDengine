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

package org.apache.avro.perf;

import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.WarmupMode;

/**
 * Performance tests for various low level operations of Avro encoding and
 * decoding.
 */
public final class Perf {

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(Option.builder().argName("measurementIterations").longOpt("mi").hasArg()
        .desc("The number of measure iterations").numberOfArgs(1).build());

    options.addOption(Option.builder().argName("warmupIterations").longOpt("wi").hasArg()
        .desc("The number of warmup iterations").numberOfArgs(1).build());

    options.addOption(Option.builder().argName("bulkWarmup").longOpt("bw").desc("Flag to enabled bulk warmup").build());

    options.addOption(
        Option.builder().argName("test").longOpt("test").hasArg().desc("The performance tests to run").build());

    options.addOption(Option.builder().argName("help").longOpt("help").desc("Print the help menu").build());

    final CommandLine cmd = new DefaultParser().parse(options, args);

    if (cmd.hasOption("help")) {
      final HelpFormatter formatter = new HelpFormatter();
      final PrintWriter pw = new PrintWriter(System.out);
      formatter.printUsage(pw, 80, "Perf", options);
      pw.flush();
      return;
    }

    String[] tests = cmd.getOptionValues("test");
    if (tests == null || tests.length == 0) {
      tests = new String[] { Perf.class.getPackage().getName() + ".*" };
    }

    final Integer measurementIterations = Integer.valueOf(cmd.getOptionValue("mi", "3"));
    final Integer warmupIterations = Integer.valueOf(cmd.getOptionValue("wi", "3"));

    final ChainedOptionsBuilder runOpt = new OptionsBuilder().mode(Mode.Throughput).timeout(TimeValue.seconds(60))
        .warmupIterations(warmupIterations).measurementIterations(measurementIterations).forks(1).threads(1)
        .shouldDoGC(true);

    if (cmd.hasOption("builkWarmup")) {
      runOpt.warmupMode(WarmupMode.BULK);
    }

    for (final String test : tests) {
      runOpt.include(test);
    }

    new Runner(runOpt.build()).run();
  }
}
