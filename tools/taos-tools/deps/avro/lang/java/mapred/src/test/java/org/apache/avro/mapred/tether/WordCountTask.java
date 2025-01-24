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
import java.util.StringTokenizer;

import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example Java tethered mapreduce executable. Implements map and reduce
 * functions for word count.
 */
public class WordCountTask extends TetherTask<Utf8, Pair<Utf8, Long>, Pair<Utf8, Long>> {

  static final Logger LOG = LoggerFactory.getLogger(WordCountTask.class);

  @Override
  public void map(Utf8 text, Collector<Pair<Utf8, Long>> collector) throws IOException {
    StringTokenizer tokens = new StringTokenizer(text.toString());
    while (tokens.hasMoreTokens())
      collector.collect(new Pair<>(new Utf8(tokens.nextToken()), 1L));
  }

  private long sum;

  @Override
  public void reduce(Pair<Utf8, Long> wc, Collector<Pair<Utf8, Long>> c) {
    sum += wc.value();
  }

  @Override
  public void reduceFlush(Pair<Utf8, Long> wc, Collector<Pair<Utf8, Long>> c) throws IOException {
    wc.value(sum);
    c.collect(wc);
    sum = 0;
  }

  public static void main(String[] args) throws Exception {
    new TetherTaskRunner(new WordCountTask()).join();
    LOG.info("WordCountTask finished");
  }

}
