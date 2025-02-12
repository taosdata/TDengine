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

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;

/**
 * A reducer for Avro data.
 *
 * <p>
 * Applications should subclass this class and pass their subclass to
 * {@link AvroJob#setReducerClass(JobConf, Class)} and perhaps
 * {@link AvroJob#setCombinerClass(JobConf, Class)}. Subclasses override
 * {@link #reduce(Object, Iterable, AvroCollector, Reporter)}.
 */

public class AvroReducer<K, V, OUT> extends Configured implements JobConfigurable, Closeable {

  private Pair<K, V> outputPair;

  /**
   * Called with all map output values with a given key. By default, pairs key
   * with each value, collecting {@link Pair} instances.
   */
  @SuppressWarnings("unchecked")
  public void reduce(K key, Iterable<V> values, AvroCollector<OUT> collector, Reporter reporter) throws IOException {
    if (outputPair == null)
      outputPair = new Pair<>(AvroJob.getOutputSchema(getConf()));
    for (V value : values) {
      outputPair.set(key, value);
      collector.collect((OUT) outputPair);
    }
  }

  /** Subclasses can override this as desired. */
  @Override
  public void close() throws IOException {
    // no op
  }

  /** Subclasses can override this as desired. */
  @Override
  public void configure(JobConf jobConf) {
    // no op
  }
}
