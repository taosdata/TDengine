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

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Bridge between a {@link org.apache.hadoop.mapred.Reducer} and an
 * {@link AvroReducer} used when combining. When combining, map output pairs
 * must be split before they're collected.
 */
class HadoopCombiner<K, V> extends HadoopReducerBase<K, V, Pair<K, V>, AvroKey<K>, AvroValue<V>> {

  @Override
  @SuppressWarnings("unchecked")
  protected AvroReducer<K, V, Pair<K, V>> getReducer(JobConf conf) {
    return ReflectionUtils.newInstance(conf.getClass(AvroJob.COMBINER, AvroReducer.class, AvroReducer.class), conf);
  }

  private class PairCollector extends AvroCollector<Pair<K, V>> {
    private final AvroKey<K> keyWrapper = new AvroKey<>(null);
    private final AvroValue<V> valueWrapper = new AvroValue<>(null);
    private OutputCollector<AvroKey<K>, AvroValue<V>> collector;

    public PairCollector(OutputCollector<AvroKey<K>, AvroValue<V>> collector) {
      this.collector = collector;
    }

    @Override
    public void collect(Pair<K, V> datum) throws IOException {
      keyWrapper.datum(datum.key()); // split the Pair
      valueWrapper.datum(datum.value());
      collector.collect(keyWrapper, valueWrapper);
    }
  }

  @Override
  protected AvroCollector<Pair<K, V>> getCollector(OutputCollector<AvroKey<K>, AvroValue<V>> collector) {
    return new PairCollector(collector);
  }

}
