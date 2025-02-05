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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;

@SuppressWarnings("unchecked")
class MapCollector<OUT, K, V, KO, VO> extends AvroCollector<OUT> {
  private final AvroWrapper<OUT> wrapper = new AvroWrapper<>(null);
  private final AvroKey<K> keyWrapper = new AvroKey<>(null);
  private final AvroValue<V> valueWrapper = new AvroValue<>(null);
  private OutputCollector<KO, VO> collector;
  private boolean isMapOnly;

  public MapCollector(OutputCollector<KO, VO> collector, boolean isMapOnly) {
    this.collector = collector;
    this.isMapOnly = isMapOnly;
  }

  @Override
  public void collect(OUT datum) throws IOException {
    if (isMapOnly) {
      wrapper.datum(datum);
      collector.collect((KO) wrapper, (VO) NullWritable.get());
    } else {
      // split a pair
      Pair<K, V> pair = (Pair<K, V>) datum;
      keyWrapper.datum(pair.key());
      valueWrapper.datum(pair.value());
      collector.collect((KO) keyWrapper, (VO) valueWrapper);
    }
  }
}
