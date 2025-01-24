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
import java.util.Iterator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

abstract class HadoopReducerBase<K, V, OUT, KO, VO> extends MapReduceBase
    implements Reducer<AvroKey<K>, AvroValue<V>, KO, VO> {

  private AvroReducer<K, V, OUT> reducer;
  private AvroCollector<OUT> collector;

  protected abstract AvroReducer<K, V, OUT> getReducer(JobConf conf);

  protected abstract AvroCollector<OUT> getCollector(OutputCollector<KO, VO> c);

  @Override
  public void configure(JobConf conf) {
    this.reducer = getReducer(conf);
  }

  class ReduceIterable implements Iterable<V>, Iterator<V> {
    private Iterator<AvroValue<V>> values;

    @Override
    public boolean hasNext() {
      return values.hasNext();
    }

    @Override
    public V next() {
      return values.next().datum();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<V> iterator() {
      return this;
    }
  }

  private ReduceIterable reduceIterable = new ReduceIterable();

  @Override
  public final void reduce(AvroKey<K> key, Iterator<AvroValue<V>> values, OutputCollector<KO, VO> out,
      Reporter reporter) throws IOException {
    if (this.collector == null)
      this.collector = getCollector(out);
    reduceIterable.values = values;
    reducer.reduce(key.datum(), reduceIterable, collector, reporter);
  }

  @Override
  public void close() throws IOException {
    this.reducer.close();
  }
}
