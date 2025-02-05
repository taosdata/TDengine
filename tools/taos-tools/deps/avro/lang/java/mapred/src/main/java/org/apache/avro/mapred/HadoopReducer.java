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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Bridge between a {@link org.apache.hadoop.mapred.Reducer} and an
 * {@link AvroReducer}.
 */
class HadoopReducer<K, V, OUT> extends HadoopReducerBase<K, V, OUT, AvroWrapper<OUT>, NullWritable> {

  @Override
  @SuppressWarnings("unchecked")
  protected AvroReducer<K, V, OUT> getReducer(JobConf conf) {
    return ReflectionUtils.newInstance(conf.getClass(AvroJob.REDUCER, AvroReducer.class, AvroReducer.class), conf);
  }

  private class ReduceCollector extends AvroCollector<OUT> {
    private final AvroWrapper<OUT> wrapper = new AvroWrapper<>(null);
    private OutputCollector<AvroWrapper<OUT>, NullWritable> out;

    public ReduceCollector(OutputCollector<AvroWrapper<OUT>, NullWritable> out) {
      this.out = out;
    }

    @Override
    public void collect(OUT datum) throws IOException {
      wrapper.datum(datum);
      out.collect(wrapper, NullWritable.get());
    }
  }

  @Override
  protected AvroCollector<OUT> getCollector(OutputCollector<AvroWrapper<OUT>, NullWritable> collector) {
    return new ReduceCollector(collector);
  }

}
