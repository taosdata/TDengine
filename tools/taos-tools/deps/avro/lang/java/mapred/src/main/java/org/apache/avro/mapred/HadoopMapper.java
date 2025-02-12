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
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Bridge between a {@link org.apache.hadoop.mapred.Mapper} and an
 * {@link AvroMapper}. Outputs are written directly when a job is map-only, but
 * are otherwise assumed to be pairs that are split.
 */
class HadoopMapper<IN, OUT, K, V, KO, VO> extends MapReduceBase
    implements Mapper<AvroWrapper<IN>, NullWritable, KO, VO> {

  private AvroMapper<IN, OUT> mapper;
  private MapCollector<OUT, K, V, KO, VO> out;
  private boolean isMapOnly;

  @Override
  @SuppressWarnings("unchecked")
  public void configure(JobConf conf) {
    this.mapper = ReflectionUtils.newInstance(conf.getClass(AvroJob.MAPPER, AvroMapper.class, AvroMapper.class), conf);
    this.isMapOnly = conf.getNumReduceTasks() == 0;
  }

  @Override
  public void map(AvroWrapper<IN> wrapper, NullWritable value, OutputCollector<KO, VO> collector, Reporter reporter)
      throws IOException {
    if (this.out == null)
      this.out = new MapCollector<>(collector, isMapOnly);
    mapper.map(wrapper.datum(), out, reporter);
  }

  @Override
  public void close() throws IOException {
    this.mapper.close();
  }

}
