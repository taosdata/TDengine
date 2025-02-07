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
 * A mapper for Avro data.
 *
 * <p>
 * Applications subclass this class and pass their subclass to
 * {@link AvroJob#setMapperClass(JobConf, Class)}, overriding
 * {@link #map(Object, AvroCollector, Reporter)}.
 */
public class AvroMapper<IN, OUT> extends Configured implements JobConfigurable, Closeable {

  /** Called with each map input datum. By default, collects inputs. */
  @SuppressWarnings("unchecked")
  public void map(IN datum, AvroCollector<OUT> collector, Reporter reporter) throws IOException {
    collector.collect((OUT) datum);
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
