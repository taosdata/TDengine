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

import java.nio.ByteBuffer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroJob;

class TetherPartitioner implements Partitioner<TetherData, NullWritable> {

  private static final ThreadLocal<Integer> CACHE = new ThreadLocal<>();

  private Schema schema;

  @Override
  public void configure(JobConf job) {
    schema = AvroJob.getMapOutputSchema(job);
  }

  static void setNextPartition(int newValue) {
    CACHE.set(newValue);
  }

  @Override
  public int getPartition(TetherData key, NullWritable value, int numPartitions) {
    Integer result = CACHE.get();
    if (result != null) // return cached value
      return result;

    ByteBuffer b = key.buffer();
    int p = b.position();
    int hashCode = BinaryData.hashCode(b.array(), p, b.limit() - p, schema);
    if (hashCode < 0)
      hashCode = -hashCode;
    return hashCode % numPartitions;
  }

}
