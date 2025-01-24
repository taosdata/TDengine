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

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryData;
import org.apache.avro.mapred.AvroJob;

/** The {@link RawComparator} used by jobs configured with {@link TetherJob}. */
class TetherKeyComparator extends Configured implements RawComparator<TetherData> {

  private Schema schema;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null)
      schema = AvroJob.getMapOutputSchema(conf);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int diff = BinaryData.compare(b1, BinaryData.skipLong(b1, s1), l1, b2, BinaryData.skipLong(b2, s2), l2, schema);
    return diff == 0 ? -1 : diff;
  }

  @Override
  public int compare(TetherData x, TetherData y) {
    ByteBuffer b1 = x.buffer(), b2 = y.buffer();
    int diff = BinaryData.compare(b1.array(), b1.position(), b2.array(), b2.position(), schema);
    return diff == 0 ? -1 : diff;
  }

}
