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

package org.apache.trevni.avro.mapreduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

/**
 * Reads records from an input split representing a chunk of an Trenvi container
 * file.
 *
 * @param <T> The (java) type of data in Trevni container file.
 */
public class AvroTrevniKeyRecordReader<T> extends AvroTrevniRecordReaderBase<AvroKey<T>, NullWritable, T> {

  /** A reusable object to hold records of the Avro container file. */
  private final AvroKey<T> mCurrentKey = new AvroKey<>();

  /** {@inheritDoc} */
  @Override
  public AvroKey<T> getCurrentKey() throws IOException, InterruptedException {
    return mCurrentKey;
  }

  /** {@inheritDoc} */
  @Override
  public NullWritable getCurrentValue() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean hasNext = super.nextKeyValue();
    mCurrentKey.datum(getCurrentRecord());
    return hasNext;
  }

}
