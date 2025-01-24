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
package org.apache.avro.ipc.stats;

/**
 * Specific implementation of histogram for integers, which also keeps track of
 * basic summary statistics.
 * 
 * @param <B>
 */
class IntegerHistogram<B> extends Histogram<B, Integer> {
  private float runningSum;
  private float runningSumOfSquares;

  public IntegerHistogram(Segmenter<B, Integer> segmenter) {
    super(segmenter);
  }

  @Override
  public void add(Integer value) {
    super.add(value);
    runningSum += value;
    runningSumOfSquares += value * value;
  }

  public float getMean() {
    if (totalCount == 0) {
      return -1;
    }
    return runningSum / (float) totalCount;
  }

  public float getUnbiasedStdDev() {
    if (totalCount <= 1) {
      return -1;
    }
    float mean = getMean();
    return (float) Math.sqrt((runningSumOfSquares - totalCount * mean * mean) / (float) (totalCount - 1));
  }
}
