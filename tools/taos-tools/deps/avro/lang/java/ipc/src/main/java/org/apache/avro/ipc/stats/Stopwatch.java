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

/** Encapsulates the passing of time. */
class Stopwatch {
  /** Encapsulates ticking time sources. */
  interface Ticks {
    /**
     * Returns a number of "ticks" in nanoseconds. This should be monotonically
     * non-decreasing.
     */
    long ticks();
  }

  /** Default System time source. */
  public final static Ticks SYSTEM_TICKS = new SystemTicks();

  private Ticks ticks;
  private long start;
  private long elapsed = -1;
  private boolean running;

  public Stopwatch(Ticks ticks) {
    this.ticks = ticks;
  }

  /** Returns seconds that have elapsed since start() */
  public long elapsedNanos() {
    if (running) {
      return this.ticks.ticks() - start;
    } else {
      if (elapsed == -1)
        throw new IllegalStateException();
      return elapsed;
    }
  }

  /** Starts the stopwatch. */
  public void start() {
    if (running)
      throw new IllegalStateException();
    start = ticks.ticks();
    running = true;
  }

  /** Stops the stopwatch and calculates the elapsed time. */
  public void stop() {
    if (!running)
      throw new IllegalStateException();
    elapsed = ticks.ticks() - start;
    running = false;
  }

  /** Implementation of Ticks using System.nanoTime(). */
  private static class SystemTicks implements Ticks {
    @Override
    public long ticks() {
      return System.nanoTime();
    }
  }
}
