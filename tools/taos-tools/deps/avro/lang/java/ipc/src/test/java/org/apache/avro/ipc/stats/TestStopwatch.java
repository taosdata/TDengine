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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestStopwatch {
  @Test
  public void testNormal() {
    FakeTicks f = new FakeTicks();
    Stopwatch s = new Stopwatch(f);
    f.passTime(10);
    s.start();
    f.passTime(20);
    assertEquals(20, s.elapsedNanos());
    f.passTime(40);
    s.stop();
    f.passTime(80);
    assertEquals(60, s.elapsedNanos());
  }

  @Test(expected = IllegalStateException.class)
  public void testNotStarted1() {
    FakeTicks f = new FakeTicks();
    Stopwatch s = new Stopwatch(f);
    s.elapsedNanos();
  }

  @Test(expected = IllegalStateException.class)
  public void testNotStarted2() {
    FakeTicks f = new FakeTicks();
    Stopwatch s = new Stopwatch(f);
    s.stop();
  }

  @Test(expected = IllegalStateException.class)
  public void testTwiceStarted() {
    FakeTicks f = new FakeTicks();
    Stopwatch s = new Stopwatch(f);
    s.start();
    s.start();
  }

  @Test(expected = IllegalStateException.class)
  public void testTwiceStopped() {
    FakeTicks f = new FakeTicks();
    Stopwatch s = new Stopwatch(f);
    s.start();
    s.stop();
    s.stop();
  }

  @Test
  public void testSystemStopwatch() {
    Stopwatch s = new Stopwatch(Stopwatch.SYSTEM_TICKS);
    s.start();
    s.stop();
    assertTrue(s.elapsedNanos() >= 0);
  }

}
