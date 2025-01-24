/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.trevni;

import java.util.Random;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

public class TestUtil {

  private static long seed;
  private static boolean seedSet;

  /**
   * Returns the random seed for this test run. By default uses the current time,
   * but a test run can be replicated by specifying the "test.seed" system
   * property. The seed is printed the first time it's accessed so that failures
   * can be replicated if needed.
   */
  public static long getRandomSeed() {
    if (!seedSet) {
      String configured = System.getProperty("test.seed");
      if (configured != null)
        seed = Long.valueOf(configured);
      else
        seed = System.currentTimeMillis();
      System.err.println("test.seed=" + seed);
      seedSet = true;
    }
    return seed;
  }

  public static void resetRandomSeed() {
    seedSet = false;
  }

  public static Random createRandom() {
    return new Random(getRandomSeed());
  }

  public static ByteBuffer randomBytes(Random random) {
    byte[] bytes = new byte[randomLength(random)];
    random.nextBytes(bytes);
    return ByteBuffer.wrap(bytes);
  }

  public static String randomString(Random random) {
    int length = randomLength(random);
    char[] chars = new char[length];
    for (int i = 0; i < length; i++)
      chars[i] = (char) ('a' + random.nextInt('z' - 'a'));
    return new String(chars);
  }

  /**
   * Returns [0-15] 15/16 times. Returns [0-255] 255/256 times. Returns [0-4095]
   * 4095/4096 times. Returns [0-65535] every time.
   */
  public static int randomLength(Random random) {
    int n = random.nextInt();
    if (n < 0)
      n = -n;
    return n & ((n & 0xF0000) != 0 ? 0xF : ((n & 0xFF0000) != 0 ? 0xFF : ((n & 0xFFF0000) != 0 ? 0xFFF : 0xFFFF)));
  }

  @Test
  public void testRandomLength() {
    long total = 0;
    int count = 1024 * 1024;
    int min = Short.MAX_VALUE;
    int max = 0;
    Random r = createRandom();
    for (int i = 0; i < count; i++) {
      int length = randomLength(r);
      if (min > length)
        min = length;
      if (max < length)
        max = length;
      total += length;
    }
    Assert.assertEquals(0, min);
    Assert.assertTrue(max > 1024 * 32);

    float average = total / (float) count;
    Assert.assertTrue(average > 16.0f);
    Assert.assertTrue(average < 64.0f);

  }

}
