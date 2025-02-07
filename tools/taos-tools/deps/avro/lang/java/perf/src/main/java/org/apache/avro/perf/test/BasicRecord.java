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

package org.apache.avro.perf.test;

import java.util.Random;

public final class BasicRecord {
  public double f1;
  public double f2;
  public double f3;
  public int f4;
  public int f5;
  public int f6;

  public BasicRecord() {

  }

  public BasicRecord(final Random r) {
    f1 = r.nextDouble();
    f2 = r.nextDouble();
    f3 = r.nextDouble();
    f4 = r.nextInt();
    f5 = r.nextInt();
    f6 = r.nextInt();
  }
}
