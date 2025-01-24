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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.ipc.stats.Histogram.Segmenter;
import org.apache.avro.ipc.stats.Stopwatch.Ticks;

/**
 * Collects count and latency statistics about RPC calls. Keeps data for every
 * method. Can be added to a Requestor (client) or Responder (server).
 *
 * This uses milliseconds as the standard unit of measure throughout the class,
 * stored in floats.
 */
public class StatsPlugin extends RPCPlugin {
  /** Static declaration of histogram buckets. */
  public static final Segmenter<String, Float> LATENCY_SEGMENTER = new Histogram.TreeMapSegmenter<>(
      new TreeSet<>(Arrays.asList(0f, 25f, 50f, 75f, 100f, 200f, 300f, 500f, 750f, 1000f, // 1 second
          2000f, 5000f, 10000f, 60000f, // 1 minute
          600000f)));

  public static final Segmenter<String, Integer> PAYLOAD_SEGMENTER = new Histogram.TreeMapSegmenter<>(
      new TreeSet<>(Arrays.asList(0, 25, 50, 75, 100, 200, 300, 500, 750, 1000, // 1 k
          2000, 5000, 10000, 50000, 100000)));

  /**
   * Per-method histograms. Must be accessed while holding a lock.
   */
  Map<Message, FloatHistogram<?>> methodTimings = new HashMap<>();

  Map<Message, IntegerHistogram<?>> sendPayloads = new HashMap<>();

  Map<Message, IntegerHistogram<?>> receivePayloads = new HashMap<>();

  /** RPCs in flight. */
  ConcurrentMap<RPCContext, Stopwatch> activeRpcs = new ConcurrentHashMap<>();
  private Ticks ticks;

  /** How long I've been alive */
  public Date startupTime = new Date();

  private Segmenter<?, Float> floatSegmenter;
  private Segmenter<?, Integer> integerSegmenter;

  /** Construct a plugin with custom Ticks and Segmenter implementations. */
  public StatsPlugin(Ticks ticks, Segmenter<?, Float> floatSegmenter, Segmenter<?, Integer> integerSegmenter) {
    this.floatSegmenter = floatSegmenter;
    this.integerSegmenter = integerSegmenter;
    this.ticks = ticks;
  }

  /**
   * Construct a plugin with default (system) ticks, and default histogram
   * segmentation.
   */
  public StatsPlugin() {
    this(Stopwatch.SYSTEM_TICKS, LATENCY_SEGMENTER, PAYLOAD_SEGMENTER);
  }

  /**
   * Helper to get the size of an RPC payload.
   */
  private int getPayloadSize(List<ByteBuffer> payload) {
    if (payload == null) {
      return 0;
    }

    int size = 0;
    for (ByteBuffer bb : payload) {
      size = size + bb.limit();
    }

    return size;
  }

  @Override
  public void serverReceiveRequest(RPCContext context) {
    Stopwatch t = new Stopwatch(ticks);
    t.start();
    this.activeRpcs.put(context, t);

    synchronized (receivePayloads) {
      IntegerHistogram<?> h = receivePayloads.get(context.getMessage());
      if (h == null) {
        h = createNewIntegerHistogram();
        receivePayloads.put(context.getMessage(), h);
      }
      h.add(getPayloadSize(context.getRequestPayload()));
    }
  }

  @Override
  public void serverSendResponse(RPCContext context) {
    Stopwatch t = this.activeRpcs.remove(context);
    t.stop();
    publish(context, t);

    synchronized (sendPayloads) {
      IntegerHistogram<?> h = sendPayloads.get(context.getMessage());
      if (h == null) {
        h = createNewIntegerHistogram();
        sendPayloads.put(context.getMessage(), h);
      }
      h.add(getPayloadSize(context.getResponsePayload()));
    }
  }

  @Override
  public void clientSendRequest(RPCContext context) {
    Stopwatch t = new Stopwatch(ticks);
    t.start();
    this.activeRpcs.put(context, t);

    synchronized (sendPayloads) {
      IntegerHistogram<?> h = sendPayloads.get(context.getMessage());
      if (h == null) {
        h = createNewIntegerHistogram();
        sendPayloads.put(context.getMessage(), h);
      }
      h.add(getPayloadSize(context.getRequestPayload()));
    }
  }

  @Override
  public void clientReceiveResponse(RPCContext context) {
    Stopwatch t = this.activeRpcs.remove(context);
    t.stop();
    publish(context, t);

    synchronized (receivePayloads) {
      IntegerHistogram<?> h = receivePayloads.get(context.getMessage());
      if (h == null) {
        h = createNewIntegerHistogram();
        receivePayloads.put(context.getMessage(), h);
      }
      h.add(getPayloadSize(context.getRequestPayload()));
    }
  }

  /** Adds timing to the histograms. */
  private void publish(RPCContext context, Stopwatch t) {
    Message message = context.getMessage();
    if (message == null)
      throw new IllegalArgumentException();
    synchronized (methodTimings) {
      FloatHistogram<?> h = methodTimings.get(context.getMessage());
      if (h == null) {
        h = createNewFloatHistogram();
        methodTimings.put(context.getMessage(), h);
      }
      h.add(nanosToMillis(t.elapsedNanos()));
    }
  }

  private FloatHistogram<?> createNewFloatHistogram() {
    return new FloatHistogram<>(floatSegmenter);
  }

  private IntegerHistogram<?> createNewIntegerHistogram() {
    return new IntegerHistogram<>(integerSegmenter);
  }

  /** Converts nanoseconds to milliseconds. */
  static float nanosToMillis(long elapsedNanos) {
    return elapsedNanos / 1000000.0f;
  }
}
