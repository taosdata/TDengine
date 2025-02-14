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

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import javax.servlet.ServletException;
import javax.servlet.UnavailableException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import org.apache.avro.Protocol.Message;
import org.apache.avro.ipc.RPCContext;

/**
 * Exposes information provided by a StatsPlugin as a web page.
 *
 * This class follows the same synchronization conventions as StatsPlugin, to
 * avoid requiring StatsPlugin to serve a copy of the data.
 */
public class StatsServlet extends HttpServlet {
  private final StatsPlugin statsPlugin;
  private VelocityEngine velocityEngine;
  private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss");

  public StatsServlet(StatsPlugin statsPlugin) throws UnavailableException {
    this.statsPlugin = statsPlugin;
    this.velocityEngine = new VelocityEngine();

    // These two properties tell Velocity to use its own classpath-based loader
    velocityEngine.addProperty("resource.loader", "class");
    velocityEngine.addProperty("class.resource.loader.class",
        "org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader");

    velocityEngine.setProperty("runtime.references.strict", true);
    String logChuteName = "org.apache.velocity.runtime.log.NullLogChute";
    velocityEngine.setProperty("runtime.log.logsystem.class", logChuteName);
  }

  /*
   * Helper class to store per-message data which is passed to templates.
   *
   * The template expects a list of charts, each of which is parameterized by map
   * key-value string attributes.
   */
  public class RenderableMessage { // Velocity brakes if not public
    public String name;
    public int numCalls;
    public ArrayList<HashMap<String, String>> charts;

    public RenderableMessage(String name) {
      this.name = name;
      this.charts = new ArrayList<>();
    }

    public ArrayList<HashMap<String, String>> getCharts() {
      return this.charts;
    }

    public String getname() {
      return this.name;
    }

    public int getNumCalls() {
      return this.numCalls;
    }
  }

  /*
   * Surround each string in an array with quotation marks and escape existing
   * quotes.
   *
   * This is useful when we have an array of strings that we want to turn into a
   * javascript array declaration.
   */
  protected static List<String> escapeStringArray(List<String> input) {
    for (int i = 0; i < input.size(); i++) {
      input.set(i, "\"" + input.get(i).replace("\"", "\\\"") + "\"");
    }
    return input;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    resp.setContentType("text/html");
    try {
      writeStats(resp.getWriter());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void writeStats(Writer w) throws IOException {
    VelocityContext context = new VelocityContext();
    context.put("title", "Avro RPC Stats");

    ArrayList<String> rpcs = new ArrayList<>(); // in flight rpcs

    ArrayList<RenderableMessage> messages = new ArrayList<>();

    for (Entry<RPCContext, Stopwatch> rpc : this.statsPlugin.activeRpcs.entrySet()) {
      rpcs.add(renderActiveRpc(rpc.getKey(), rpc.getValue()));
    }

    // Get set of all seen messages
    Set<Message> keys = null;
    synchronized (this.statsPlugin.methodTimings) {
      keys = this.statsPlugin.methodTimings.keySet();

      for (Message m : keys) {
        messages.add(renderMethod(m));
      }
    }

    context.put("inFlightRpcs", rpcs);
    context.put("messages", messages);

    context.put("currTime", FORMATTER.format(new Date()));
    context.put("startupTime", FORMATTER.format(statsPlugin.startupTime));

    Template t;
    try {
      t = velocityEngine.getTemplate("org/apache/avro/ipc/stats/templates/statsview.vm");
    } catch (Exception e) {
      throw new IOException();
    }
    t.merge(context, w);
  }

  private String renderActiveRpc(RPCContext rpc, Stopwatch stopwatch) throws IOException {
    String out = new String();
    out += rpc.getMessage().getName() + ": " + formatMillis(StatsPlugin.nanosToMillis(stopwatch.elapsedNanos()));
    return out;
  }

  private RenderableMessage renderMethod(Message message) {
    RenderableMessage out = new RenderableMessage(message.getName());

    synchronized (this.statsPlugin.methodTimings) {
      FloatHistogram<?> hist = this.statsPlugin.methodTimings.get(message);
      out.numCalls = hist.getCount();

      HashMap<String, String> latencyBar = new HashMap<>();
      // Fill in chart attributes for velocity
      latencyBar.put("type", "bar");
      latencyBar.put("title", "All-Time Latency");
      latencyBar.put("units", "ms");
      latencyBar.put("numCalls", Integer.toString(hist.getCount()));
      latencyBar.put("avg", Float.toString(hist.getMean()));
      latencyBar.put("stdDev", Float.toString(hist.getUnbiasedStdDev()));
      latencyBar.put("labelStr", Arrays.toString(hist.getSegmenter().getBoundaryLabels().toArray()));
      latencyBar.put("boundaryStr",
          Arrays.toString(escapeStringArray(hist.getSegmenter().getBucketLabels()).toArray()));
      latencyBar.put("dataStr", Arrays.toString(hist.getHistogram()));
      out.charts.add(latencyBar);

      HashMap<String, String> latencyDot = new HashMap<>();
      latencyDot.put("title", "Latency");
      latencyDot.put("type", "dot");
      latencyDot.put("dataStr", Arrays.toString(hist.getRecentAdditions().toArray()));
      out.charts.add(latencyDot);
    }

    synchronized (this.statsPlugin.sendPayloads) {
      IntegerHistogram<?> hist = this.statsPlugin.sendPayloads.get(message);
      HashMap<String, String> latencyBar = new HashMap<>();
      // Fill in chart attributes for velocity
      latencyBar.put("type", "bar");
      latencyBar.put("title", "All-Time Send Payload");
      latencyBar.put("units", "ms");
      latencyBar.put("numCalls", Integer.toString(hist.getCount()));
      latencyBar.put("avg", Float.toString(hist.getMean()));
      latencyBar.put("stdDev", Float.toString(hist.getUnbiasedStdDev()));
      latencyBar.put("labelStr", Arrays.toString(hist.getSegmenter().getBoundaryLabels().toArray()));
      latencyBar.put("boundaryStr",
          Arrays.toString(escapeStringArray(hist.getSegmenter().getBucketLabels()).toArray()));
      latencyBar.put("dataStr", Arrays.toString(hist.getHistogram()));
      out.charts.add(latencyBar);

      HashMap<String, String> latencyDot = new HashMap<>();
      latencyDot.put("title", "Send Payload");
      latencyDot.put("type", "dot");
      latencyDot.put("dataStr", Arrays.toString(hist.getRecentAdditions().toArray()));
      out.charts.add(latencyDot);
    }

    synchronized (this.statsPlugin.receivePayloads) {
      IntegerHistogram<?> hist = this.statsPlugin.receivePayloads.get(message);
      HashMap<String, String> latencyBar = new HashMap<>();
      // Fill in chart attributes for velocity
      latencyBar.put("type", "bar");
      latencyBar.put("title", "All-Time Receive Payload");
      latencyBar.put("units", "ms");
      latencyBar.put("numCalls", Integer.toString(hist.getCount()));
      latencyBar.put("avg", Float.toString(hist.getMean()));
      latencyBar.put("stdDev", Float.toString(hist.getUnbiasedStdDev()));
      latencyBar.put("labelStr", Arrays.toString(hist.getSegmenter().getBoundaryLabels().toArray()));
      latencyBar.put("boundaryStr",
          Arrays.toString(escapeStringArray(hist.getSegmenter().getBucketLabels()).toArray()));
      latencyBar.put("dataStr", Arrays.toString(hist.getHistogram()));
      out.charts.add(latencyBar);

      HashMap<String, String> latencyDot = new HashMap<>();
      latencyDot.put("title", "Recv Payload");
      latencyDot.put("type", "dot");
      latencyDot.put("dataStr", Arrays.toString(hist.getRecentAdditions().toArray()));
      out.charts.add(latencyDot);
    }

    return out;
  }

  private CharSequence formatMillis(float millis) {
    return String.format("%.0fms", millis);
  }
}
