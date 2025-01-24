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
package org.apache.avro.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The AvroMultipleOutputs class simplifies writing Avro output data to multiple
 * outputs
 *
 * <p>
 * Case one: writing to additional outputs other than the job default output.
 *
 * Each additional output, or named output, may be configured with its own
 * <code>Schema</code> and <code>OutputFormat</code>.
 * </p>
 * <p>
 * Case two: to write data to different files provided by user
 * </p>
 *
 * <p>
 * AvroMultipleOutputs supports counters, by default they are disabled. The
 * counters group is the {@link AvroMultipleOutputs} class name. The names of
 * the counters are the same as the output name. These count the number of
 * records written to each output name.
 * </p>
 *
 * Usage pattern for job submission:
 * 
 * <pre>
 *
 * Job job = Job.getInstance();
 *
 * FileInputFormat.setInputPath(job, inDir);
 * FileOutputFormat.setOutputPath(job, outDir);
 *
 * job.setMapperClass(MyAvroMapper.class);
 * job.setReducerClass(MyAvroReducer.class);
 * ...
 *
 * Schema schema;
 * ...
 * // Defines additional single output 'avro1' for the job
 * AvroMultipleOutputs.addNamedOutput(job, "avro1", AvroKeyValueOutputFormat.class,
 * keyschema, valueSchema);  // valueSchema can be set to null if there only Key to be written
                                   to file in the RecordWriter
 *
 * // Defines additional output 'avro2' with different schema for the job
 * AvroMultipleOutputs.addNamedOutput(job, "avro2",
 *   AvroKeyOutputFormat.class,
 *   schema,null);
 * ...
 *
 * job.waitForCompletion(true);
 * ...
 * </pre>
 * <p>
 * Usage in Reducer:
 * 
 * <pre>
 *
 * public class MyAvroReducer extends
 *   Reducer&lt;K, V, T, NullWritable&gt; {
 * private MultipleOutputs amos;
 *
 *
 * public void setup(Context context) {
 * ...
 * amos = new AvroMultipleOutputs(context);
 * }
 *
 * public void reduce(K, Iterator&lt;V&gt; values,Context context)
 * throws IOException {
 * ...
 * amos.write("avro1",datum,NullWritable.get());
 * amos.write("avro2",datum,NullWritable.get());
 * amos.getCollector("avro3",datum); // here the value is taken as NullWritable
 * ...
 * }
 *
 * public void cleanup(Context context) throws IOException {
 * amos.close();
 * ...
 * }
 *
 * }
 * </pre>
 */

public class AvroMultipleOutputs {

  private static final String MULTIPLE_OUTPUTS = "avro.mapreduce.multipleoutputs";

  private static final String MO_PREFIX = "avro.mapreduce.multipleoutputs.namedOutput.";

  private static final String FORMAT = ".format";
  private static final String COUNTERS_ENABLED = "avro.mapreduce.multipleoutputs.counters";

  /**
   * Counters group used by the counters of MultipleOutputs.
   */
  private static final String COUNTERS_GROUP = AvroMultipleOutputs.class.getName();

  /**
   * Cache for the taskContexts
   */
  private Map<String, TaskAttemptContext> taskContexts = new HashMap<>();

  /**
   * Checks if a named output name is valid token.
   *
   * @param namedOutput named output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkTokenName(String namedOutput) {
    if (namedOutput == null || namedOutput.length() == 0) {
      throw new IllegalArgumentException("Name cannot be NULL or empty");
    }
    for (char ch : namedOutput.toCharArray()) {
      if ((ch >= 'A') && (ch <= 'Z')) {
        continue;
      }
      if ((ch >= 'a') && (ch <= 'z')) {
        continue;
      }
      if ((ch >= '0') && (ch <= '9')) {
        continue;
      }
      throw new IllegalArgumentException("Name cannot have a '" + ch + "' char");
    }
  }

  /**
   * Checks if output name is valid.
   *
   * name cannot be the name used for the default output
   * 
   * @param outputPath base output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkBaseOutputPath(String outputPath) {
    if (outputPath.equals("part")) {
      throw new IllegalArgumentException("output name cannot be 'part'");
    }
  }

  /**
   * Checks if a named output name is valid.
   *
   * @param namedOutput named output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkNamedOutputName(JobContext job, String namedOutput, boolean alreadyDefined) {
    checkTokenName(namedOutput);
    checkBaseOutputPath(namedOutput);
    List<String> definedChannels = getNamedOutputsList(job);
    if (alreadyDefined && definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput + "' already alreadyDefined");
    } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput + "' not defined");
    }
  }

  // Returns list of channel names.
  private static List<String> getNamedOutputsList(JobContext job) {
    List<String> names = new ArrayList<>();
    StringTokenizer st = new StringTokenizer(job.getConfiguration().get(MULTIPLE_OUTPUTS, ""), " ");
    while (st.hasMoreTokens()) {
      names.add(st.nextToken());
    }
    return names;
  }

  // Returns the named output OutputFormat.
  @SuppressWarnings("unchecked")
  private static Class<? extends OutputFormat<?, ?>> getNamedOutputFormatClass(JobContext job, String namedOutput) {
    return (Class<? extends OutputFormat<?, ?>>) job.getConfiguration().getClass(MO_PREFIX + namedOutput + FORMAT, null,
        OutputFormat.class);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   *
   * @param job               job to add the named output
   * @param namedOutput       named output name, it has to be a word, letters and
   *                          numbers only, cannot be the word 'part' as that is
   *                          reserved for the default output.
   * @param outputFormatClass OutputFormat class.
   * @param keySchema         Schema for the Key
   */
  @SuppressWarnings("unchecked")
  public static void addNamedOutput(Job job, String namedOutput, Class<? extends OutputFormat> outputFormatClass,
      Schema keySchema) {
    addNamedOutput(job, namedOutput, outputFormatClass, keySchema, null);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   *
   * @param job               job to add the named output
   * @param namedOutput       named output name, it has to be a word, letters and
   *                          numbers only, cannot be the word 'part' as that is
   *                          reserved for the default output.
   * @param outputFormatClass OutputFormat class.
   * @param keySchema         Schema for the Key
   * @param valueSchema       Schema for the Value (used in case of
   *                          AvroKeyValueOutputFormat or null)
   */
  @SuppressWarnings("unchecked")
  public static void addNamedOutput(Job job, String namedOutput, Class<? extends OutputFormat> outputFormatClass,
      Schema keySchema, Schema valueSchema) {
    checkNamedOutputName(job, namedOutput, true);
    Configuration conf = job.getConfiguration();
    conf.set(MULTIPLE_OUTPUTS, conf.get(MULTIPLE_OUTPUTS, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass, OutputFormat.class);
    conf.set(MO_PREFIX + namedOutput + ".keyschema", keySchema.toString());
    if (valueSchema != null) {
      conf.set(MO_PREFIX + namedOutput + ".valueschema", valueSchema.toString());
    }
  }

  /**
   * Enables or disables counters for the named outputs.
   *
   * The counters group is the {@link AvroMultipleOutputs} class name. The names
   * of the counters are the same as the named outputs. These counters count the
   * number records written to each output name. By default these counters are
   * disabled.
   *
   * @param job     job to enable counters
   * @param enabled indicates if the counters will be enabled or not.
   */
  public static void setCountersEnabled(Job job, boolean enabled) {
    job.getConfiguration().setBoolean(COUNTERS_ENABLED, enabled);
  }

  /**
   * Returns if the counters for the named outputs are enabled or not. By default
   * these counters are disabled.
   *
   * @param job the job
   * @return TRUE if the counters are enabled, FALSE if they are disabled.
   */
  public static boolean getCountersEnabled(JobContext job) {
    return job.getConfiguration().getBoolean(COUNTERS_ENABLED, false);
  }

  /**
   * Wraps RecordWriter to increment counters.
   */
  @SuppressWarnings("unchecked")
  private static class RecordWriterWithCounter extends RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private TaskInputOutputContext context;

    public RecordWriterWithCounter(RecordWriter writer, String counterName, TaskInputOutputContext context) {
      this.writer = writer;
      this.counterName = counterName;
      this.context = context;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public void write(Object key, Object value) throws IOException, InterruptedException {
      context.getCounter(COUNTERS_GROUP, counterName).increment(1);
      writer.write(key, value);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      writer.close(context);
    }
  }

  // instance code, to be used from Mapper/Reducer code

  private TaskInputOutputContext<?, ?, ?, ?> context;
  private Set<String> namedOutputs;
  private Map<String, RecordWriter<?, ?>> recordWriters;
  private boolean countersEnabled;

  /**
   * Creates and initializes multiple outputs support, it should be instantiated
   * in the Mapper/Reducer setup method.
   *
   * @param context the TaskInputOutputContext object
   */
  public AvroMultipleOutputs(TaskInputOutputContext<?, ?, ?, ?> context) {
    this.context = context;
    namedOutputs = Collections.unmodifiableSet(new HashSet<>(AvroMultipleOutputs.getNamedOutputsList(context)));
    recordWriters = new HashMap<>();
    countersEnabled = getCountersEnabled(context);
  }

  /**
   * Write key and value to the namedOutput.
   *
   * Output path is a unique file generated for the namedOutput. For example,
   * {namedOutput}-(m|r)-{part-number}
   *
   * @param namedOutput the named output name
   * @param key         the key , value is NullWritable
   */
  @SuppressWarnings("unchecked")
  public void write(String namedOutput, Object key) throws IOException, InterruptedException {
    write(namedOutput, key, NullWritable.get(), namedOutput);
  }

  /**
   * Write key and value to the namedOutput.
   *
   * Output path is a unique file generated for the namedOutput. For example,
   * {namedOutput}-(m|r)-{part-number}
   *
   * @param namedOutput the named output name
   * @param key         the key
   * @param value       the value
   */
  @SuppressWarnings("unchecked")
  public void write(String namedOutput, Object key, Object value) throws IOException, InterruptedException {
    write(namedOutput, key, value, namedOutput);
  }

  /**
   * Write key and value to baseOutputPath using the namedOutput.
   *
   * @param namedOutput    the named output name
   * @param key            the key
   * @param value          the value
   * @param baseOutputPath base-output path to write the record to. Note:
   *                       Framework will generate unique filename for the
   *                       baseOutputPath
   */
  @SuppressWarnings("unchecked")
  public void write(String namedOutput, Object key, Object value, String baseOutputPath)
      throws IOException, InterruptedException {
    checkNamedOutputName(context, namedOutput, false);
    checkBaseOutputPath(baseOutputPath);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" + namedOutput + "'");
    }
    TaskAttemptContext taskContext = getContext(namedOutput);
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }

  /**
   * Write key value to an output file name.
   *
   * Gets the record writer from job's output format. Job's output format should
   * be a FileOutputFormat.
   *
   * @param key            the key
   * @param value          the value
   * @param baseOutputPath base-output path to write the record to. Note:
   *                       Framework will generate unique filename for the
   *                       baseOutputPath
   */
  public void write(Object key, Object value, String baseOutputPath) throws IOException, InterruptedException {
    write(key, value, null, null, baseOutputPath);
  }

  /**
   * Write key value to an output file name.
   *
   * Gets the record writer from job's output format. Job's output format should
   * be a FileOutputFormat.
   *
   * @param key            the key
   * @param value          the value
   * @param keySchema      keySchema to use
   * @param valSchema      ValueSchema to use
   * @param baseOutputPath base-output path to write the record to. Note:
   *                       Framework will generate unique filename for the
   *                       baseOutputPath
   */
  @SuppressWarnings("unchecked")
  public void write(Object key, Object value, Schema keySchema, Schema valSchema, String baseOutputPath)
      throws IOException, InterruptedException {
    checkBaseOutputPath(baseOutputPath);
    Job job = Job.getInstance(context.getConfiguration());
    setSchema(job, keySchema, valSchema);
    TaskAttemptContext taskContext = createTaskAttemptContext(job.getConfiguration(), context.getTaskAttemptID());
    getRecordWriter(taskContext, baseOutputPath).write(key, value);
  }

  /**
   *
   * Gets the record writer from job's output format. Job's output format should
   * be a FileOutputFormat.If the record writer implements Syncable then returns
   * the current position as a value that may be passed to
   * DataFileReader.seek(long) otherwise returns -1. Forces the end of the current
   * block, emitting a synchronization marker.
   *
   * @param namedOutput    the namedOutput
   * @param baseOutputPath base-output path to write the record to. Note:
   *                       Framework will generate unique filename for the
   *                       baseOutputPath
   */
  @SuppressWarnings("unchecked")
  public long sync(String namedOutput, String baseOutputPath) throws IOException, InterruptedException {
    checkNamedOutputName(context, namedOutput, false);
    checkBaseOutputPath(baseOutputPath);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" + namedOutput + "'");
    }
    TaskAttemptContext taskContext = getContext(namedOutput);
    RecordWriter recordWriter = getRecordWriter(taskContext, baseOutputPath);
    long position = -1;
    if (recordWriter instanceof Syncable) {
      Syncable syncableWriter = (Syncable) recordWriter;
      position = syncableWriter.sync();
    }
    return position;
  }

  // by being synchronized MultipleOutputTask can be use with a
  // MultithreadedMapper.
  @SuppressWarnings("unchecked")
  private synchronized RecordWriter getRecordWriter(TaskAttemptContext taskContext, String baseFileName)
      throws IOException, InterruptedException {

    // look for record-writer in the cache
    RecordWriter writer = recordWriters.get(baseFileName);

    // If not in cache, create a new one
    if (writer == null) {
      // get the record writer from context output format
      // FileOutputFormat.setOutputName(taskContext, baseFileName);
      taskContext.getConfiguration().set("avro.mo.config.namedOutput", baseFileName);
      try {
        writer = ReflectionUtils.newInstance(taskContext.getOutputFormatClass(), taskContext.getConfiguration())
            .getRecordWriter(taskContext);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }

      // if counters are enabled, wrap the writer with context
      // to increment counters
      if (countersEnabled) {
        writer = new RecordWriterWithCounter(writer, baseFileName, context);
      }

      // add the record-writer to the cache
      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }

  private void setSchema(Job job, Schema keySchema, Schema valSchema) {

    boolean isMaponly = job.getNumReduceTasks() == 0;
    if (keySchema != null) {
      if (isMaponly)
        AvroJob.setMapOutputKeySchema(job, keySchema);
      else
        AvroJob.setOutputKeySchema(job, keySchema);
    }
    if (valSchema != null) {
      if (isMaponly)
        AvroJob.setMapOutputValueSchema(job, valSchema);
      else
        AvroJob.setOutputValueSchema(job, valSchema);
    }

  }

  // Create a taskAttemptContext for the named output with
  // output format and output key/value types put in the context
  @SuppressWarnings("deprecation")
  private TaskAttemptContext getContext(String nameOutput) throws IOException {

    TaskAttemptContext taskContext = taskContexts.get(nameOutput);

    if (taskContext != null) {
      return taskContext;
    }

    // The following trick leverages the instantiation of a record writer via
    // the job thus supporting arbitrary output formats.
    Job job = new Job(context.getConfiguration());
    job.setOutputFormatClass(getNamedOutputFormatClass(context, nameOutput));
    Schema keySchema = null, valSchema = null;
    if (job.getConfiguration().get(MO_PREFIX + nameOutput + ".keyschema", null) != null)
      keySchema = Schema.parse(job.getConfiguration().get(MO_PREFIX + nameOutput + ".keyschema"));
    if (job.getConfiguration().get(MO_PREFIX + nameOutput + ".valueschema", null) != null)
      valSchema = Schema.parse(job.getConfiguration().get(MO_PREFIX + nameOutput + ".valueschema"));
    setSchema(job, keySchema, valSchema);
    taskContext = createTaskAttemptContext(job.getConfiguration(), context.getTaskAttemptID());

    taskContexts.put(nameOutput, taskContext);

    return taskContext;
  }

  private TaskAttemptContext createTaskAttemptContext(Configuration conf, TaskAttemptID taskId) {
    // Use reflection since the context types changed incompatibly between 1.0
    // and 2.0.
    try {
      Class<?> c = getTaskAttemptContextClass();
      Constructor<?> cons = c.getConstructor(Configuration.class, TaskAttemptID.class);
      return (TaskAttemptContext) cons.newInstance(conf, taskId);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private Class<?> getTaskAttemptContextClass() {
    try {
      return Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
    } catch (Exception e) {
      try {
        return Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext");
      } catch (Exception ex) {
        throw new IllegalStateException(ex);
      }
    }
  }

  /**
   * Closes all the opened outputs.
   *
   * This should be called from cleanup method of map/reduce task. If overridden
   * subclasses must invoke <code>super.close()</code> at the end of their
   * <code>close()</code>
   *
   */
  @SuppressWarnings("unchecked")
  public void close() throws IOException, InterruptedException {
    for (RecordWriter writer : recordWriters.values()) {
      writer.close(context);
    }
  }
}
