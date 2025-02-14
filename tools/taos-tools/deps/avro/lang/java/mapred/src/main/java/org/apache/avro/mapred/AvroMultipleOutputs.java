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
package org.apache.avro.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Collections;

import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.avro.Schema;

import org.apache.hadoop.io.NullWritable;

/**
 * The AvroMultipleOutputs class simplifies writing Avro output data to multiple
 * outputs
 *
 * <p>
 * Case one: writing to additional outputs other than the job default output.
 *
 * Each additional output, or named output, may be configured with its own
 * <code>Schema</code> and <code>OutputFormat</code>. A named output can be a
 * single file or a multi file. The later is refered as a multi named output
 * which is an unbound set of files all sharing the same <code>Schema</code>.
 * </p>
 * <p>
 * Case two: to write data to different files provided by user
 * </p>
 *
 * <p>
 * AvroMultipleOutputs supports counters, by default they are disabled. The
 * counters group is the {@link AvroMultipleOutputs} class name. The names of
 * the counters are the same as the output name. These count the number of
 * records written to each output name. For multi named outputs the name of the
 * counter is the concatenation of the named output, and underscore '_' and the
 * multiname.
 * </p>
 *
 * Usage pattern for job submission:
 * 
 * <pre>
 *
 * JobConf job = new JobConf();
 *
 * FileInputFormat.setInputPath(job, inDir);
 * FileOutputFormat.setOutputPath(job, outDir);
 *
 * job.setMapperClass(MyAvroMapper.class);
 * job.setReducerClass(HadoopReducer.class);
 * job.set("avro.reducer",MyAvroReducer.class);
 * ...
 *
 * Schema schema;
 * ...
 * // Defines additional single output 'avro1' for the job
 * AvroMultipleOutputs.addNamedOutput(job, "avro1", AvroOutputFormat.class,
 * schema);
 *
 * // Defines additional output 'avro2' with different schema for the job
 * AvroMultipleOutputs.addNamedOutput(job, "avro2",
 *   AvroOutputFormat.class,
 *   null); // if Schema is specified as null then the default output schema is used
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
 *   AvroReducer&lt;K, V, OUT&gt; {
 * private MultipleOutputs amos;
 *
 *
 * public void configure(JobConf conf) {
 * ...
 * amos = new AvroMultipleOutputs(conf);
 * }
 *
 * public void reduce(K, Iterator&lt;V&gt; values,
 * AvroCollector&lt;OUT&gt;, Reporter reporter)
 * throws IOException {
 * ...
 * amos.collect("avro1", reporter,datum);
 * amos.getCollector("avro2", "A", reporter).collect(datum);
 * amos.collect("avro1",reporter,schema,datum,"testavrofile");// this create a file testavrofile and writes data with schema "schema" into it
 *                                                            and uses other values from namedoutput "avro1" like outputclass etc.
 * amos.collect("avro1",reporter,schema,datum,"testavrofile1");
 * ...
 * }
 *
 * public void close() throws IOException {
 * amos.close();
 * ...
 * }
 *
 * }
 * </pre>
 */

public class AvroMultipleOutputs {

  private static final String NAMED_OUTPUTS = "mo.namedOutputs";

  private static final String MO_PREFIX = "mo.namedOutput.";

  private static final String FORMAT = ".avro";
  private static final String MULTI = ".multi";

  private static final String COUNTERS_ENABLED = "mo.counters";

  /**
   * Counters group used by the counters of MultipleOutputs.
   */
  private static final String COUNTERS_GROUP = AvroMultipleOutputs.class.getName();

  /**
   * Checks if a named output is alreadyDefined or not.
   *
   * @param conf           job conf
   * @param namedOutput    named output names
   * @param alreadyDefined whether the existence/non-existence of the named output
   *                       is to be checked
   * @throws IllegalArgumentException if the output name is alreadyDefined or not
   *                                  depending on the value of the
   *                                  'alreadyDefined' parameter
   */
  private static void checkNamedOutput(JobConf conf, String namedOutput, boolean alreadyDefined) {
    List<String> definedChannels = getNamedOutputsList(conf);
    if (alreadyDefined && definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput + "' already alreadyDefined");
    } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput + "' not defined");
    }
  }

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
   * Checks if a named output name is valid.
   *
   * @param namedOutput named output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkNamedOutputName(String namedOutput) {
    checkTokenName(namedOutput);
    // name cannot be the name used for the default output
    if (namedOutput.equals("part")) {
      throw new IllegalArgumentException("Named output name cannot be 'part'");
    }
  }

  /**
   * Returns list of channel names.
   *
   * @param conf job conf
   * @return List of channel Names
   */
  public static List<String> getNamedOutputsList(JobConf conf) {
    List<String> names = new ArrayList<>();
    StringTokenizer st = new StringTokenizer(conf.get(NAMED_OUTPUTS, ""), " ");
    while (st.hasMoreTokens()) {
      names.add(st.nextToken());
    }
    return names;
  }

  /**
   * Returns if a named output is multiple.
   *
   * @param conf        job conf
   * @param namedOutput named output
   * @return <code>true</code> if the name output is multi, <code>false</code> if
   *         it is single. If the name output is not defined it returns
   *         <code>false</code>
   */
  public static boolean isMultiNamedOutput(JobConf conf, String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getBoolean(MO_PREFIX + namedOutput + MULTI, false);
  }

  /**
   * Returns the named output OutputFormat.
   *
   * @param conf        job conf
   * @param namedOutput named output
   * @return namedOutput OutputFormat
   */
  public static Class<? extends OutputFormat> getNamedOutputFormatClass(JobConf conf, String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getClass(MO_PREFIX + namedOutput + FORMAT, null, OutputFormat.class);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   *
   * @param conf              job conf to add the named output
   * @param namedOutput       named output name, it has to be a word, letters and
   *                          numbers only, cannot be the word 'part' as that is
   *                          reserved for the default output.
   * @param outputFormatClass OutputFormat class.
   * @param schema            Schema to used for this namedOutput
   */
  public static void addNamedOutput(JobConf conf, String namedOutput, Class<? extends OutputFormat> outputFormatClass,
      Schema schema) {
    addNamedOutput(conf, namedOutput, false, outputFormatClass, schema);
  }

  /**
   * Adds a multi named output for the job.
   * <p/>
   *
   * @param conf              job conf to add the named output
   * @param namedOutput       named output name, it has to be a word, letters and
   *                          numbers only, cannot be the word 'part' as that is
   *                          reserved for the default output.
   * @param outputFormatClass OutputFormat class.
   * @param schema            Schema to used for this namedOutput
   */
  public static void addMultiNamedOutput(JobConf conf, String namedOutput,
      Class<? extends OutputFormat> outputFormatClass, Schema schema) {
    addNamedOutput(conf, namedOutput, true, outputFormatClass, schema);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   *
   * @param conf              job conf to add the named output
   * @param namedOutput       named output name, it has to be a word, letters and
   *                          numbers only, cannot be the word 'part' as that is
   *                          reserved for the default output.
   * @param multi             indicates if the named output is multi
   * @param outputFormatClass OutputFormat class.
   * @param schema            Schema to used for this namedOutput
   */
  private static void addNamedOutput(JobConf conf, String namedOutput, boolean multi,
      Class<? extends OutputFormat> outputFormatClass, Schema schema) {
    checkNamedOutputName(namedOutput);
    checkNamedOutput(conf, namedOutput, true);
    if (schema != null)
      conf.set(MO_PREFIX + namedOutput + ".schema", schema.toString());
    conf.set(NAMED_OUTPUTS, conf.get(NAMED_OUTPUTS, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass, OutputFormat.class);
    conf.setBoolean(MO_PREFIX + namedOutput + MULTI, multi);
  }

  /**
   * Enables or disables counters for the named outputs.
   * <p/>
   * By default these counters are disabled.
   * <p/>
   * MultipleOutputs supports counters, by default the are disabled. The counters
   * group is the {@link AvroMultipleOutputs} class name.
   * </p>
   * The names of the counters are the same as the named outputs. For multi named
   * outputs the name of the counter is the concatenation of the named output, and
   * underscore '_' and the multiname.
   *
   * @param conf    job conf to enableadd the named output.
   * @param enabled indicates if the counters will be enabled or not.
   */
  public static void setCountersEnabled(JobConf conf, boolean enabled) {
    conf.setBoolean(COUNTERS_ENABLED, enabled);
  }

  /**
   * Returns if the counters for the named outputs are enabled or not.
   * <p/>
   * By default these counters are disabled.
   * <p/>
   * MultipleOutputs supports counters, by default the are disabled. The counters
   * group is the {@link AvroMultipleOutputs} class name.
   * </p>
   * The names of the counters are the same as the named outputs. For multi named
   * outputs the name of the counter is the concatenation of the named output, and
   * underscore '_' and the multiname.
   *
   *
   * @param conf job conf to enableadd the named output.
   * @return TRUE if the counters are enabled, FALSE if they are disabled.
   */
  public static boolean getCountersEnabled(JobConf conf) {
    return conf.getBoolean(COUNTERS_ENABLED, false);
  }

  // instance code, to be used from Mapper/Reducer code

  private JobConf conf;
  private OutputFormat outputFormat;
  private Set<String> namedOutputs;
  private Map<String, RecordWriter> recordWriters;
  private boolean countersEnabled;

  /**
   * Creates and initializes multiple named outputs support, it should be
   * instantiated in the Mapper/Reducer configure method.
   *
   * @param job the job configuration object
   */
  public AvroMultipleOutputs(JobConf job) {
    this.conf = job;
    outputFormat = new InternalFileOutputFormat();
    namedOutputs = Collections.unmodifiableSet(new HashSet<>(AvroMultipleOutputs.getNamedOutputsList(job)));
    recordWriters = new HashMap<>();
    countersEnabled = getCountersEnabled(job);
  }

  /**
   * Returns iterator with the defined name outputs.
   *
   * @return iterator with the defined named outputs
   */
  public Iterator<String> getNamedOutputs() {
    return namedOutputs.iterator();
  }

  // by being synchronized MultipleOutputTask can be use with a
  // MultithreaderMapRunner.
  private synchronized RecordWriter getRecordWriter(String namedOutput, String baseFileName, final Reporter reporter,
      Schema schema) throws IOException {
    RecordWriter writer = recordWriters.get(baseFileName);
    if (writer == null) {
      if (countersEnabled && reporter == null) {
        throw new IllegalArgumentException("Counters are enabled, Reporter cannot be NULL");
      }
      if (schema != null)
        conf.set(MO_PREFIX + namedOutput + ".schema", schema.toString());
      JobConf jobConf = new JobConf(conf);
      jobConf.set(InternalFileOutputFormat.CONFIG_NAMED_OUTPUT, namedOutput);
      FileSystem fs = FileSystem.get(conf);
      writer = outputFormat.getRecordWriter(fs, jobConf, baseFileName, reporter);

      if (countersEnabled) {
        if (reporter == null) {
          throw new IllegalArgumentException("Counters are enabled, Reporter cannot be NULL");
        }
        writer = new RecordWriterWithCounter(writer, baseFileName, reporter);
      }
      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }

  private static class RecordWriterWithCounter implements RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private Reporter reporter;

    public RecordWriterWithCounter(RecordWriter writer, String counterName, Reporter reporter) {
      this.writer = writer;
      this.counterName = counterName;
      this.reporter = reporter;
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public void write(Object key, Object value) throws IOException {
      reporter.incrCounter(COUNTERS_GROUP, counterName, 1);
      writer.write(key, value);
    }

    @Override
    public void close(Reporter reporter) throws IOException {
      writer.close(reporter);
    }
  }

  /**
   * Output Collector for the default schema.
   * <p/>
   *
   * @param namedOutput the named output name
   * @param reporter    the reporter
   * @param datum       output data
   * @throws IOException thrown if output collector could not be created
   */
  public void collect(String namedOutput, Reporter reporter, Object datum) throws IOException {
    getCollector(namedOutput, reporter).collect(datum);
  }

  /**
   * OutputCollector with custom schema.
   * <p/>
   *
   * @param namedOutput the named output name (this will the output file name)
   * @param reporter    the reporter
   * @param datum       output data
   * @param schema      schema to use for this output
   * @throws IOException thrown if output collector could not be created
   */
  public void collect(String namedOutput, Reporter reporter, Schema schema, Object datum) throws IOException {
    getCollector(namedOutput, reporter, schema).collect(datum);
  }

  /**
   * OutputCollector with custom schema and file name.
   * <p/>
   *
   * @param namedOutput    the named output name
   * @param reporter       the reporter
   * @param baseOutputPath outputfile name to use.
   * @param datum          output data
   * @param schema         schema to use for this output
   * @throws IOException thrown if output collector could not be created
   */
  public void collect(String namedOutput, Reporter reporter, Schema schema, Object datum, String baseOutputPath)
      throws IOException {
    getCollector(namedOutput, null, reporter, baseOutputPath, schema).collect(datum);
  }

  /**
   * Gets the output collector for a named output.
   * <p/>
   *
   * @param namedOutput the named output name
   * @param reporter    the reporter
   * @return the output collector for the given named output
   * @throws IOException thrown if output collector could not be created
   * @deprecated Use {@link #collect} method for collecting output
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public AvroCollector getCollector(String namedOutput, Reporter reporter) throws IOException {
    return getCollector(namedOutput, null, reporter, namedOutput, null);
  }

  @SuppressWarnings("rawtypes")
  private AvroCollector getCollector(String namedOutput, Reporter reporter, Schema schema) throws IOException {
    return getCollector(namedOutput, null, reporter, namedOutput, schema);
  }

  /**
   * Gets the output collector for a named output.
   * <p/>
   *
   * @param namedOutput the named output name
   * @param reporter    the reporter
   * @param multiName   the multiname
   * @return the output collector for the given named output
   * @throws IOException thrown if output collector could not be created
   */
  @SuppressWarnings("rawtypes")
  public AvroCollector getCollector(String namedOutput, String multiName, Reporter reporter) throws IOException {
    return getCollector(namedOutput, multiName, reporter, namedOutput, null);
  }

  /**
   * Gets the output collector for a multi named output.
   * <p/>
   *
   * @param namedOutput the named output name
   * @param multiName   the multi name part
   * @param reporter    the reporter
   * @return the output collector for the given named output
   * @throws IOException thrown if output collector could not be created
   */
  @SuppressWarnings({ "unchecked" })
  private AvroCollector getCollector(String namedOutput, String multiName, Reporter reporter, String baseOutputFileName,
      Schema schema) throws IOException {

    checkNamedOutputName(namedOutput);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" + namedOutput + "'");
    }
    boolean multi = isMultiNamedOutput(conf, namedOutput);

    if (!multi && multiName != null) {
      throw new IllegalArgumentException("Name output '" + namedOutput + "' has not been defined as multi");
    }
    if (multi) {
      checkTokenName(multiName);
    }

    String baseFileName = (multi) ? namedOutput + "_" + multiName : baseOutputFileName;

    final RecordWriter writer = getRecordWriter(namedOutput, baseFileName, reporter, schema);

    return new AvroCollector() {

      @SuppressWarnings({ "unchecked" })
      @Override
      public void collect(Object key) throws IOException {
        AvroWrapper wrapper = new AvroWrapper(key);
        writer.write(wrapper, NullWritable.get());
      }

    };
  }

  /**
   * Closes all the opened named outputs.
   * <p/>
   * If overriden subclasses must invoke <code>super.close()</code> at the end of
   * their <code>close()</code>
   *
   * @throws java.io.IOException thrown if any of the MultipleOutput files could
   *                             not be closed properly.
   */
  public void close() throws IOException {
    for (RecordWriter writer : recordWriters.values()) {
      writer.close(null);
    }
  }

  private static class InternalFileOutputFormat extends FileOutputFormat<Object, Object> {
    public static final String CONFIG_NAMED_OUTPUT = "mo.config.namedOutput";

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public RecordWriter<Object, Object> getRecordWriter(FileSystem fs, JobConf job, String baseFileName,
        Progressable arg3) throws IOException {
      String nameOutput = job.get(CONFIG_NAMED_OUTPUT, null);
      String fileName = getUniqueName(job, baseFileName);
      Schema schema = null;
      String schemastr = job.get(MO_PREFIX + nameOutput + ".schema", null);
      if (schemastr != null)
        schema = Schema.parse(schemastr);
      JobConf outputConf = new JobConf(job);
      outputConf.setOutputFormat(getNamedOutputFormatClass(job, nameOutput));
      boolean isMapOnly = job.getNumReduceTasks() == 0;
      if (schema != null) {
        if (isMapOnly)
          AvroJob.setMapOutputSchema(outputConf, schema);
        else
          AvroJob.setOutputSchema(outputConf, schema);
      }
      OutputFormat outputFormat = outputConf.getOutputFormat();
      return outputFormat.getRecordWriter(fs, outputConf, fileName, arg3);
    }
  }
}
