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
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.reflect.ReflectDatumReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class TestAvroMultipleInputs {

  @Rule
  public TemporaryFolder OUTPUT_DIR = new TemporaryFolder();

  @Rule
  public TemporaryFolder INPUT_DIR_1 = new TemporaryFolder();

  @Rule
  public TemporaryFolder INPUT_DIR_2 = new TemporaryFolder();

  /**
   * The input-1 record.
   */
  public static class NamesRecord {
    private int id = -1;
    private CharSequence name = "";

    public NamesRecord() {
    }

    public NamesRecord(int id, CharSequence name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public String toString() {
      return id + "\t" + name;
    }
  }

  /**
   * The input-2 record.
   */
  public static class BalancesRecord {
    private int id = -1;
    private long balance = 0L;

    public BalancesRecord() {
    }

    public BalancesRecord(int id, long balance) {
      this.id = id;
      this.balance = balance;
    }

    @Override
    public String toString() {
      return id + "\t" + balance;
    }
  }

  /**
   * The map output key record.
   */
  public static class KeyRecord {
    private int id = -1;

    public KeyRecord() {
    }

    public KeyRecord(int id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return ((Integer) id).toString();
    }
  }

  /**
   * The common map output value record. Carries a tag specifying what source
   * record type was.
   */
  public static class JoinableRecord {
    private int id = -1;
    private CharSequence name = "";
    private long balance = 0L;
    private CharSequence recType = "";

    public JoinableRecord() {
    }

    public JoinableRecord(CharSequence recType, int id, CharSequence name, long balance) {
      this.id = id;
      this.recType = recType;
      this.name = name;
      this.balance = balance;
    }

    @Override
    public String toString() {
      return recType.toString();
    }
  }

  /**
   * The output, combined record.
   */
  public static class CompleteRecord {
    private int id = -1;
    private CharSequence name = "";
    private long balance = 0L;

    public CompleteRecord() {
    }

    public CompleteRecord(int id, CharSequence name, long balance) {
      this.name = name;
      this.id = id;
      this.balance = balance;
    }

    void setId(int id) {
      this.id = id;
    }

    void setName(CharSequence name) {
      this.name = name;
    }

    void setBalance(long balance) {
      this.balance = balance;
    }

    @Override
    public String toString() {
      return id + "\t" + name + "\t" + balance;
    }
  }

  public static class NamesMapImpl extends AvroMapper<NamesRecord, Pair<KeyRecord, JoinableRecord>> {

    @Override
    public void map(NamesRecord nameRecord, AvroCollector<Pair<KeyRecord, JoinableRecord>> collector, Reporter reporter)
        throws IOException {
      collector.collect(new Pair<>(new KeyRecord(nameRecord.id),
          new JoinableRecord(nameRecord.getClass().getName(), nameRecord.id, nameRecord.name, -1L)));
    }

  }

  public static class BalancesMapImpl extends AvroMapper<BalancesRecord, Pair<KeyRecord, JoinableRecord>> {

    @Override
    public void map(BalancesRecord balanceRecord, AvroCollector<Pair<KeyRecord, JoinableRecord>> collector,
        Reporter reporter) throws IOException {
      collector.collect(new Pair<>(new KeyRecord(balanceRecord.id),
          new JoinableRecord(balanceRecord.getClass().getName(), balanceRecord.id, "", balanceRecord.balance)));
    }

  }

  public static class ReduceImpl extends AvroReducer<KeyRecord, JoinableRecord, CompleteRecord> {

    @Override
    public void reduce(KeyRecord ID, Iterable<JoinableRecord> joinables, AvroCollector<CompleteRecord> collector,
        Reporter reporter) throws IOException {
      CompleteRecord rec = new CompleteRecord();
      for (JoinableRecord joinable : joinables) {
        rec.setId(joinable.id);
        if (joinable.recType.toString().contains("NamesRecord")) {
          rec.setName(joinable.name);
        } else {
          rec.setBalance(joinable.balance);
        }
      }
      collector.collect(rec);
    }

  }

  @Test
  public void testJob() throws Exception {
    JobConf job = new JobConf();
    Path inputPath1 = new Path(INPUT_DIR_1.getRoot().getPath());
    Path inputPath2 = new Path(INPUT_DIR_2.getRoot().getPath());
    Path outputPath = new Path(OUTPUT_DIR.getRoot().getPath());

    outputPath.getFileSystem(job).delete(outputPath, true);

    writeNamesFiles(new File(inputPath1.toUri().getPath()));
    writeBalancesFiles(new File(inputPath2.toUri().getPath()));

    job.setJobName("multiple-inputs-join");
    AvroMultipleInputs.addInputPath(job, inputPath1, NamesMapImpl.class,
        ReflectData.get().getSchema(NamesRecord.class));
    AvroMultipleInputs.addInputPath(job, inputPath2, BalancesMapImpl.class,
        ReflectData.get().getSchema(BalancesRecord.class));

    Schema keySchema = ReflectData.get().getSchema(KeyRecord.class);
    Schema valueSchema = ReflectData.get().getSchema(JoinableRecord.class);
    AvroJob.setMapOutputSchema(job, Pair.getPairSchema(keySchema, valueSchema));
    AvroJob.setOutputSchema(job, ReflectData.get().getSchema(CompleteRecord.class));

    AvroJob.setReducerClass(job, ReduceImpl.class);
    job.setNumReduceTasks(1);

    FileOutputFormat.setOutputPath(job, outputPath);

    AvroJob.setReflect(job);

    JobClient.runJob(job);

    validateCompleteFile(new File(OUTPUT_DIR.getRoot(), "part-00000.avro"));
  }

  /**
   * Writes a "names.avro" file with five sequential <id, name> pairs.
   */
  private void writeNamesFiles(File dir) throws IOException {
    DatumWriter<NamesRecord> writer = new ReflectDatumWriter<>();
    File namesFile = new File(dir + "/names.avro");
    try (DataFileWriter<NamesRecord> out = new DataFileWriter<>(writer)) {
      out.create(ReflectData.get().getSchema(NamesRecord.class), namesFile);
      for (int i = 0; i < 5; i++) {
        out.append(new NamesRecord(i, "record" + i));
      }
    }
  }

  /**
   * Writes a "balances.avro" file with five sequential <id, balance> pairs.
   */
  private void writeBalancesFiles(File dir) throws IOException {
    DatumWriter<BalancesRecord> writer = new ReflectDatumWriter<>();
    File namesFile = new File(dir + "/balances.avro");
    try (DataFileWriter<BalancesRecord> out = new DataFileWriter<>(writer)) {
      out.create(ReflectData.get().getSchema(BalancesRecord.class), namesFile);
      for (int i = 0; i < 5; i++) {
        out.append(new BalancesRecord(i, (long) i + 100));
      }
    }
  }

  private void validateCompleteFile(File file) throws Exception {
    DatumReader<CompleteRecord> reader = new ReflectDatumReader<>();
    int numRecs = 0;
    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
      try (DataFileStream<CompleteRecord> records = new DataFileStream<>(in, reader)) {
        for (CompleteRecord rec : records) {
          assertEquals(rec.id, numRecs);
          assertEquals(rec.balance - 100, rec.id);
          assertEquals(rec.name, "record" + rec.id);
          numRecs++;
        }
      }
    }
    assertEquals(5, numRecs);
  }

}
