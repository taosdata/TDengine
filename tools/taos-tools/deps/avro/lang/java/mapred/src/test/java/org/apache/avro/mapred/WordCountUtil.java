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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JobConf;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileStream;

public class WordCountUtil {

  public static final String[] LINES = new String[] { "the quick brown fox jumps over the lazy dog",
      "the cow jumps over the moon", "the rain in spain falls mainly on the plains" };

  public static final Map<String, Long> COUNTS = new TreeMap<>();
  static {
    for (String line : LINES) {
      StringTokenizer tokens = new StringTokenizer(line);
      while (tokens.hasMoreTokens()) {
        String word = tokens.nextToken();
        long count = COUNTS.getOrDefault(word, 0L);
        count++;
        COUNTS.put(word, count);
      }
    }
  }

  public static void writeLinesFile(String dir) throws IOException {
    writeLinesFile(new File(dir));
  }

  public static void writeLinesFile(File dir) throws IOException {
    DatumWriter<Utf8> writer = new GenericDatumWriter<>();
    try (DataFileWriter<Utf8> out = new DataFileWriter<>(writer)) {
      out.create(Schema.create(Schema.Type.STRING), dir);
      for (String line : LINES) {
        out.append(new Utf8(line));
      }
    }
  }

  public static void writeLinesBytesFile(String dir) throws IOException {
    writeLinesBytesFile(new File(dir));
  }

  public static void writeLinesBytesFile(File dir) throws IOException {
    FileUtil.fullyDelete(dir);
    File fileLines = new File(dir + "/lines.avro");
    fileLines.getParentFile().mkdirs();

    DatumWriter<ByteBuffer> writer = new GenericDatumWriter<>();
    try (DataFileWriter<ByteBuffer> out = new DataFileWriter<>(writer)) {
      out.create(Schema.create(Schema.Type.BYTES), fileLines);
      for (String line : LINES) {
        out.append(ByteBuffer.wrap(line.getBytes(StandardCharsets.UTF_8)));
      }
    }
  }

  public static void writeLinesTextFile(File dir) throws IOException {
    FileUtil.fullyDelete(dir);
    File fileLines = new File(dir, "lines.avro");
    fileLines.getParentFile().mkdirs();
    try (PrintStream out = new PrintStream(fileLines)) {
      for (String line : LINES) {
        out.println(line);
      }
    }
  }

  public static void validateCountsFile(File file) throws Exception {
    int numWords = 0;

    DatumReader<Pair<Utf8, Long>> reader = new SpecificDatumReader<>();
    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
      try (DataFileStream<Pair<Utf8, Long>> counts = new DataFileStream<>(in, reader)) {
        for (Pair<Utf8, Long> wc : counts) {
          assertEquals(wc.key().toString(), COUNTS.get(wc.key().toString()), wc.value());
          numWords++;
        }
        checkMeta(counts);
      }
    }

    assertEquals(COUNTS.size(), numWords);
  }

  public static void validateSortedFile(String file) throws Exception {
    validateSortedFile(new File(file));
  }

  public static void validateSortedFile(File file) throws Exception {
    DatumReader<ByteBuffer> reader = new GenericDatumReader<>();
    try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
      try (DataFileStream<ByteBuffer> lines = new DataFileStream<>(in, reader)) {
        List<String> sortedLines = new ArrayList<>(Arrays.asList(LINES));
        Collections.sort(sortedLines);
        for (String expectedLine : sortedLines) {
          ByteBuffer buf = lines.next();
          byte[] b = new byte[buf.remaining()];
          buf.get(b);
          assertEquals(expectedLine, new String(b, StandardCharsets.UTF_8).trim());
        }
        assertFalse(lines.hasNext());
      }
    }
  }

  // metadata tests
  private static final String STRING_KEY = "string-key";
  private static final String LONG_KEY = "long-key";
  private static final String BYTES_KEY = "bytes-key";

  private static final String STRING_META_VALUE = "value";
  private static final long LONG_META_VALUE = 666;
  private static final byte[] BYTES_META_VALUE = new byte[] { (byte) 0x00, (byte) 0x80, (byte) 0xff };

  public static void setMeta(JobConf job) {
    AvroJob.setOutputMeta(job, STRING_KEY, STRING_META_VALUE);
    AvroJob.setOutputMeta(job, LONG_KEY, LONG_META_VALUE);
    AvroJob.setOutputMeta(job, BYTES_KEY, BYTES_META_VALUE);
  }

  public static void checkMeta(DataFileStream<?> in) throws Exception {
    assertEquals(STRING_META_VALUE, in.getMetaString(STRING_KEY));
    assertEquals(LONG_META_VALUE, in.getMetaLong(LONG_KEY));
    assertTrue(Arrays.equals(BYTES_META_VALUE, in.getMeta(BYTES_KEY)));
  }

}
