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

package org.apache.trevni.avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileUtil;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.mapred.Pair;

public class WordCountUtil {

  public File dir;
  public File linesFiles;
  public File countFiles;

  public WordCountUtil(String testName) {
    this(testName, "part-00000");
  }

  public WordCountUtil(String testName, String partDirName) {
    dir = new File("target/wc", testName);
    linesFiles = new File(new File(dir, "in"), "lines.avro");
    countFiles = new File(new File(dir, "out"), partDirName + "/part-0.trv");
  }

  public static final String[] LINES = new String[] { "the quick brown fox jumps over the lazy dog",
      "the cow jumps over the moon", "the rain in spain falls mainly on the plains" };

  public static final Map<String, Long> COUNTS = new TreeMap<>();
  public static final long TOTAL;
  static {
    long total = 0;
    for (String line : LINES) {
      StringTokenizer tokens = new StringTokenizer(line);
      while (tokens.hasMoreTokens()) {
        String word = tokens.nextToken();
        long count = COUNTS.getOrDefault(word, 0L);
        count++;
        total++;
        COUNTS.put(word, count);
      }
    }
    TOTAL = total;
  }

  public File getDir() {
    return dir;
  }

  public void writeLinesFile() throws IOException {
    FileUtil.fullyDelete(dir);
    DatumWriter<String> writer = new GenericDatumWriter<>();
    DataFileWriter<String> out = new DataFileWriter<>(writer);
    linesFiles.getParentFile().mkdirs();
    out.create(Schema.create(Schema.Type.STRING), linesFiles);
    for (String line : LINES)
      out.append(line);
    out.close();
  }

  public void validateCountsFile() throws Exception {
    AvroColumnReader<Pair<String, Long>> reader = new AvroColumnReader<>(
        new AvroColumnReader.Params(countFiles).setModel(SpecificData.get()));
    int numWords = 0;
    for (Pair<String, Long> wc : reader) {
      assertEquals(wc.key(), COUNTS.get(wc.key()), wc.value());
      numWords++;
    }
    reader.close();
    assertEquals(COUNTS.size(), numWords);
  }

  public void validateCountsFileGenericRecord() throws Exception {
    AvroColumnReader<GenericRecord> reader = new AvroColumnReader<>(
        new AvroColumnReader.Params(countFiles).setModel(SpecificData.get()));
    int numWords = 0;
    for (GenericRecord wc : reader) {
      assertEquals((String) wc.get("key"), COUNTS.get(wc.get("key")), wc.get("value"));
      // assertEquals(wc.getKey(), COUNTS.get(wc.getKey()), wc.getValue());
      numWords++;
    }
    reader.close();
    assertEquals(COUNTS.size(), numWords);
  }

}
