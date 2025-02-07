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
package org.apache.avro;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.RandomData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class TestDataFileConcat {
  private static final Logger LOG = LoggerFactory.getLogger(TestDataFileConcat.class);

  @Rule
  public TemporaryFolder DIR = new TemporaryFolder();

  CodecFactory codec;
  CodecFactory codec2;
  boolean recompress;

  public TestDataFileConcat(CodecFactory codec, CodecFactory codec2, Boolean recompress) {
    this.codec = codec;
    this.codec2 = codec2;
    this.recompress = recompress;
    LOG.info("Testing concatenating files, " + codec2 + " into " + codec + " with recompress=" + recompress);
  }

  @Parameters
  public static List<Object[]> codecs() {
    List<Object[]> r = new ArrayList<>();
    r.add(new Object[] { null, null, false });
    r.add(new Object[] { null, null, true });
    r.add(new Object[] { CodecFactory.deflateCodec(1), CodecFactory.deflateCodec(6), false });
    r.add(new Object[] { CodecFactory.deflateCodec(1), CodecFactory.deflateCodec(6), true });
    r.add(new Object[] { CodecFactory.deflateCodec(3), CodecFactory.nullCodec(), false });
    r.add(new Object[] { CodecFactory.nullCodec(), CodecFactory.deflateCodec(6), false });
    r.add(new Object[] { CodecFactory.xzCodec(1), CodecFactory.xzCodec(2), false });
    r.add(new Object[] { CodecFactory.xzCodec(1), CodecFactory.xzCodec(2), true });
    r.add(new Object[] { CodecFactory.xzCodec(2), CodecFactory.nullCodec(), false });
    r.add(new Object[] { CodecFactory.nullCodec(), CodecFactory.xzCodec(2), false });
    return r;
  }

  private static final int COUNT = Integer.parseInt(System.getProperty("test.count", "200"));
  private static final boolean VALIDATE = !"false".equals(System.getProperty("test.validate", "true"));

  private static final long SEED = System.currentTimeMillis();

  private static final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": ["
      + "{\"name\":\"stringField\", \"type\":\"string\"}" + "," + "{\"name\":\"longField\", \"type\":\"long\"}" + "]}";
  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

  private File makeFile(String name) {
    return new File(DIR.getRoot().getPath(), "test-" + name + ".avro");
  }

  @Test
  public void testConcatenateFiles() throws IOException {
    System.out.println("SEED = " + SEED);
    System.out.println("COUNT = " + COUNT);
    for (int k = 0; k < 5; k++) {
      int syncInterval = 460 + k;
      RandomData data1 = new RandomData(SCHEMA, COUNT, SEED);
      RandomData data2 = new RandomData(SCHEMA, COUNT, SEED + 1);
      File file1 = makeFile((codec == null ? "null" : codec.toString()) + "-A");
      File file2 = makeFile((codec2 == null ? "null" : codec2.toString()) + "-B");
      DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(syncInterval);
      if (codec != null) {
        writer.setCodec(codec);
      }
      writer.create(SCHEMA, file1);
      try {
        for (Object datum : data1) {
          writer.append(datum);
        }
      } finally {
        writer.close();
      }
      DataFileWriter<Object> writer2 = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(syncInterval);
      if (codec2 != null) {
        writer2.setCodec(codec2);
      }
      writer2.create(SCHEMA, file2);
      try {
        for (Object datum : data2) {
          writer2.append(datum);
        }
      } finally {
        writer2.close();
      }
      DataFileWriter<Object> concatinto = new DataFileWriter<>(new GenericDatumWriter<>())
          .setSyncInterval(syncInterval);
      concatinto.appendTo(file1);
      DataFileReader<Object> concatfrom = new DataFileReader<>(file2, new GenericDatumReader<>());
      concatinto.appendAllFrom(concatfrom, recompress);
      concatinto.close();
      concatfrom.close();

      concatfrom = new DataFileReader<>(file2, new GenericDatumReader<>());

      DataFileReader<Object> concat = new DataFileReader<>(file1, new GenericDatumReader<>());
      int count = 0;
      try {
        Object datum = null;
        if (VALIDATE) {
          for (Object expected : data1) {
            datum = concat.next(datum);
            assertEquals("at " + count++, expected, datum);
          }
          for (Object expected : data2) {
            datum = concatfrom.next(datum);
            assertEquals("at " + count++, expected, datum);
          }
          for (Object expected : data2) {
            datum = concat.next(datum);
            assertEquals("at " + count++, expected, datum);
          }
        } else {
          for (int i = 0; i < COUNT * 2; i++) {
            datum = concat.next(datum);
          }
        }
      } finally {
        if (count != 3 * COUNT) {
          System.out.println(count + " " + k);
        }
        concat.close();
        concatfrom.close();
      }

    }
  }

}
