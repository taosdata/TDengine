/**
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
package org.apache.avro.file;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.RandomData;
import org.junit.Test;

/*
 * Tests if we not write any garbage to the end of the file after any exception occurred
 */
public class TestIOExceptionDuringWrite {

  private static class FailingOutputStream extends OutputStream {
    private int byteCnt;

    public FailingOutputStream(int failAfter) {
      byteCnt = failAfter;
    }

    @Override
    public void write(int b) throws IOException {
      if (byteCnt > 0) {
        --byteCnt;
      } else if (byteCnt == 0) {
        --byteCnt;
        throw new IOException("Artificial failure from FailingOutputStream");
      } else {
        fail("No bytes should have been written after IOException");
      }
    }
  }

  private static final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \"Test\", \"fields\": ["
      + "{\"name\":\"stringField\", \"type\":\"string\"}," + "{\"name\":\"longField\", \"type\":\"long\"}]}";
  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

  @Test
  public void testNoWritingAfterException() throws IOException {
    try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(SCHEMA, new FailingOutputStream(100000));
      int recordCnt = 0;
      for (Object datum : new RandomData(SCHEMA, 100000, 42)) {
        writer.append(datum);
        if (++recordCnt % 17 == 0) {
          writer.flush();
        }
      }
    } catch (IOException e) {
      return;
    }
    fail("IOException should have been thrown");
  }

}
