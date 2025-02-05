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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.mapreduce;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;

/**
 * A utility class for working with Avro container files within tests.
 */
public final class AvroFiles {
  private AvroFiles() {
  }

  /**
   * Creates an avro container file.
   *
   * @param file    The file to create.
   * @param schema  The schema for the records the file should contain.
   * @param records The records to put in the file.
   * @param <T>     The (java) type of the avro records.
   * @return The created file.
   */
  public static <T> File createFile(File file, Schema schema, T... records) throws IOException {
    DatumWriter<T> datumWriter = new GenericDatumWriter<>(schema);
    DataFileWriter<T> fileWriter = new DataFileWriter<>(datumWriter);
    fileWriter.create(schema, file);
    for (T record : records) {
      fileWriter.append(record);
    }
    fileWriter.close();

    return file;
  }
}
