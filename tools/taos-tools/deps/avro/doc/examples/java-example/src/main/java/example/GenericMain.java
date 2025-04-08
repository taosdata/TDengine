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

package example;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericMain {
  public static void main(String[] args) throws IOException {
    Schema schema = new Parser().parse(new File("./../user.avsc"));

    GenericRecord user1 = new GenericData.Record(schema);
    user1.put("name", "Alyssa");
    user1.put("favorite_number", 256);
    // Leave favorite color null

    GenericRecord user2 = new GenericData.Record(schema);
    user2.put("name", "Ben");
    user2.put("favorite_number", 7);
    user2.put("favorite_color", "red");

    // Serialize user1 and user2 to disk
    File file = new File("users.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
    dataFileWriter.create(schema, file);
    dataFileWriter.append(user1);
    dataFileWriter.append(user2);
    dataFileWriter.close();

    // Deserialize users from disk
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
    GenericRecord user = null;
    try(DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader)){
      while (dataFileReader.hasNext()) {
        // Reuse user object by passing it to next(). This saves us from
        // allocating and garbage collecting many objects for files with
        // many items.
        user = dataFileReader.next(user);
        System.out.println(user);
      }
    }


  }
}
