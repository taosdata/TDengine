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
import java.util.Random;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import example.avro.User;

public class GenerateData {
  public static final String[] COLORS = {"red", "orange", "yellow", "green", "blue", "purple", null};
  public static final int USERS = 20;
  public static final String PATH = "./input/users.avro";

  public static void main(String[] args) throws IOException {
    // Open data file
    File file = new File(PATH);
    if (file.getParentFile() != null) {
      file.getParentFile().mkdirs();
    }
    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
    DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
    dataFileWriter.create(User.SCHEMA$, file);

    // Create random users
    User user;
    Random random = new Random();
    for (int i = 0; i < USERS; i++) {
      user = new User("user", null, COLORS[random.nextInt(COLORS.length)]);
      dataFileWriter.append(user);
      System.out.println(user);
    }

    dataFileWriter.close();
  }
}
