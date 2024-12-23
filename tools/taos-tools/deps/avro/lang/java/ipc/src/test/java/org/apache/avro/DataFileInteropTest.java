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

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataFileInteropTest {

  private static final File DATAFILE_DIR = new File(System.getProperty("test.dir", "/tmp"));

  @BeforeClass
  public static void printDir() {
    System.out.println("Reading data files from directory: " + DATAFILE_DIR.getAbsolutePath());
  }

  @Test
  public void testGeneratedGeneric() throws IOException {
    System.out.println("Reading with generic:");
    DatumReaderProvider<Object> provider = GenericDatumReader::new;
    readFiles(provider);
  }

  @Test
  public void testGeneratedSpecific() throws IOException {
    System.out.println("Reading with specific:");
    DatumReaderProvider<Interop> provider = SpecificDatumReader::new;
    readFiles(provider);
  }

  // Can't use same Interop.java as specific for reflect.
  // This used to be the case because one used Utf8 and the other String, but
  // we use CharSequence now.
  // The current incompatibility is now that one uses byte[] and the other
  // ByteBuffer

  // We could
  // fix this by defining a reflect-specific version of Interop.java, but we'd
  // need to put it on a different classpath than the specific one.
  // I think changing Specific to generate more flexible code would help too --
  // it could convert ByteBuffer to byte[] or vice/versa.
  // Additionally, some complication arises because of IndexedRecord's simplicity

//   @Test
//   public void testGeneratedReflect() throws IOException {
//     DatumReaderProvider<Interop> provider = new DatumReaderProvider<Interop>() {
//       @Override public DatumReader<Interop> get() {
//         return new ReflectDatumReader<Interop>(Interop.class);
//         }
//       };
//     readFiles(provider);
//   }

  private <T extends Object> void readFiles(DatumReaderProvider<T> provider) throws IOException {
    for (File f : Objects.requireNonNull(DATAFILE_DIR.listFiles())) {
      System.out.println("Reading: " + f.getName());
      try (FileReader<? extends Object> reader = DataFileReader.openReader(f, provider.get())) {
        int i = 0;
        for (Object datum : reader) {
          i++;
          Assert.assertNotNull(datum);
        }
        Assert.assertNotEquals(0, i);
      }
    }
  }

  interface DatumReaderProvider<T extends Object> {
    public DatumReader<T> get();
  }

}
