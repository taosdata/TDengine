/*
 * Copyright 2021 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.specific;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.atomic.AtomicReference;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import org.junit.Assert;
import org.junit.Test;

public class TestSpecificData {

  @Test
  public void testSeparateThreadContextClassLoader() throws Exception {
    Schema schema = new Schema.Parser().parse(new File("src/test/resources/foo.Bar.avsc"));
    SpecificCompiler compiler = new SpecificCompiler(schema);
    compiler.setStringType(GenericData.StringType.String);
    compiler.compileToDestination(null, new File("target"));

    GenericRecord bar = new GenericData.Record(schema);
    bar.put("title", "hello");
    bar.put("created_at", 1630126246000L);

    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    writer.write(bar, encoder);
    encoder.flush();
    byte[] data = out.toByteArray();

    JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
    StandardJavaFileManager fileManager = javac.getStandardFileManager(null, null, null);
    Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjects("target/foo/Bar.java");
    javac.getTask(null, fileManager, null, null, null, units).call();
    fileManager.close();

    AtomicReference<Exception> ref = new AtomicReference<>();
    ClassLoader cl = new URLClassLoader(new URL[] { new File("target/").toURI().toURL() });
    Thread t = new Thread() {
      @Override
      public void run() {
        SpecificDatumReader<Object> reader = new SpecificDatumReader<>(schema);
        try {
          Object o = reader.read(null, DecoderFactory.get().binaryDecoder(data, null));
          System.out.println(o.getClass() + ": " + o);
        } catch (Exception ex) {
          ref.set(ex);
        }
      }
    };

    t.setContextClassLoader(cl);
    t.start();
    t.join();

    Exception ex = ref.get();
    if (ex != null) {
      ex.printStackTrace();
      Assert.fail(ex.getMessage());
    }
  }
}
