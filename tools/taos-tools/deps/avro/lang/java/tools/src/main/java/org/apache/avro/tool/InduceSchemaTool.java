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
package org.apache.avro.tool;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import org.apache.avro.reflect.ReflectData;

/**
 * Utility to induce a schema from a class or a protocol from an interface.
 */
public class InduceSchemaTool implements Tool {

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    if (args.size() == 0 || args.size() > 2) {
      System.err.println("Usage: [colon-delimited-classpath] classname");
      return 1;
    }
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    String className;
    if (args.size() == 2) {
      String classpaths = args.get(0);
      className = args.get(1);
      if (!classpaths.isEmpty()) {
        String[] paths = args.get(0).split(":");
        URL[] urls = new URL[paths.length];
        for (int i = 0; i < paths.length; ++i) {
          urls[i] = new File(paths[i]).toURI().toURL();
        }
        classLoader = URLClassLoader.newInstance(urls, classLoader);
      }
    } else {
      className = args.get(0);
    }

    Class<?> klass = classLoader.loadClass(className);
    if (klass.isInterface()) {
      System.out.println(ReflectData.get().getProtocol(klass).toString(true));
    } else {
      System.out.println(ReflectData.get().getSchema(klass).toString(true));
    }
    return 0;
  }

  @Override
  public String getName() {
    return "induce";
  }

  @Override
  public String getShortDescription() {
    return "Induce schema/protocol from Java class/interface via reflection.";
  }
}
