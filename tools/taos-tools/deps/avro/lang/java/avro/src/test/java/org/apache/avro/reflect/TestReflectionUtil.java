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
package org.apache.avro.reflect;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class TestReflectionUtil {

  @Test
  public void testUnsafeUtil() {
    new Tester().checkUnsafe();
  }

  @Test
  public void testUnsafeWhenNotExists() throws Exception {
    ClassLoader cl = new NoUnsafe();
    Class<?> testerClass = cl.loadClass(Tester.class.getName());
    testerClass.getDeclaredMethod("checkUnsafe").invoke(testerClass.getDeclaredConstructor().newInstance());
  }

  public static final class Tester {
    public Tester() {
    }

    public void checkUnsafe() {
      ReflectionUtil.getFieldAccess();
    }

  }

  private static final class NoUnsafe extends ClassLoader {
    private ClassLoader parent = TestReflectionUtil.class.getClassLoader();

    @Override
    public java.lang.Class<?> loadClass(String name) throws ClassNotFoundException {
      Class<?> clazz = findLoadedClass(name);
      if (clazz != null) {
        return clazz;
      }
      if ("sun.misc.Unsafe".equals(name)) {
        throw new ClassNotFoundException(name);
      }
      if (!name.startsWith("org.apache.avro.")) {
        return parent.loadClass(name);
      }

      InputStream data = parent.getResourceAsStream(name.replace('.', '/') + ".class");
      byte[] buf = new byte[10240]; // big enough, too lazy to loop
      int size;
      try {
        size = data.read(buf);
      } catch (IOException e) {
        throw new ClassNotFoundException();
      }
      clazz = defineClass(name, buf, 0, size);
      resolveClass(clazz);
      return clazz;
    }

  }
}
