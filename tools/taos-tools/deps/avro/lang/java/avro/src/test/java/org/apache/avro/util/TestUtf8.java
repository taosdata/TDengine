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
package org.apache.avro.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class TestUtf8 {
  @Test
  public void testByteConstructor() throws Exception {
    byte[] bs = "Foo".getBytes(StandardCharsets.UTF_8);
    Utf8 u = new Utf8(bs);
    assertEquals(bs.length, u.getByteLength());
    for (int i = 0; i < bs.length; i++) {
      assertEquals(bs[i], u.getBytes()[i]);
    }
  }

  @Test
  public void testArrayReusedWhenLargerThanRequestedSize() {
    byte[] bs = "55555".getBytes(StandardCharsets.UTF_8);
    Utf8 u = new Utf8(bs);
    assertEquals(5, u.getByteLength());
    byte[] content = u.getBytes();
    u.setByteLength(3);
    assertEquals(3, u.getByteLength());
    assertSame(content, u.getBytes());
    u.setByteLength(4);
    assertEquals(4, u.getByteLength());
    assertSame(content, u.getBytes());
  }

  @Test
  public void testHashCodeReused() {
    assertEquals(97, new Utf8("a").hashCode());
    assertEquals(3904, new Utf8("zz").hashCode());
    assertEquals(122, new Utf8("z").hashCode());
    assertEquals(99162322, new Utf8("hello").hashCode());
    assertEquals(3198781, new Utf8("hell").hashCode());

    Utf8 u = new Utf8("a");
    assertEquals(97, u.hashCode());
    assertEquals(97, u.hashCode());

    u.set("a");
    assertEquals(97, u.hashCode());

    u.setByteLength(1);
    assertEquals(97, u.hashCode());
    u.setByteLength(2);
    assertNotEquals(97, u.hashCode());

    u.set("zz");
    assertEquals(3904, u.hashCode());
    u.setByteLength(1);
    assertEquals(122, u.hashCode());

    u.set("hello");
    assertEquals(99162322, u.hashCode());
    u.setByteLength(4);
    assertEquals(3198781, u.hashCode());

    u.set(new Utf8("zz"));
    assertEquals(3904, u.hashCode());
    u.setByteLength(1);
    assertEquals(122, u.hashCode());

    u.set(new Utf8("hello"));
    assertEquals(99162322, u.hashCode());
    u.setByteLength(4);
    assertEquals(3198781, u.hashCode());
  }

  @Test
  public void testSerialization() throws IOException, ClassNotFoundException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos)) {

      Utf8 originalEmpty = new Utf8();
      Utf8 originalBytes = new Utf8("originalBytes".getBytes(StandardCharsets.UTF_8));
      Utf8 originalString = new Utf8("originalString");

      oos.writeObject(originalEmpty);
      oos.writeObject(originalBytes);
      oos.writeObject(originalString);
      oos.flush();

      try (ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
          ObjectInputStream ois = new ObjectInputStream(bis)) {
        assertThat(ois.readObject(), is(originalEmpty));
        assertThat(ois.readObject(), is(originalBytes));
        assertThat(ois.readObject(), is(originalString));
      }
    }
  }

}
