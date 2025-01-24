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

package org.apache.avro.hadoop.util;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import org.junit.Before;
import org.junit.Test;

import org.apache.avro.util.Utf8;

public class TestAvroCharSequenceComparator {
  private AvroCharSequenceComparator<CharSequence> mComparator;

  @Before
  public void setup() {
    mComparator = new AvroCharSequenceComparator<>();
  }

  @Test
  public void testCompareString() {
    assertEquals(0, mComparator.compare("", ""));
    assertThat(mComparator.compare("", "a"), lessThan(0));
    assertThat(mComparator.compare("a", ""), greaterThan(0));

    assertEquals(0, mComparator.compare("a", "a"));
    assertThat(mComparator.compare("a", "b"), lessThan(0));
    assertThat(mComparator.compare("b", "a"), greaterThan(0));

    assertEquals(0, mComparator.compare("ab", "ab"));
    assertThat(mComparator.compare("a", "aa"), lessThan(0));
    assertThat(mComparator.compare("aa", "a"), greaterThan(0));

    assertThat(mComparator.compare("abc", "abcdef"), lessThan(0));
    assertThat(mComparator.compare("abcdef", "abc"), greaterThan(0));
  }

  @Test
  public void testCompareUtf8() {
    assertEquals(0, mComparator.compare(new Utf8(""), new Utf8("")));
    assertThat(mComparator.compare(new Utf8(""), new Utf8("a")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("a"), new Utf8("")), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("a"), new Utf8("a")));
    assertThat(mComparator.compare(new Utf8("a"), new Utf8("b")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("b"), new Utf8("a")), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("ab"), new Utf8("ab")));
    assertThat(mComparator.compare(new Utf8("a"), new Utf8("aa")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("aa"), new Utf8("a")), greaterThan(0));

    assertThat(mComparator.compare(new Utf8("abc"), new Utf8("abcdef")), lessThan(0));
    assertThat(mComparator.compare(new Utf8("abcdef"), new Utf8("abc")), greaterThan(0));
  }

  @Test
  public void testCompareUtf8ToString() {
    assertEquals(0, mComparator.compare(new Utf8(""), ""));
    assertThat(mComparator.compare(new Utf8(""), "a"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("a"), ""), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("a"), "a"));
    assertThat(mComparator.compare(new Utf8("a"), "b"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("b"), "a"), greaterThan(0));

    assertEquals(0, mComparator.compare(new Utf8("ab"), "ab"));
    assertThat(mComparator.compare(new Utf8("a"), "aa"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("aa"), "a"), greaterThan(0));

    assertThat(mComparator.compare(new Utf8("abc"), "abcdef"), lessThan(0));
    assertThat(mComparator.compare(new Utf8("abcdef"), "abc"), greaterThan(0));
  }
}
