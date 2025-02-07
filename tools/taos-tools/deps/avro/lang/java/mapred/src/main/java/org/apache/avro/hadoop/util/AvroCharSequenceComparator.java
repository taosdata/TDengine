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

import java.util.Comparator;

/**
 * Compares Avro string data (data with schema <i>"string"</i>).
 *
 * <p>
 * The only case where comparing Avro objects does not work using their natural
 * order is when the schema is <i>"string"</i>. The Avro string schema maps to
 * the Java <code>CharSequence</code> interface, which does not define
 * <code>equals</code>, <code>hashCode</code>, or <code>compareTo</code>.
 * </p>
 *
 * <p>
 * Using this comparator enables comparisons between <code>String</code> and
 * <code>Utf8</code> objects that are both valid when working with Avro strings.
 * </p>
 *
 * @param <T> The type of object to compare.
 */
public class AvroCharSequenceComparator<T> implements Comparator<T> {
  /** A singleton instance. */
  public static final AvroCharSequenceComparator<CharSequence> INSTANCE = new AvroCharSequenceComparator<>();

  /** {@inheritDoc} */
  @Override
  public int compare(T o1, T o2) {
    if (!(o1 instanceof CharSequence) || !(o2 instanceof CharSequence)) {
      throw new RuntimeException("Attempted use of AvroCharSequenceComparator on non-CharSequence objects: "
          + o1.getClass().getName() + " and " + o2.getClass().getName());
    }
    return compareCharSequence((CharSequence) o1, (CharSequence) o2);
  }

  /**
   * Compares the CharSequences <code>o1</code> and <code>o2</code>.
   *
   * @param o1 The left charsequence.
   * @param o2 The right charsequence.
   * @return a negative integer, zero, or a positive integer if the first argument
   *         is less than, equal to, or greater than the second, respectively.
   */
  private int compareCharSequence(CharSequence o1, CharSequence o2) {
    for (int i = 0; i < Math.max(o1.length(), o2.length()); i++) {
      int charComparison = compareCharacter(o1, o2, i);
      if (0 != charComparison) {
        return charComparison;
      }
    }
    return 0;
  }

  /**
   * Compares the characters of <code>o1</code> and <code>o2</code> at index
   * <code>index</code>.
   *
   * @param o1    The left charsequence.
   * @param o2    The right charsequence.
   * @param index The zero-based index into the charsequences to compare.
   * @return a negative integer, zero, or a positive integer if the first argument
   *         is less than, equal to, or greater than the second, respectively.
   */
  private int compareCharacter(CharSequence o1, CharSequence o2, int index) {
    if (index < o1.length() && index < o2.length()) {
      return Character.compare(o1.charAt(index), o2.charAt(index));
    }
    if (index >= o1.length() && index >= o2.length()) {
      return 0;
    }
    return o1.length() - o2.length();
  }
}
