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
package org.apache.avro.ipc.stats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;

/**
 * Represents a histogram of values. This class uses a {@link Segmenter} to
 * determine which bucket to place a given value into. Also stores the last
 * MAX_HISTORY_SIZE entries which have been added to this histogram, in order.
 *
 * Note that Histogram, by itself, is not synchronized.
 * 
 * @param <B> Bucket type. Often String, since buckets are typically used for
 *            their toString() representation.
 * @param <T> Type of value
 */
class Histogram<B, T> {
  /**
   * How many recent additions we should track.
   */
  public static final int MAX_HISTORY_SIZE = 20;

  private Segmenter<B, T> segmenter;
  private int[] counts;
  protected int totalCount;
  private LinkedList<T> recentAdditions;

  /**
   * Interface to determine which bucket to place a value in.
   *
   * Segmenters should be immutable, so many histograms can re-use the same
   * segmenter.
   */
  interface Segmenter<B, T> {
    /** Number of buckets to use. */
    int size();

    /**
     * Which bucket to place value in.
     *
     * @return Index of bucket for the value. At least 0 and less than size().
     * @throws SegmenterException if value does not fit in a bucket.
     */
    int segment(T value);

    /**
     * Returns an iterator of buckets. The order of iteration is consistent with the
     * segment numbers.
     */
    Iterator<B> getBuckets();

    /**
     * Returns a List of bucket boundaries. Useful for printing segmenters.
     */
    List<String> getBoundaryLabels();

    /**
     * Returns the bucket labels as an array;
     */
    List<String> getBucketLabels();
  }

  public static class SegmenterException extends RuntimeException {
    public SegmenterException(String s) {
      super(s);
    }
  }

  public static class TreeMapSegmenter<T extends Comparable<T>> implements Segmenter<String, T> {
    private TreeMap<T, Integer> index = new TreeMap<>();

    public TreeMapSegmenter(SortedSet<T> leftEndpoints) {
      if (leftEndpoints.isEmpty()) {
        throw new IllegalArgumentException("Endpoints must not be empty: " + leftEndpoints);
      }
      int i = 0;
      for (T t : leftEndpoints) {
        index.put(t, i++);
      }
    }

    @Override
    public int segment(T value) {
      Map.Entry<T, Integer> e = index.floorEntry(value);
      if (e == null) {
        throw new SegmenterException("Could not find bucket for: " + value);
      }
      return e.getValue();
    }

    @Override
    public int size() {
      return index.size();
    }

    private String rangeAsString(T a, T b) {
      return String.format("[%s,%s)", a, b == null ? "infinity" : b);
    }

    @Override
    public ArrayList<String> getBoundaryLabels() {
      ArrayList<String> outArray = new ArrayList<>(index.keySet().size());
      for (T obj : index.keySet()) {
        outArray.add(obj.toString());
      }
      return outArray;
    }

    @Override
    public ArrayList<String> getBucketLabels() {
      ArrayList<String> outArray = new ArrayList<>(index.keySet().size());
      Iterator<String> bucketsIt = this.getBuckets();
      while (bucketsIt.hasNext()) {
        outArray.add(bucketsIt.next());
      }
      return outArray;
    }

    @Override
    public Iterator<String> getBuckets() {
      return new Iterator<String>() {
        Iterator<T> it = index.keySet().iterator();
        T cur = it.next(); // there's always at least one element
        int pos = 0;

        @Override
        public boolean hasNext() {
          return (pos < index.keySet().size());
        }

        @Override
        public String next() {
          pos = pos + 1;
          T left = cur;
          cur = it.hasNext() ? it.next() : null;
          return rangeAsString(left, cur);
        }

        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  /**
   * Creates a histogram using the specified segmenter.
   */
  public Histogram(Segmenter<B, T> segmenter) {
    this.segmenter = segmenter;
    this.counts = new int[segmenter.size()];
    this.recentAdditions = new LinkedList<>();
  }

  /** Tallies a value in the histogram. */
  public void add(T value) {
    int i = segmenter.segment(value);
    counts[i]++;
    totalCount++;
    if (this.recentAdditions.size() > Histogram.MAX_HISTORY_SIZE) {
      this.recentAdditions.pollLast();
    }
    this.recentAdditions.push(value);
  }

  /**
   * Returns the underlying bucket values.
   */
  public int[] getHistogram() {
    return counts;
  }

  /**
   * Returns the underlying segmenter used for this histogram.
   */
  public Segmenter<B, T> getSegmenter() {
    return this.segmenter;
  }

  /**
   * Returns values recently added to this histogram. These are in reverse order
   * (most recent first).
   */
  public List<T> getRecentAdditions() {
    return this.recentAdditions;
  }

  /** Returns the total count of entries. */
  public int getCount() {
    return totalCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Entry<B> e : entries()) {
      if (!first) {
        sb.append(";");
      } else {
        first = false;
      }
      sb.append(e.bucket).append("=").append(e.count);
    }
    return sb.toString();
  }

  static class Entry<B> {
    public Entry(B bucket, int count) {
      this.bucket = bucket;
      this.count = count;
    }

    B bucket;
    int count;
  }

  private class EntryIterator implements Iterable<Entry<B>>, Iterator<Entry<B>> {
    int i = 0;
    Iterator<B> bucketNameIterator = segmenter.getBuckets();

    @Override
    public Iterator<Entry<B>> iterator() {
      return this;
    }

    @Override
    public boolean hasNext() {
      return i < segmenter.size();
    }

    @Override
    public Entry<B> next() {
      return new Entry<>(bucketNameIterator.next(), counts[i++]);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  public Iterable<Entry<B>> entries() {
    return new EntryIterator();
  }
}
