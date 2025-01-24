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

package org.apache.avro.hadoop.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.hadoop.util.AvroCharSequenceComparator;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SortedKeyValueFile is an indexed Avro container file of KeyValue records
 * sorted by key.
 *
 * <p>
 * The SortedKeyValueFile is a directory with two files, named 'data' and
 * 'index'. The 'data' file is an ordinary Avro container file with records.
 * Each record has exactly two fields, 'key' and 'value'. The keys are sorted
 * lexicographically. The 'index' file is a small Avro container file mapping
 * keys in the 'data' file to their byte positions. The index file is intended
 * to fit in memory, so it should remain small. There is one entry in the index
 * file for each data block in the Avro container file.
 * </p>
 *
 * <p>
 * SortedKeyValueFile is to Avro container file as MapFile is to SequenceFile.
 * </p>
 */
public class SortedKeyValueFile {
  private static final Logger LOG = LoggerFactory.getLogger(SortedKeyValueFile.class);

  /** The name of the data file within the SortedKeyValueFile directory. */
  public static final String DATA_FILENAME = "data";

  /** The name of the index file within the SortedKeyValueFile directory. */
  public static final String INDEX_FILENAME = "index";

  /**
   * Reads a SortedKeyValueFile by loading the key index into memory.
   *
   * <p>
   * When doing a lookup, this reader finds the correct block in the data file
   * using the key index. It performs a single disk seek to the block and loads
   * the entire block into memory. The block is scanned until the key is found or
   * is determined not to exist.
   * </p>
   *
   * @param <K> The key type.
   * @param <V> The value type.
   */
  public static class Reader<K, V> implements Closeable, Iterable<AvroKeyValue<K, V>> {
    /** The index from key to its byte offset into the data file. */
    private final NavigableMap<K, Long> mIndex;

    /** The reader for the data file. */
    private final DataFileReader<GenericRecord> mDataFileReader;

    /** The key schema for the data file. */
    private final Schema mKeySchema;

    /** The model for the data. */
    private GenericData model;

    /** A class to encapsulate the options of a Reader. */
    public static class Options {
      /** The configuration. */
      private Configuration mConf;

      /** The path to the SortedKeyValueFile to read. */
      private Path mPath;

      /** The reader schema for the key. */
      private Schema mKeySchema;

      /** The reader schema for the value. */
      private Schema mValueSchema;

      /** The model for the data. */
      private GenericData model = SpecificData.get();

      /**
       * Sets the configuration.
       *
       * @param conf The configuration.
       * @return This options instance.
       */
      public Options withConfiguration(Configuration conf) {
        mConf = conf;
        return this;
      }

      /**
       * Gets the configuration.
       *
       * @return The configuration.
       */
      public Configuration getConfiguration() {
        return mConf;
      }

      /**
       * Sets the input path.
       *
       * @param path The input path.
       * @return This options instance.
       */
      public Options withPath(Path path) {
        mPath = path;
        return this;
      }

      /**
       * Gets the input path.
       *
       * @return The input path.
       */
      public Path getPath() {
        return mPath;
      }

      /**
       * Sets the reader schema for the key.
       *
       * @param keySchema The reader schema for the key.
       * @return This options instance.
       */
      public Options withKeySchema(Schema keySchema) {
        mKeySchema = keySchema;
        return this;
      }

      /**
       * Gets the reader schema for the key.
       *
       * @return The reader schema for the key.
       */
      public Schema getKeySchema() {
        return mKeySchema;
      }

      /**
       * Sets the reader schema for the value.
       *
       * @param valueSchema The reader schema for the value.
       * @return This options instance.
       */
      public Options withValueSchema(Schema valueSchema) {
        mValueSchema = valueSchema;
        return this;
      }

      /**
       * Gets the reader schema for the value.
       *
       * @return The reader schema for the value.
       */
      public Schema getValueSchema() {
        return mValueSchema;
      }

      /** Set the data model. */
      public Options withDataModel(GenericData model) {
        this.model = model;
        return this;
      }

      /** Return the data model. */
      public GenericData getDataModel() {
        return model;
      }

    }

    /**
     * Constructs a reader.
     *
     * @param options The options.
     * @throws IOException If there is an error.
     */
    public Reader(Options options) throws IOException {
      mKeySchema = options.getKeySchema();
      this.model = options.getDataModel();

      // Load the whole index file into memory.
      Path indexFilePath = new Path(options.getPath(), INDEX_FILENAME);
      LOG.debug("Loading the index from {}", indexFilePath);
      mIndex = loadIndexFile(options.getConfiguration(), indexFilePath, mKeySchema);

      // Open the data file.
      Path dataFilePath = new Path(options.getPath(), DATA_FILENAME);
      LOG.debug("Loading the data file {}", dataFilePath);
      Schema recordSchema = AvroKeyValue.getSchema(mKeySchema, options.getValueSchema());
      DatumReader<GenericRecord> datumReader = model.createDatumReader(recordSchema);
      mDataFileReader = new DataFileReader<>(new FsInput(dataFilePath, options.getConfiguration()), datumReader);

    }

    /**
     * Gets the first value associated with a given key, or null if it is not found.
     *
     * <p>
     * This method will move the current position in the file to the record
     * immediately following the requested key.
     * </p>
     *
     * @param key The key to look up.
     * @return The value associated with the key, or null if not found.
     * @throws IOException If there is an error.
     */
    public V get(K key) throws IOException {
      // Look up the entry in the index.
      LOG.debug("Looking up key {} in the index", key);
      Map.Entry<K, Long> indexEntry = mIndex.floorEntry(key);
      if (null == indexEntry) {
        LOG.debug("Key {} was not found in the index (it is before the first entry)", key);
        return null;
      }
      LOG.debug("Key was found in the index, seeking to syncpoint {}", indexEntry.getValue());

      // Seek to the data block that would contain the entry.
      mDataFileReader.seek(indexEntry.getValue());

      // Scan from this position of the file until we find it or pass it.
      for (AvroKeyValue<K, V> record : this) {
        int comparison = model.compare(record.getKey(), key, mKeySchema);
        if (0 == comparison) {
          // We've found it!
          LOG.debug("Found record for key {}", key);
          return record.getValue();
        }
        if (comparison > 0) {
          // We've passed it.
          LOG.debug("Searched beyond the point where key {} would appear in the file", key);
          return null;
        }
      }

      // We've reached the end of the file.
      LOG.debug("Searched to the end of the file but did not find key {}", key);
      return null;
    }

    /**
     * Returns an iterator starting at the current position in the file.
     *
     * <p>
     * Use the get() method to move the current position.
     * </p>
     *
     * <p>
     * Note that this iterator is shared with other clients of the file; it does not
     * contain a separate pointer into the file.
     * </p>
     *
     * @return An iterator.
     */
    @Override
    public Iterator<AvroKeyValue<K, V>> iterator() {
      return new AvroKeyValue.Iterator<>(mDataFileReader.iterator());
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mDataFileReader.close();
    }

    /**
     * Loads an index file into an in-memory map, from key to file offset in bytes.
     *
     * @param conf      The configuration.
     * @param path      The path to the index file.
     * @param keySchema The reader schema for the key.
     * @throws IOException If there is an error.
     */
    private <K> NavigableMap<K, Long> loadIndexFile(Configuration conf, Path path, Schema keySchema)
        throws IOException {
      DatumReader<GenericRecord> datumReader = model
          .createDatumReader(AvroKeyValue.getSchema(keySchema, Schema.create(Schema.Type.LONG)));

      NavigableMap<K, Long> index = new TreeMap<>();
      try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(new FsInput(path, conf), datumReader)) {
        if (Schema.create(Schema.Type.STRING).equals(keySchema)) {
          // Because Avro STRING types are mapped to the Java CharSequence class that does
          // not
          // mandate the implementation of Comparable, we need to specify a special
          // CharSequence comparator if the key type is a string. This hack only fixes the
          // problem for primitive string types. If, for example, you tried to use a
          // record
          // type as the key, any string fields inside of it would not be compared
          // correctly
          // against java.lang.Strings.
          index = new TreeMap<>(new AvroCharSequenceComparator<>());
        }
        for (GenericRecord genericRecord : fileReader) {
          AvroKeyValue<K, Long> indexRecord = new AvroKeyValue<>(genericRecord);
          index.put(indexRecord.getKey(), indexRecord.getValue());
        }
      }
      return index;
    }
  }

  /**
   * Writes a SortedKeyValueFile.
   *
   * @param <K> The key type.
   * @param <V> The value type.
   */
  public static class Writer<K, V> implements Closeable {
    /** The key schema. */
    private final Schema mKeySchema;

    /** The value schema. */
    private final Schema mValueSchema;

    /** The schema of the data file records. */
    private final Schema mRecordSchema;

    /** The schema of the index file records. */
    private final Schema mIndexSchema;

    /** The model for the data. */
    private GenericData model;

    /** The writer for the data file. */
    private final DataFileWriter<GenericRecord> mDataFileWriter;

    /** The writer for the index file. */
    private final DataFileWriter<GenericRecord> mIndexFileWriter;

    /**
     * We store an indexed key for every mIndexInterval records written to the data
     * file.
     */
    private final int mIndexInterval;

    /** The number of records written to the file so far. */
    private long mRecordsWritten;

    /** The most recent key that was appended to the file, or null. */
    private K mPreviousKey;

    /**
     * A class to encapsulate the various options of a SortedKeyValueFile.Writer.
     */
    public static class Options {
      /** The key schema. */
      private Schema mKeySchema;

      /** The value schema. */
      private Schema mValueSchema;

      /** The configuration. */
      private Configuration mConf;

      /** The path to the output file. */
      private Path mPath;

      /** The number of records between indexed entries. */
      private int mIndexInterval = 128;

      /** The model for the data. */
      private GenericData model = SpecificData.get();

      /** The compression codec for the data. */
      private CodecFactory codec = CodecFactory.nullCodec();

      /**
       * Sets the key schema.
       *
       * @param keySchema The key schema.
       * @return This options instance.
       */
      public Options withKeySchema(Schema keySchema) {
        mKeySchema = keySchema;
        return this;
      }

      /**
       * Gets the key schema.
       *
       * @return The key schema.
       */
      public Schema getKeySchema() {
        return mKeySchema;
      }

      /**
       * Sets the value schema.
       *
       * @param valueSchema The value schema.
       * @return This options instance.
       */
      public Options withValueSchema(Schema valueSchema) {
        mValueSchema = valueSchema;
        return this;
      }

      /**
       * Gets the value schema.
       *
       * @return The value schema.
       */
      public Schema getValueSchema() {
        return mValueSchema;
      }

      /**
       * Sets the configuration.
       *
       * @param conf The configuration.
       * @return This options instance.
       */
      public Options withConfiguration(Configuration conf) {
        mConf = conf;
        return this;
      }

      /**
       * Gets the configuration.
       *
       * @return The configuration.
       */
      public Configuration getConfiguration() {
        return mConf;
      }

      /**
       * Sets the output path.
       *
       * @param path The output path.
       * @return This options instance.
       */
      public Options withPath(Path path) {
        mPath = path;
        return this;
      }

      /**
       * Gets the output path.
       *
       * @return The output path.
       */
      public Path getPath() {
        return mPath;
      }

      /**
       * Sets the index interval.
       *
       * <p>
       * If the index inverval is N, then every N records will be indexed into the
       * index file.
       * </p>
       *
       * @param indexInterval The index interval.
       * @return This options instance.
       */
      public Options withIndexInterval(int indexInterval) {
        mIndexInterval = indexInterval;
        return this;
      }

      /**
       * Gets the index interval.
       *
       * @return The index interval.
       */
      public int getIndexInterval() {
        return mIndexInterval;
      }

      /** Set the data model. */
      public Options withDataModel(GenericData model) {
        this.model = model;
        return this;
      }

      /** Return the data model. */
      public GenericData getDataModel() {
        return model;
      }

      /** Set the compression codec. */
      public Options withCodec(String codec) {
        this.codec = CodecFactory.fromString(codec);
        return this;
      }

      /** Set the compression codec. */
      public Options withCodec(CodecFactory codec) {
        this.codec = codec;
        return this;
      }

      /** Return the compression codec. */
      public CodecFactory getCodec() {
        return this.codec;
      }
    }

    /**
     * Creates a writer for a new file.
     *
     * @param options The options.
     * @throws IOException If there is an error.
     */
    public Writer(Options options) throws IOException {
      this.model = options.getDataModel();

      if (null == options.getConfiguration()) {
        throw new IllegalArgumentException("Configuration may not be null");
      }

      FileSystem fileSystem = options.getPath().getFileSystem(options.getConfiguration());

      // Save the key and value schemas.
      mKeySchema = options.getKeySchema();
      if (null == mKeySchema) {
        throw new IllegalArgumentException("Key schema may not be null");
      }
      mValueSchema = options.getValueSchema();
      if (null == mValueSchema) {
        throw new IllegalArgumentException("Value schema may not be null");
      }

      // Save the index interval.
      mIndexInterval = options.getIndexInterval();

      // Create the directory.
      if (!fileSystem.mkdirs(options.getPath())) {
        throw new IOException("Unable to create directory for SortedKeyValueFile: " + options.getPath());
      }
      LOG.debug("Created directory {}", options.getPath());

      // Open a writer for the data file.
      Path dataFilePath = new Path(options.getPath(), DATA_FILENAME);
      LOG.debug("Creating writer for avro data file: {}", dataFilePath);
      mRecordSchema = AvroKeyValue.getSchema(mKeySchema, mValueSchema);
      DatumWriter<GenericRecord> datumWriter = model.createDatumWriter(mRecordSchema);
      OutputStream dataOutputStream = fileSystem.create(dataFilePath);
      mDataFileWriter = new DataFileWriter<>(datumWriter).setSyncInterval(1 << 20) // Set the auto-sync interval
                                                                                   // sufficiently large, since
                                                                                   // we will manually sync every
                                                                                   // mIndexInterval records.
          .setCodec(options.getCodec()).create(mRecordSchema, dataOutputStream);

      // Open a writer for the index file.
      Path indexFilePath = new Path(options.getPath(), INDEX_FILENAME);
      LOG.debug("Creating writer for avro index file: {}", indexFilePath);
      mIndexSchema = AvroKeyValue.getSchema(mKeySchema, Schema.create(Schema.Type.LONG));
      DatumWriter<GenericRecord> indexWriter = model.createDatumWriter(mIndexSchema);
      OutputStream indexOutputStream = fileSystem.create(indexFilePath);
      mIndexFileWriter = new DataFileWriter<>(indexWriter).create(mIndexSchema, indexOutputStream);
    }

    /**
     * Appends a record to the SortedKeyValueFile.
     *
     * @param key   The key.
     * @param value The value.
     * @throws IOException If there is an error.
     */
    public void append(K key, V value) throws IOException {
      // Make sure the keys are inserted in sorted order.
      if (null != mPreviousKey && model.compare(key, mPreviousKey, mKeySchema) < 0) {
        throw new IllegalArgumentException("Records must be inserted in sorted key order." + " Attempted to insert key "
            + key + " after " + mPreviousKey + ".");
      }
      mPreviousKey = model.deepCopy(mKeySchema, key);

      // Construct the data record.
      AvroKeyValue<K, V> dataRecord = new AvroKeyValue<>(new GenericData.Record(mRecordSchema));
      dataRecord.setKey(key);
      dataRecord.setValue(value);

      // Index it if necessary.
      if (0 == mRecordsWritten++ % mIndexInterval) {
        // Force a sync to the data file writer, which closes the current data block (if
        // nonempty) and reports the current position in the file.
        long position = mDataFileWriter.sync();

        // Construct the record to put in the index.
        AvroKeyValue<K, Long> indexRecord = new AvroKeyValue<>(new GenericData.Record(mIndexSchema));
        indexRecord.setKey(key);
        indexRecord.setValue(position);
        mIndexFileWriter.append(indexRecord.get());
      }

      // Write it to the data file.
      mDataFileWriter.append(dataRecord.get());
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mIndexFileWriter.close();
      mDataFileWriter.close();
    }
  }
}
