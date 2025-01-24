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

package org.apache.avro.hadoop.io;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * A wrapper around a Hadoop {@link org.apache.hadoop.io.SequenceFile} that also
 * supports reading and writing Avro data.
 *
 * <p>
 * The vanilla Hadoop <code>SequenceFile</code> contains a <i>header</i>
 * followed by a sequence of <i>records</i>. A <i>record</i> consists of a
 * <i>key</i> and a <i>value</i>. The <i>key</i> and <i>value</i> must either:
 * </p>
 *
 * <ul>
 * <li>implement the <code>Writable</code> interface, or</li>
 * <li>be accepted by a <code>Serialization</code> registered with the
 * <code>SerializationFactory</code>.</li>
 * </ul>
 *
 * <p>
 * Since Avro data are Plain Old Java Objects (e.g., <code>Integer</code> for
 * data with schema <i>"int"</i>), they do not implement <i>Writable</i>.
 * Furthermore, a {@link org.apache.hadoop.io.serializer.Serialization}
 * implementation cannot determine whether an object instance of type
 * <code>CharSequence</code> that also implements <code>Writable</code> should
 * be serialized using Avro or WritableSerialization.
 * </p>
 *
 * <p>
 * The solution implemented in <code>AvroSequenceFile</code> is to:
 * </p>
 *
 * <ul>
 * <li>wrap Avro key data in an <code>AvroKey</code> object,</li>
 * <li>wrap Avro value data in an <code>AvroValue</code> object,</li>
 * <li>configure and register <code>AvroSerialization</code> with the
 * <code>SerializationFactory</code>, which will accept only objects that are
 * instances of either <code>AvroKey</code> or <code>AvroValue</code>, and</li>
 * <li>store the Avro key and value schemas in the SequenceFile
 * <i>header</i>.</li>
 * </ul>
 */
public class AvroSequenceFile {
  private static final Logger LOG = LoggerFactory.getLogger(AvroSequenceFile.class);

  /** The SequenceFile.Metadata field for the Avro key writer schema. */
  public static final Text METADATA_FIELD_KEY_SCHEMA = new Text("avro.key.schema");

  /** The SequenceFile.Metadata field for the Avro value writer schema. */
  public static final Text METADATA_FIELD_VALUE_SCHEMA = new Text("avro.value.schema");

  /** Constructor disabled for this container class. */
  private AvroSequenceFile() {
  }

  /**
   * Creates a writer from a set of options.
   *
   * <p>
   * Since there are different implementations of <code>Writer</code> depending on
   * the compression type, this method constructs the appropriate subclass
   * depending on the compression type given in the <code>options</code>.
   * </p>
   *
   * @param options The options for the writer.
   * @return A new writer instance.
   * @throws IOException If the writer cannot be created.
   */
  public static SequenceFile.Writer createWriter(Writer.Options options) throws IOException {
    return SequenceFile.createWriter(options.getFileSystem(), options.getConfigurationWithAvroSerialization(),
        options.getOutputPath(), options.getKeyClass(), options.getValueClass(), options.getBufferSizeBytes(),
        options.getReplicationFactor(), options.getBlockSizeBytes(), options.getCompressionType(),
        options.getCompressionCodec(), options.getProgressable(), options.getMetadataWithAvroSchemas());
  }

  /**
   * A writer for an uncompressed SequenceFile that supports Avro data.
   */
  public static class Writer extends SequenceFile.Writer {
    /**
     * A helper class to encapsulate the options that can be used to construct a
     * Writer.
     */
    public static class Options {
      /**
       * A magic value representing the default for buffer size, block size, and
       * replication factor.
       */
      private static final short DEFAULT = -1;

      private FileSystem mFileSystem;
      private Configuration mConf;
      private Path mOutputPath;
      private Class<?> mKeyClass;
      private Schema mKeyWriterSchema;
      private Class<?> mValueClass;
      private Schema mValueWriterSchema;
      private int mBufferSizeBytes;
      private short mReplicationFactor;
      private long mBlockSizeBytes;
      private Progressable mProgressable;
      private CompressionType mCompressionType;
      private CompressionCodec mCompressionCodec;
      private Metadata mMetadata;

      /**
       * Creates a new <code>Options</code> instance with default values.
       */
      public Options() {
        mBufferSizeBytes = DEFAULT;
        mReplicationFactor = DEFAULT;
        mBlockSizeBytes = DEFAULT;
        mCompressionType = CompressionType.NONE;
        mMetadata = new Metadata();
      }

      /**
       * Sets the filesystem the SequenceFile should be written to.
       *
       * @param fileSystem The filesystem.
       * @return This options instance.
       */
      public Options withFileSystem(FileSystem fileSystem) {
        if (null == fileSystem) {
          throw new IllegalArgumentException("Filesystem may not be null");
        }
        mFileSystem = fileSystem;
        return this;
      }

      /**
       * Sets the Hadoop configuration.
       *
       * @param conf The configuration.
       * @return This options instance.
       */
      public Options withConfiguration(Configuration conf) {
        if (null == conf) {
          throw new IllegalArgumentException("Configuration may not be null");
        }
        mConf = conf;
        return this;
      }

      /**
       * Sets the output path for the SequenceFile.
       *
       * @param outputPath The output path.
       * @return This options instance.
       */
      public Options withOutputPath(Path outputPath) {
        if (null == outputPath) {
          throw new IllegalArgumentException("Output path may not be null");
        }
        mOutputPath = outputPath;
        return this;
      }

      /**
       * Sets the class of the key records to be written.
       *
       * <p>
       * If the keys will be Avro data, use
       * {@link #withKeySchema(org.apache.avro.Schema)} to specify the writer schema.
       * The key class will be automatically set to
       * {@link org.apache.avro.mapred.AvroKey}.
       * </p>
       *
       * @param keyClass The key class.
       * @return This options instance.
       */
      public Options withKeyClass(Class<?> keyClass) {
        if (null == keyClass) {
          throw new IllegalArgumentException("Key class may not be null");
        }
        mKeyClass = keyClass;
        return this;
      }

      /**
       * Sets the writer schema of the key records when using Avro data.
       *
       * <p>
       * The key class will automatically be set to
       * {@link org.apache.avro.mapred.AvroKey}, so there is no need to call
       * {@link #withKeyClass(Class)} when using this method.
       * </p>
       *
       * @param keyWriterSchema The writer schema for the keys.
       * @return This options instance.
       */
      public Options withKeySchema(Schema keyWriterSchema) {
        if (null == keyWriterSchema) {
          throw new IllegalArgumentException("Key schema may not be null");
        }
        withKeyClass(AvroKey.class);
        mKeyWriterSchema = keyWriterSchema;
        return this;
      }

      /**
       * Sets the class of the value records to be written.
       *
       * <p>
       * If the values will be Avro data, use
       * {@link #withValueSchema(org.apache.avro.Schema)} to specify the writer
       * schema. The value class will be automatically set to
       * {@link org.apache.avro.mapred.AvroValue}.
       * </p>
       *
       * @param valueClass The value class.
       * @return This options instance.
       */
      public Options withValueClass(Class<?> valueClass) {
        if (null == valueClass) {
          throw new IllegalArgumentException("Value class may not be null");
        }
        mValueClass = valueClass;
        return this;
      }

      /**
       * Sets the writer schema of the value records when using Avro data.
       *
       * <p>
       * The value class will automatically be set to
       * {@link org.apache.avro.mapred.AvroValue}, so there is no need to call
       * {@link #withValueClass(Class)} when using this method.
       * </p>
       *
       * @param valueWriterSchema The writer schema for the values.
       * @return This options instance.
       */
      public Options withValueSchema(Schema valueWriterSchema) {
        if (null == valueWriterSchema) {
          throw new IllegalArgumentException("Value schema may not be null");
        }
        withValueClass(AvroValue.class);
        mValueWriterSchema = valueWriterSchema;
        return this;
      }

      /**
       * Sets the write buffer size in bytes.
       *
       * @param bytes The desired buffer size.
       * @return This options instance.
       */
      public Options withBufferSizeBytes(int bytes) {
        if (bytes < 0) {
          throw new IllegalArgumentException("Buffer size may not be negative");
        }
        mBufferSizeBytes = bytes;
        return this;
      }

      /**
       * Sets the desired replication factor for the file.
       *
       * @param replicationFactor The replication factor.
       * @return This options instance.
       */
      public Options withReplicationFactor(short replicationFactor) {
        if (replicationFactor <= 0) {
          throw new IllegalArgumentException("Replication factor must be positive");
        }
        mReplicationFactor = replicationFactor;
        return this;
      }

      /**
       * Sets the desired size of the file blocks.
       *
       * @param bytes The desired block size in bytes.
       * @return This options instance.
       */
      public Options withBlockSizeBytes(long bytes) {
        if (bytes <= 0) {
          throw new IllegalArgumentException("Block size must be positive");
        }
        mBlockSizeBytes = bytes;
        return this;
      }

      /**
       * Sets an object to report progress to.
       *
       * @param progressable A progressable object to track progress.
       * @return This options instance.
       */
      public Options withProgressable(Progressable progressable) {
        mProgressable = progressable;
        return this;
      }

      /**
       * Sets the type of compression.
       *
       * @param compressionType The type of compression for the output file.
       * @return This options instance.
       */
      public Options withCompressionType(CompressionType compressionType) {
        mCompressionType = compressionType;
        return this;
      }

      /**
       * Sets the compression codec to use if it is enabled.
       *
       * @param compressionCodec The compression codec.
       * @return This options instance.
       */
      public Options withCompressionCodec(CompressionCodec compressionCodec) {
        mCompressionCodec = compressionCodec;
        return this;
      }

      /**
       * Sets the metadata that should be stored in the file <i>header</i>.
       *
       * @param metadata The file metadata.
       * @return This options instance.
       */
      public Options withMetadata(Metadata metadata) {
        if (null == metadata) {
          throw new IllegalArgumentException("Metadata may not be null");
        }
        mMetadata = metadata;
        return this;
      }

      /**
       * Gets the filesystem the SequenceFile should be written to.
       *
       * @return The file system to write to.
       */
      public FileSystem getFileSystem() {
        if (null == mFileSystem) {
          throw new RuntimeException("Must call Options.withFileSystem()");
        }
        return mFileSystem;
      }

      /**
       * Gets the Hadoop configuration.
       *
       * @return The Hadoop configuration.
       */
      public Configuration getConfiguration() {
        return mConf;
      }

      /**
       * Gets the Hadoop configuration with Avro serialization registered.
       *
       * @return The Hadoop configuration.
       */
      public Configuration getConfigurationWithAvroSerialization() {
        Configuration conf = getConfiguration();
        if (null == conf) {
          throw new RuntimeException("Must call Options.withConfiguration()");
        }

        Configuration confWithAvro = new Configuration(conf);
        if (null != mKeyWriterSchema) {
          AvroSerialization.setKeyWriterSchema(confWithAvro, mKeyWriterSchema);
        }
        if (null != mValueWriterSchema) {
          AvroSerialization.setValueWriterSchema(confWithAvro, mValueWriterSchema);
        }
        AvroSerialization.addToConfiguration(confWithAvro);
        return confWithAvro;
      }

      /**
       * Gets the output path for the sequence file.
       *
       * @return The output path.
       */
      public Path getOutputPath() {
        if (null == mOutputPath) {
          throw new RuntimeException("Must call Options.withOutputPath()");
        }
        return mOutputPath;
      }

      /**
       * Gets the class of the key records.
       *
       * @return The key class.
       */
      public Class<?> getKeyClass() {
        if (null == mKeyClass) {
          throw new RuntimeException("Must call Options.withKeyClass() or Options.withKeySchema()");
        }
        return mKeyClass;
      }

      /**
       * Gets the class of the value records.
       *
       * @return The value class.
       */
      public Class<?> getValueClass() {
        if (null == mValueClass) {
          throw new RuntimeException("Must call Options.withValueClass() or Options.withValueSchema()");
        }
        return mValueClass;
      }

      /**
       * Gets the desired size of the buffer used when flushing records to disk.
       *
       * @return The buffer size in bytes.
       */
      public int getBufferSizeBytes() {
        if (DEFAULT == mBufferSizeBytes) {
          return getConfiguration().getInt(IO_FILE_BUFFER_SIZE_KEY, IO_FILE_BUFFER_SIZE_DEFAULT);
        }
        return mBufferSizeBytes;
      }

      /**
       * Gets the desired number of replicas to store for each block of the file.
       *
       * @return The replication factor for the blocks of the file.
       */
      public short getReplicationFactor() {
        if (DEFAULT == mReplicationFactor) {
          return getFileSystem().getDefaultReplication();
        }
        return mReplicationFactor;
      }

      /**
       * Gets the desired size of the file blocks.
       *
       * @return The size of a file block in bytes.
       */
      public long getBlockSizeBytes() {
        if (DEFAULT == mBlockSizeBytes) {
          return getFileSystem().getDefaultBlockSize();
        }
        return mBlockSizeBytes;
      }

      /**
       * Gets the object to report progress to.
       *
       * @return A progressable object to track progress.
       */
      public Progressable getProgressable() {
        return mProgressable;
      }

      /**
       * Gets the type of compression.
       *
       * @return The compression type.
       */
      public CompressionType getCompressionType() {
        return mCompressionType;
      }

      /**
       * Gets the compression codec.
       *
       * @return The compression codec.
       */
      public CompressionCodec getCompressionCodec() {
        return mCompressionCodec;
      }

      /**
       * Gets the SequenceFile metadata to store in the <i>header</i>.
       *
       * @return The metadata header.
       */
      public Metadata getMetadata() {
        return mMetadata;
      }

      /**
       * Gets the metadata to store in the file header, which includes any necessary
       * Avro writer schemas.
       *
       * @return The metadata header with Avro writer schemas if Avro data is being
       *         written.
       */
      private Metadata getMetadataWithAvroSchemas() {
        // mMetadata was intialized in the constructor, and cannot be set to null.
        assert null != mMetadata;

        if (null != mKeyWriterSchema) {
          mMetadata.set(METADATA_FIELD_KEY_SCHEMA, new Text(mKeyWriterSchema.toString()));
        }
        if (null != mValueWriterSchema) {
          mMetadata.set(METADATA_FIELD_VALUE_SCHEMA, new Text(mValueWriterSchema.toString()));
        }
        return mMetadata;
      }
    }

    /**
     * Creates a new <code>Writer</code> to a SequenceFile that supports Avro data.
     *
     * @param options The writer options.
     * @throws IOException If the writer cannot be initialized.
     */
    public Writer(Options options) throws IOException {
      super(options.getFileSystem(), options.getConfigurationWithAvroSerialization(), options.getOutputPath(),
          options.getKeyClass(), options.getValueClass(), options.getBufferSizeBytes(), options.getReplicationFactor(),
          options.getBlockSizeBytes(), options.getProgressable(), options.getMetadataWithAvroSchemas());
    }
  }

  /**
   * A reader for SequenceFiles that may contain Avro data.
   */
  public static class Reader extends SequenceFile.Reader {
    /**
     * A helper class to encapsulate the options that can be used to construct a
     * Reader.
     */
    public static class Options {
      private FileSystem mFileSystem;
      private Path mInputPath;
      private Configuration mConf;
      private Schema mKeyReaderSchema;
      private Schema mValueReaderSchema;

      /**
       * Sets the filesystem the SequenceFile should be read from.
       *
       * @param fileSystem The filesystem.
       * @return This options instance.
       */
      public Options withFileSystem(FileSystem fileSystem) {
        if (null == fileSystem) {
          throw new IllegalArgumentException("Filesystem may not be null");
        }
        mFileSystem = fileSystem;
        return this;
      }

      /**
       * Sets the input path for the SequenceFile.
       *
       * @param inputPath The input path.
       * @return This options instance.
       */
      public Options withInputPath(Path inputPath) {
        if (null == inputPath) {
          throw new IllegalArgumentException("Input path may not be null");
        }
        mInputPath = inputPath;
        return this;
      }

      /**
       * Sets the Hadoop configuration.
       *
       * @param conf The configuration.
       * @return This options instance.
       */
      public Options withConfiguration(Configuration conf) {
        if (null == conf) {
          throw new IllegalArgumentException("Configuration may not be null");
        }
        mConf = conf;
        return this;
      }

      /**
       * Sets the reader schema of the key records when using Avro data.
       *
       * <p>
       * If not set, the writer schema will be used as the reader schema.
       * </p>
       *
       * @param keyReaderSchema The reader schema for the keys.
       * @return This options instance.
       */
      public Options withKeySchema(Schema keyReaderSchema) {
        mKeyReaderSchema = keyReaderSchema;
        return this;
      }

      /**
       * Sets the reader schema of the value records when using Avro data.
       *
       * <p>
       * If not set, the writer schema will be used as the reader schema.
       * </p>
       *
       * @param valueReaderSchema The reader schema for the values.
       * @return This options instance.
       */
      public Options withValueSchema(Schema valueReaderSchema) {
        mValueReaderSchema = valueReaderSchema;
        return this;
      }

      /**
       * Gets the filesystem the SequenceFile should be read rom.
       *
       * @return The file system to read from.
       */
      public FileSystem getFileSystem() {
        if (null == mFileSystem) {
          throw new RuntimeException("Must call Options.withFileSystem()");
        }
        return mFileSystem;
      }

      /**
       * Gets the input path for the sequence file.
       *
       * @return The input path.
       */
      public Path getInputPath() {
        if (null == mInputPath) {
          throw new RuntimeException("Must call Options.withInputPath()");
        }
        return mInputPath;
      }

      /**
       * Gets the Hadoop configuration.
       *
       * @return The Hadoop configuration.
       */
      public Configuration getConfiguration() {
        return mConf;
      }

      /**
       * Gets the Hadoop configuration with Avro serialization registered.
       *
       * @return The Hadoop configuration.
       * @throws IOException If there is an error configuring Avro serialization.
       */
      public Configuration getConfigurationWithAvroSerialization() throws IOException {
        Configuration conf = getConfiguration();
        if (null == conf) {
          throw new RuntimeException("Must call Options.withConfiguration()");
        }

        // Configure schemas and add Avro serialization to the configuration.
        Configuration confWithAvro = new Configuration(conf);
        AvroSerialization.addToConfiguration(confWithAvro);

        // Read the metadata header from the SequenceFile to get the writer schemas.
        Metadata metadata = AvroSequenceFile.getMetadata(getFileSystem(), getInputPath(), confWithAvro);

        // Set the key schema if present in the metadata.
        Text keySchemaText = metadata.get(METADATA_FIELD_KEY_SCHEMA);
        if (null != keySchemaText) {
          LOG.debug("Using key writer schema from SequenceFile metadata: {}", keySchemaText);
          AvroSerialization.setKeyWriterSchema(confWithAvro, new Schema.Parser().parse(keySchemaText.toString()));
          if (null != mKeyReaderSchema) {
            AvroSerialization.setKeyReaderSchema(confWithAvro, mKeyReaderSchema);
          }
        }

        // Set the value schema if present in the metadata.
        Text valueSchemaText = metadata.get(METADATA_FIELD_VALUE_SCHEMA);
        if (null != valueSchemaText) {
          LOG.debug("Using value writer schema from SequenceFile metadata: {}", valueSchemaText);
          AvroSerialization.setValueWriterSchema(confWithAvro, new Schema.Parser().parse(valueSchemaText.toString()));
          if (null != mValueReaderSchema) {
            AvroSerialization.setValueReaderSchema(confWithAvro, mValueReaderSchema);
          }
        }
        return confWithAvro;
      }
    }

    /**
     * Creates a new <code>Reader</code> from a SequenceFile that supports Avro
     * data.
     *
     * @param options The reader options.
     * @throws IOException If the reader cannot be initialized.
     */
    public Reader(Options options) throws IOException {
      super(options.getFileSystem(), options.getInputPath(), options.getConfigurationWithAvroSerialization());
    }
  }

  /**
   * Open and read just the metadata header from a SequenceFile.
   *
   * @param fs   The FileSystem the SequenceFile is on.
   * @param path The path to the file.
   * @param conf The Hadoop configuration.
   * @return The metadata header.
   * @throws IOException If the metadata cannot be read from the file.
   */
  private static Metadata getMetadata(FileSystem fs, Path path, Configuration conf) throws IOException {
    try (SequenceFile.Reader metadataReader = new SequenceFile.Reader(fs, path, conf)) {
      return metadataReader.getMetadata();
    }
  }
}
