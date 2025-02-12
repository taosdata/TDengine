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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluate the compatibility between a reader schema and a writer schema. A
 * reader and a writer schema are declared compatible if all datum instances of
 * the writer schema can be successfully decoded using the specified reader
 * schema.
 */
public class SchemaCompatibility {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaCompatibility.class);

  /** Utility class cannot be instantiated. */
  private SchemaCompatibility() {
  }

  /** Message to annotate reader/writer schema pairs that are compatible. */
  public static final String READER_WRITER_COMPATIBLE_MESSAGE = "Reader schema can always successfully decode data written using the writer schema.";

  /**
   * Validates that the provided reader schema can be used to decode avro data
   * written with the provided writer schema.
   *
   * @param reader schema to check.
   * @param writer schema to check.
   * @return a result object identifying any compatibility errors.
   */
  public static SchemaPairCompatibility checkReaderWriterCompatibility(final Schema reader, final Schema writer) {
    final SchemaCompatibilityResult compatibility = new ReaderWriterCompatibilityChecker().getCompatibility(reader,
        writer);

    final String message;
    switch (compatibility.getCompatibility()) {
    case INCOMPATIBLE: {
      message = String.format(
          "Data encoded using writer schema:%n%s%n" + "will or may fail to decode using reader schema:%n%s%n",
          writer.toString(true), reader.toString(true));
      break;
    }
    case COMPATIBLE: {
      message = READER_WRITER_COMPATIBLE_MESSAGE;
      break;
    }
    default:
      throw new AvroRuntimeException("Unknown compatibility: " + compatibility);
    }

    return new SchemaPairCompatibility(compatibility, reader, writer, message);
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Tests the equality of two Avro named schemas.
   *
   * <p>
   * Matching includes reader name aliases.
   * </p>
   *
   * @param reader Named reader schema.
   * @param writer Named writer schema.
   * @return whether the names of the named schemas match or not.
   */
  public static boolean schemaNameEquals(final Schema reader, final Schema writer) {
    if (objectsEqual(reader.getName(), writer.getName())) {
      return true;
    }
    // Apply reader aliases:
    return reader.getAliases().contains(writer.getFullName());
  }

  /**
   * Identifies the writer field that corresponds to the specified reader field.
   *
   * <p>
   * Matching includes reader name aliases.
   * </p>
   *
   * @param writerSchema Schema of the record where to look for the writer field.
   * @param readerField  Reader field to identify the corresponding writer field
   *                     of.
   * @return the writer field, if any does correspond, or None.
   */
  public static Field lookupWriterField(final Schema writerSchema, final Field readerField) {
    assert (writerSchema.getType() == Type.RECORD);
    final List<Field> writerFields = new ArrayList<>();
    final Field direct = writerSchema.getField(readerField.name());
    if (direct != null) {
      writerFields.add(direct);
    }
    for (final String readerFieldAliasName : readerField.aliases()) {
      final Field writerField = writerSchema.getField(readerFieldAliasName);
      if (writerField != null) {
        writerFields.add(writerField);
      }
    }
    switch (writerFields.size()) {
    case 0:
      return null;
    case 1:
      return writerFields.get(0);
    default: {
      throw new AvroRuntimeException(String.format(
          "Reader record field %s matches multiple fields in writer record schema %s", readerField, writerSchema));
    }
    }
  }

  /**
   * Reader/writer schema pair that can be used as a key in a hash map.
   *
   * This reader/writer pair differentiates Schema objects based on their system
   * hash code.
   */
  private static final class ReaderWriter {
    private final Schema mReader;
    private final Schema mWriter;

    /**
     * Initializes a new reader/writer pair.
     *
     * @param reader Reader schema.
     * @param writer Writer schema.
     */
    public ReaderWriter(final Schema reader, final Schema writer) {
      mReader = reader;
      mWriter = writer;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return System.identityHashCode(mReader) ^ System.identityHashCode(mWriter);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof ReaderWriter)) {
        return false;
      }
      final ReaderWriter that = (ReaderWriter) obj;
      // Use pointer comparison here:
      return (this.mReader == that.mReader) && (this.mWriter == that.mWriter);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return String.format("ReaderWriter{reader:%s, writer:%s}", mReader, mWriter);
    }
  }

  /**
   * Determines the compatibility of a reader/writer schema pair.
   *
   * <p>
   * Provides memoization to handle recursive schemas.
   * </p>
   */
  private static final class ReaderWriterCompatibilityChecker {
    private static final String ROOT_REFERENCE_TOKEN = "";
    private final Map<ReaderWriter, SchemaCompatibilityResult> mMemoizeMap = new HashMap<>();

    /**
     * Reports the compatibility of a reader/writer schema pair.
     *
     * <p>
     * Memoizes the compatibility results.
     * </p>
     *
     * @param reader Reader schema to test.
     * @param writer Writer schema to test.
     * @return the compatibility of the reader/writer schema pair.
     */
    public SchemaCompatibilityResult getCompatibility(final Schema reader, final Schema writer) {
      Deque<String> location = new ArrayDeque<>();
      return getCompatibility(ROOT_REFERENCE_TOKEN, reader, writer, location);
    }

    /**
     * Reports the compatibility of a reader/writer schema pair.
     * <p>
     * Memoizes the compatibility results.
     * </p>
     *
     * @param referenceToken The equivalent JSON pointer reference token
     *                       representation of the schema node being visited.
     * @param reader         Reader schema to test.
     * @param writer         Writer schema to test.
     * @param location       Stack with which to track the location within the
     *                       schema.
     * @return the compatibility of the reader/writer schema pair.
     */
    private SchemaCompatibilityResult getCompatibility(String referenceToken, final Schema reader, final Schema writer,
        final Deque<String> location) {
      location.addFirst(referenceToken);
      LOG.debug("Checking compatibility of reader {} with writer {}", reader, writer);
      final ReaderWriter pair = new ReaderWriter(reader, writer);
      SchemaCompatibilityResult result = mMemoizeMap.get(pair);
      if (result != null) {
        if (result.getCompatibility() == SchemaCompatibilityType.RECURSION_IN_PROGRESS) {
          // Break the recursion here.
          // schemas are compatible unless proven incompatible:
          result = SchemaCompatibilityResult.compatible();
        }
      } else {
        // Mark this reader/writer pair as "in progress":
        mMemoizeMap.put(pair, SchemaCompatibilityResult.recursionInProgress());
        result = calculateCompatibility(reader, writer, location);
        mMemoizeMap.put(pair, result);
      }
      location.removeFirst();
      return result;
    }

    /**
     * Calculates the compatibility of a reader/writer schema pair.
     *
     * <p>
     * Relies on external memoization performed by
     * {@link #getCompatibility(Schema, Schema)}.
     * </p>
     *
     * @param reader   Reader schema to test.
     * @param writer   Writer schema to test.
     * @param location Stack with which to track the location within the schema.
     * @return the compatibility of the reader/writer schema pair.
     */
    private SchemaCompatibilityResult calculateCompatibility(final Schema reader, final Schema writer,
        final Deque<String> location) {
      assert (reader != null);
      assert (writer != null);
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();

      if (reader.getType() == writer.getType()) {
        switch (reader.getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING: {
          return result;
        }
        case ARRAY: {
          return result
              .mergedWith(getCompatibility("items", reader.getElementType(), writer.getElementType(), location));
        }
        case MAP: {
          return result.mergedWith(getCompatibility("values", reader.getValueType(), writer.getValueType(), location));
        }
        case FIXED: {
          result = result.mergedWith(checkSchemaNames(reader, writer, location));
          return result.mergedWith(checkFixedSize(reader, writer, location));
        }
        case ENUM: {
          result = result.mergedWith(checkSchemaNames(reader, writer, location));
          return result.mergedWith(checkReaderEnumContainsAllWriterEnumSymbols(reader, writer, location));
        }
        case RECORD: {
          result = result.mergedWith(checkSchemaNames(reader, writer, location));
          return result.mergedWith(checkReaderWriterRecordFields(reader, writer, location));
        }
        case UNION: {
          // Check that each individual branch of the writer union can be decoded:
          int i = 0;
          for (final Schema writerBranch : writer.getTypes()) {
            location.addFirst(Integer.toString(i));
            SchemaCompatibilityResult compatibility = getCompatibility(reader, writerBranch);
            if (compatibility.getCompatibility() == SchemaCompatibilityType.INCOMPATIBLE) {
              String message = String.format("reader union lacking writer type: %s", writerBranch.getType());
              result = result.mergedWith(SchemaCompatibilityResult.incompatible(
                  SchemaIncompatibilityType.MISSING_UNION_BRANCH, reader, writer, message, asList(location)));
            }
            location.removeFirst();
            i++;
          }
          // Each schema in the writer union can be decoded with the reader:
          return result;
        }

        default: {
          throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
        }
        }

      } else {
        // Reader and writer have different schema types:

        // Reader compatible with all branches of a writer union is compatible
        if (writer.getType() == Schema.Type.UNION) {
          for (Schema s : writer.getTypes()) {
            result = result.mergedWith(getCompatibility(reader, s));
          }
          return result;
        }

        switch (reader.getType()) {
        case NULL:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case BOOLEAN:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case INT:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case LONG: {
          return (writer.getType() == Type.INT) ? result : result.mergedWith(typeMismatch(reader, writer, location));
        }
        case FLOAT: {
          return ((writer.getType() == Type.INT) || (writer.getType() == Type.LONG)) ? result
              : result.mergedWith(typeMismatch(reader, writer, location));

        }
        case DOUBLE: {
          return ((writer.getType() == Type.INT) || (writer.getType() == Type.LONG) || (writer.getType() == Type.FLOAT))
              ? result
              : result.mergedWith(typeMismatch(reader, writer, location));
        }
        case BYTES: {
          return (writer.getType() == Type.STRING) ? result : result.mergedWith(typeMismatch(reader, writer, location));
        }
        case STRING: {
          return (writer.getType() == Type.BYTES) ? result : result.mergedWith(typeMismatch(reader, writer, location));
        }

        case ARRAY:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case MAP:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case FIXED:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case ENUM:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case RECORD:
          return result.mergedWith(typeMismatch(reader, writer, location));
        case UNION: {
          for (final Schema readerBranch : reader.getTypes()) {
            SchemaCompatibilityResult compatibility = getCompatibility(readerBranch, writer);
            if (compatibility.getCompatibility() == SchemaCompatibilityType.COMPATIBLE) {
              return result;
            }
          }
          // No branch in the reader union has been found compatible with the writer
          // schema:
          String message = String.format("reader union lacking writer type: %s", writer.getType());
          return result.mergedWith(SchemaCompatibilityResult
              .incompatible(SchemaIncompatibilityType.MISSING_UNION_BRANCH, reader, writer, message, asList(location)));
        }

        default: {
          throw new AvroRuntimeException("Unknown schema type: " + reader.getType());
        }
        }
      }
    }

    private SchemaCompatibilityResult checkReaderWriterRecordFields(final Schema reader, final Schema writer,
        final Deque<String> location) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
      location.addFirst("fields");
      // Check that each field in the reader record can be populated from the writer
      // record:
      for (final Field readerField : reader.getFields()) {
        location.addFirst(Integer.toString(readerField.pos()));
        final Field writerField = lookupWriterField(writer, readerField);
        if (writerField == null) {
          // Reader field does not correspond to any field in the writer record schema, so
          // the
          // reader field must have a default value.
          if (!readerField.hasDefaultValue()) {
            // reader field has no default value. Check for the enum default value
            if (readerField.schema().getType() == Type.ENUM && readerField.schema().getEnumDefault() != null) {
              result = result.mergedWith(getCompatibility("type", readerField.schema(), writer, location));
            } else {
              result = result.mergedWith(
                  SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE,
                      reader, writer, readerField.name(), asList(location)));
            }
          }
        } else {
          result = result.mergedWith(getCompatibility("type", readerField.schema(), writerField.schema(), location));
        }
        // POP field index
        location.removeFirst();
      }
      // All fields in the reader record can be populated from the writer record:
      // POP "fields" literal
      location.removeFirst();
      return result;
    }

    private SchemaCompatibilityResult checkReaderEnumContainsAllWriterEnumSymbols(final Schema reader,
        final Schema writer, final Deque<String> location) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
      location.addFirst("symbols");
      final Set<String> symbols = new TreeSet<>(writer.getEnumSymbols());
      symbols.removeAll(reader.getEnumSymbols());
      if (!symbols.isEmpty()) {
        if (reader.getEnumDefault() != null && reader.getEnumSymbols().contains(reader.getEnumDefault())) {
          symbols.clear();
          result = SchemaCompatibilityResult.compatible();
        } else {
          result = SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.MISSING_ENUM_SYMBOLS, reader,
              writer, symbols.toString(), asList(location));
        }
      }
      // POP "symbols" literal
      location.removeFirst();
      return result;
    }

    private SchemaCompatibilityResult checkFixedSize(final Schema reader, final Schema writer,
        final Deque<String> location) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
      location.addFirst("size");
      int actual = reader.getFixedSize();
      int expected = writer.getFixedSize();
      if (actual != expected) {
        String message = String.format("expected: %d, found: %d", expected, actual);
        result = SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.FIXED_SIZE_MISMATCH, reader, writer,
            message, asList(location));
      }
      // POP "size" literal
      location.removeFirst();
      return result;
    }

    private SchemaCompatibilityResult checkSchemaNames(final Schema reader, final Schema writer,
        final Deque<String> location) {
      SchemaCompatibilityResult result = SchemaCompatibilityResult.compatible();
      location.addFirst("name");
      if (!schemaNameEquals(reader, writer)) {
        String message = String.format("expected: %s", writer.getFullName());
        result = SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.NAME_MISMATCH, reader, writer,
            message, asList(location));
      }
      // POP "name" literal
      location.removeFirst();
      return result;
    }

    private SchemaCompatibilityResult typeMismatch(final Schema reader, final Schema writer,
        final Deque<String> location) {
      String message = String.format("reader type: %s not compatible with writer type: %s", reader.getType(),
          writer.getType());
      return SchemaCompatibilityResult.incompatible(SchemaIncompatibilityType.TYPE_MISMATCH, reader, writer, message,
          asList(location));
    }
  }

  /**
   * Identifies the type of a schema compatibility result.
   */
  public enum SchemaCompatibilityType {
    COMPATIBLE, INCOMPATIBLE,

    /** Used internally to tag a reader/writer schema pair and prevent recursion. */
    RECURSION_IN_PROGRESS;
  }

  public enum SchemaIncompatibilityType {
    NAME_MISMATCH, FIXED_SIZE_MISMATCH, MISSING_ENUM_SYMBOLS, READER_FIELD_MISSING_DEFAULT_VALUE, TYPE_MISMATCH,
    MISSING_UNION_BRANCH;
  }

  /**
   * Immutable class representing details about a particular schema pair
   * compatibility check.
   */
  public static final class SchemaCompatibilityResult {

    /**
     * Merges the current {@code SchemaCompatibilityResult} with the supplied result
     * into a new instance, combining the list of
     * {@code Incompatibility Incompatibilities} and regressing to the
     * {@code SchemaCompatibilityType#INCOMPATIBLE INCOMPATIBLE} state if any
     * incompatibilities are encountered.
     *
     * @param toMerge The {@code SchemaCompatibilityResult} to merge with the
     *                current instance.
     * @return A {@code SchemaCompatibilityResult} that combines the state of the
     *         current and supplied instances.
     */
    public SchemaCompatibilityResult mergedWith(SchemaCompatibilityResult toMerge) {
      List<Incompatibility> mergedIncompatibilities = new ArrayList<>(mIncompatibilities);
      mergedIncompatibilities.addAll(toMerge.getIncompatibilities());
      SchemaCompatibilityType compatibilityType = mCompatibilityType == SchemaCompatibilityType.COMPATIBLE
          ? toMerge.mCompatibilityType
          : SchemaCompatibilityType.INCOMPATIBLE;
      return new SchemaCompatibilityResult(compatibilityType, mergedIncompatibilities);
    }

    private final SchemaCompatibilityType mCompatibilityType;
    // the below fields are only valid if INCOMPATIBLE
    private final List<Incompatibility> mIncompatibilities;
    // cached objects for stateless details
    private static final SchemaCompatibilityResult COMPATIBLE = new SchemaCompatibilityResult(
        SchemaCompatibilityType.COMPATIBLE, Collections.emptyList());
    private static final SchemaCompatibilityResult RECURSION_IN_PROGRESS = new SchemaCompatibilityResult(
        SchemaCompatibilityType.RECURSION_IN_PROGRESS, Collections.emptyList());

    private SchemaCompatibilityResult(SchemaCompatibilityType compatibilityType,
        List<Incompatibility> incompatibilities) {
      this.mCompatibilityType = compatibilityType;
      this.mIncompatibilities = incompatibilities;
    }

    /**
     * Returns a details object representing a compatible schema pair.
     *
     * @return a SchemaCompatibilityDetails object with COMPATIBLE
     *         SchemaCompatibilityType, and no other state.
     */
    public static SchemaCompatibilityResult compatible() {
      return COMPATIBLE;
    }

    /**
     * Returns a details object representing a state indicating that recursion is in
     * progress.
     *
     * @return a SchemaCompatibilityDetails object with RECURSION_IN_PROGRESS
     *         SchemaCompatibilityType, and no other state.
     */
    public static SchemaCompatibilityResult recursionInProgress() {
      return RECURSION_IN_PROGRESS;
    }

    /**
     * Returns a details object representing an incompatible schema pair, including
     * error details.
     *
     * @return a SchemaCompatibilityDetails object with INCOMPATIBLE
     *         SchemaCompatibilityType, and state representing the violating part.
     */
    public static SchemaCompatibilityResult incompatible(SchemaIncompatibilityType incompatibilityType,
        Schema readerFragment, Schema writerFragment, String message, List<String> location) {
      Incompatibility incompatibility = new Incompatibility(incompatibilityType, readerFragment, writerFragment,
          message, location);
      return new SchemaCompatibilityResult(SchemaCompatibilityType.INCOMPATIBLE,
          Collections.singletonList(incompatibility));
    }

    /**
     * Returns the SchemaCompatibilityType, always non-null.
     *
     * @return a SchemaCompatibilityType instance, always non-null
     */
    public SchemaCompatibilityType getCompatibility() {
      return mCompatibilityType;
    }

    /**
     * If the compatibility is INCOMPATIBLE, returns {@link Incompatibility
     * Incompatibilities} found, otherwise an empty list.
     *
     * @return a list of {@link Incompatibility Incompatibilities}, may be empty,
     *         never null.
     */
    public List<Incompatibility> getIncompatibilities() {
      return mIncompatibilities;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((mCompatibilityType == null) ? 0 : mCompatibilityType.hashCode());
      result = prime * result + ((mIncompatibilities == null) ? 0 : mIncompatibilities.hashCode());
      return result;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      SchemaCompatibilityResult other = (SchemaCompatibilityResult) obj;
      if (mIncompatibilities == null) {
        if (other.mIncompatibilities != null)
          return false;
      } else if (!mIncompatibilities.equals(other.mIncompatibilities))
        return false;
      return mCompatibilityType == other.mCompatibilityType;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return String.format("SchemaCompatibilityResult{compatibility:%s, incompatibilities:%s}", mCompatibilityType,
          mIncompatibilities);
    }
  }
  // -----------------------------------------------------------------------------------------------

  public static final class Incompatibility {
    private final SchemaIncompatibilityType mType;
    private final Schema mReaderFragment;
    private final Schema mWriterFragment;
    private final String mMessage;
    private final List<String> mLocation;

    Incompatibility(SchemaIncompatibilityType type, Schema readerFragment, Schema writerFragment, String message,
        List<String> location) {
      super();
      this.mType = type;
      this.mReaderFragment = readerFragment;
      this.mWriterFragment = writerFragment;
      this.mMessage = message;
      this.mLocation = location;
    }

    /**
     * Returns the SchemaIncompatibilityType.
     *
     * @return a SchemaIncompatibilityType instance.
     */
    public SchemaIncompatibilityType getType() {
      return mType;
    }

    /**
     * Returns the fragment of the reader schema that failed compatibility check.
     *
     * @return a Schema instance (fragment of the reader schema).
     */
    public Schema getReaderFragment() {
      return mReaderFragment;
    }

    /**
     * Returns the fragment of the writer schema that failed compatibility check.
     *
     * @return a Schema instance (fragment of the writer schema).
     */
    public Schema getWriterFragment() {
      return mWriterFragment;
    }

    /**
     * Returns a human-readable message with more details about what failed. Syntax
     * depends on the SchemaIncompatibilityType.
     *
     * @see #getType()
     * @return a String with details about the incompatibility.
     */
    public String getMessage() {
      return mMessage;
    }

    /**
     * Returns a
     * <a href="https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-08">JSON
     * Pointer</a> describing the node location within the schema's JSON document
     * tree where the incompatibility was encountered.
     *
     * @return JSON Pointer encoded as a string.
     */
    public String getLocation() {
      StringBuilder s = new StringBuilder("/");
      boolean first = true;
      // ignore root element
      for (String coordinate : mLocation.subList(1, mLocation.size())) {
        if (first) {
          first = false;
        } else {
          s.append('/');
        }
        // Apply JSON pointer escaping.
        s.append(coordinate.replace("~", "~0").replace("/", "~1"));
      }
      return s.toString();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((mType == null) ? 0 : mType.hashCode());
      result = prime * result + ((mReaderFragment == null) ? 0 : mReaderFragment.hashCode());
      result = prime * result + ((mWriterFragment == null) ? 0 : mWriterFragment.hashCode());
      result = prime * result + ((mMessage == null) ? 0 : mMessage.hashCode());
      result = prime * result + ((mLocation == null) ? 0 : mLocation.hashCode());
      return result;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Incompatibility other = (Incompatibility) obj;
      if (mType != other.mType) {
        return false;
      }
      if (mReaderFragment == null) {
        if (other.mReaderFragment != null) {
          return false;
        }
      } else if (!mReaderFragment.equals(other.mReaderFragment)) {
        return false;
      }
      if (mWriterFragment == null) {
        if (other.mWriterFragment != null) {
          return false;
        }
      } else if (!mWriterFragment.equals(other.mWriterFragment)) {
        return false;
      }
      if (mMessage == null) {
        if (other.mMessage != null) {
          return false;
        }
      } else if (!mMessage.equals(other.mMessage)) {
        return false;
      }
      if (mLocation == null) {
        return other.mLocation == null;
      } else
        return mLocation.equals(other.mLocation);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return String.format("Incompatibility{type:%s, location:%s, message:%s, reader:%s, writer:%s}", mType,
          getLocation(), mMessage, mReaderFragment, mWriterFragment);
    }
  }
  // -----------------------------------------------------------------------------------------------

  /**
   * Provides information about the compatibility of a single reader and writer
   * schema pair.
   *
   * Note: This class represents a one-way relationship from the reader to the
   * writer schema.
   */
  public static final class SchemaPairCompatibility {
    /** The details of this result. */
    private final SchemaCompatibilityResult mResult;

    /** Validated reader schema. */
    private final Schema mReader;

    /** Validated writer schema. */
    private final Schema mWriter;

    /** Human readable description of this result. */
    private final String mDescription;

    /**
     * Constructs a new instance.
     *
     * @param result      The result of the compatibility check.
     * @param reader      schema that was validated.
     * @param writer      schema that was validated.
     * @param description of this compatibility result.
     */
    public SchemaPairCompatibility(SchemaCompatibilityResult result, Schema reader, Schema writer, String description) {
      mResult = result;
      mReader = reader;
      mWriter = writer;
      mDescription = description;
    }

    /**
     * Gets the type of this result.
     *
     * @return the type of this result.
     */
    public SchemaCompatibilityType getType() {
      return mResult.getCompatibility();
    }

    /**
     * Gets more details about the compatibility, in particular if getType() is
     * INCOMPATIBLE.
     *
     * @return the details of this compatibility check.
     */
    public SchemaCompatibilityResult getResult() {
      return mResult;
    }

    /**
     * Gets the reader schema that was validated.
     *
     * @return reader schema that was validated.
     */
    public Schema getReader() {
      return mReader;
    }

    /**
     * Gets the writer schema that was validated.
     *
     * @return writer schema that was validated.
     */
    public Schema getWriter() {
      return mWriter;
    }

    /**
     * Gets a human readable description of this validation result.
     *
     * @return a human readable description of this validation result.
     */
    public String getDescription() {
      return mDescription;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return String.format("SchemaPairCompatibility{result:%s, readerSchema:%s, writerSchema:%s, description:%s}",
          mResult, mReader, mWriter, mDescription);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object other) {
      if ((other instanceof SchemaPairCompatibility)) {
        final SchemaPairCompatibility result = (SchemaPairCompatibility) other;
        return objectsEqual(result.mResult, mResult) && objectsEqual(result.mReader, mReader)
            && objectsEqual(result.mWriter, mWriter) && objectsEqual(result.mDescription, mDescription);
      } else {
        return false;
      }
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Arrays.hashCode(new Object[] { mResult, mReader, mWriter, mDescription });
    }
  }

  /** Borrowed from Guava's Objects.equal(a, b) */
  private static boolean objectsEqual(Object obj1, Object obj2) {
    return Objects.equals(obj1, obj2);
  }

  private static List<String> asList(Deque<String> deque) {
    List<String> list = new ArrayList<>(deque);
    Collections.reverse(list);
    return Collections.unmodifiableList(list);
  }
}
