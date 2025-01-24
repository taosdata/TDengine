/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.avro.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Resolver;
import org.apache.avro.Resolver.Action;
import org.apache.avro.Resolver.Container;
import org.apache.avro.Resolver.EnumAdjust;
import org.apache.avro.Resolver.Promote;
import org.apache.avro.Resolver.ReaderUnion;
import org.apache.avro.Resolver.RecordAdjust;
import org.apache.avro.Resolver.Skip;
import org.apache.avro.Resolver.WriterUnion;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.InstanceSupplier;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.FastReaderBuilder.RecordReader.Stage;
import org.apache.avro.io.parsing.ResolvingGrammarGenerator;
import org.apache.avro.reflect.ReflectionUtil;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;
import org.apache.avro.util.WeakIdentityHashMap;
import org.apache.avro.util.internal.Accessor;

public class FastReaderBuilder {

  /**
   * Generic/SpecificData instance that contains basic functionalities like
   * instantiation of objects
   */
  private final GenericData data;

  /** first schema is reader schema, second is writer schema */
  private final Map<Schema, Map<Schema, RecordReader>> readerCache = Collections
      .synchronizedMap(new WeakIdentityHashMap<>());

  private boolean keyClassEnabled = true;

  private boolean classPropEnabled = true;

  public static FastReaderBuilder get() {
    return new FastReaderBuilder(GenericData.get());
  }

  public static FastReaderBuilder getSpecific() {
    return new FastReaderBuilder(SpecificData.get());
  }

  public static boolean isSupportedData(GenericData data) {
    return data.getClass() == GenericData.class || data.getClass() == SpecificData.class;
  }

  public FastReaderBuilder(GenericData parentData) {
    this.data = parentData;
  }

  public FastReaderBuilder withKeyClassEnabled(boolean enabled) {
    this.keyClassEnabled = enabled;
    return this;
  }

  public boolean isKeyClassEnabled() {
    return this.keyClassEnabled;
  }

  public FastReaderBuilder withClassPropEnabled(boolean enabled) {
    this.classPropEnabled = enabled;
    return this;
  }

  public boolean isClassPropEnabled() {
    return this.classPropEnabled;
  }

  public <D> DatumReader<D> createDatumReader(Schema schema) throws IOException {
    return createDatumReader(schema, schema);
  }

  @SuppressWarnings("unchecked")
  public <D> DatumReader<D> createDatumReader(Schema writerSchema, Schema readerSchema) throws IOException {
    Schema resolvedWriterSchema = Schema.applyAliases(writerSchema, readerSchema);
    return (DatumReader<D>) getReaderFor(readerSchema, resolvedWriterSchema);
  }

  private FieldReader getReaderFor(Schema readerSchema, Schema writerSchema) throws IOException {
    Action resolvedAction = Resolver.resolve(writerSchema, readerSchema, data);
    return getReaderFor(resolvedAction, null);
  }

  private FieldReader getReaderFor(Action action, Conversion<?> explicitConversion) throws IOException {
    final FieldReader baseReader = getNonConvertedReader(action);
    return applyConversions(action.reader, baseReader, explicitConversion);
  }

  private RecordReader createRecordReader(RecordAdjust action) throws IOException {
    // record readers are created in a two-step process, first registering it, then
    // initializing it,
    // to prevent endless loops on recursive types
    RecordReader recordReader = getRecordReaderFromCache(action.reader, action.writer);
    synchronized (recordReader) {
      // only need to initialize once
      if (recordReader.getInitializationStage() == Stage.NEW) {
        initializeRecordReader(recordReader, action);
      }
    }
    return recordReader;
  }

  private RecordReader initializeRecordReader(RecordReader recordReader, RecordAdjust action) throws IOException {
    recordReader.startInitialization();

    // generate supplier for the new object instances
    Object testInstance = action.instanceSupplier.newInstance(null, action.reader);
    IntFunction<Conversion<?>> conversionSupplier = getConversionSupplier(testInstance);

    ExecutionStep[] readSteps = new ExecutionStep[action.fieldActions.length + action.readerOrder.length
        - action.firstDefault];

    int i = 0;
    int fieldCounter = 0;
    // compute what to do with writer's fields
    for (; i < action.fieldActions.length; i++) {
      Action fieldAction = action.fieldActions[i];
      if (fieldAction instanceof Skip) {
        readSteps[i] = (r, decoder) -> GenericDatumReader.skip(fieldAction.writer, decoder);
      } else {
        Field readerField = action.readerOrder[fieldCounter++];
        Conversion<?> conversion = conversionSupplier.apply(readerField.pos());
        FieldReader reader = getReaderFor(fieldAction, conversion);
        readSteps[i] = createFieldSetter(readerField, reader);
      }
    }

    // add defaulting if required
    for (; i < readSteps.length; i++) {
      readSteps[i] = getDefaultingStep(action.readerOrder[fieldCounter++]);
    }

    recordReader.finishInitialization(readSteps, action.reader, action.instanceSupplier);
    return recordReader;
  }

  private ExecutionStep createFieldSetter(Field field, FieldReader reader) {
    int pos = field.pos();
    if (reader.canReuse()) {
      return (object, decoder) -> {
        IndexedRecord record = (IndexedRecord) object;
        record.put(pos, reader.read(record.get(pos), decoder));
      };
    } else {
      return (object, decoder) -> {
        IndexedRecord record = (IndexedRecord) object;
        record.put(pos, reader.read(null, decoder));
      };
    }
  }

  private ExecutionStep getDefaultingStep(Schema.Field field) throws IOException {
    Object defaultValue = data.getDefaultValue(field);

    if (isObjectImmutable(defaultValue)) {
      return createFieldSetter(field, (old, d) -> defaultValue);
    } else if (defaultValue instanceof Utf8) {
      return createFieldSetter(field, reusingReader((old, d) -> readUtf8(old, (Utf8) defaultValue)));
    } else if (defaultValue instanceof List && ((List<?>) defaultValue).isEmpty()) {
      return createFieldSetter(field, reusingReader((old, d) -> data.newArray(old, 0, field.schema())));
    } else if (defaultValue instanceof Map && ((Map<?, ?>) defaultValue).isEmpty()) {
      return createFieldSetter(field, reusingReader((old, d) -> data.newMap(old, 0)));
    } else {
      DatumReader<Object> datumReader = createDatumReader(field.schema());
      byte[] encoded = getEncodedValue(field);
      FieldReader fieldReader = reusingReader(
          (old, decoder) -> datumReader.read(old, DecoderFactory.get().binaryDecoder(encoded, null)));
      return createFieldSetter(field, fieldReader);
    }
  }

  private boolean isObjectImmutable(Object object) {
    return object == null || object instanceof Number || object instanceof String || object instanceof GenericEnumSymbol
        || object.getClass().isEnum();
  }

  private Utf8 readUtf8(Object reuse, Utf8 newValue) {
    if (reuse instanceof Utf8) {
      Utf8 oldUtf8 = (Utf8) reuse;
      oldUtf8.set(newValue);
      return oldUtf8;
    } else {
      return new Utf8(newValue);
    }
  }

  private byte[] getEncodedValue(Field field) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    ResolvingGrammarGenerator.encode(encoder, field.schema(), Accessor.defaultValue(field));
    encoder.flush();

    return out.toByteArray();
  }

  private IntFunction<Conversion<?>> getConversionSupplier(Object record) {
    if (record instanceof SpecificRecordBase) {
      return ((SpecificRecordBase) record)::getConversion;
    } else {
      return index -> null;
    }
  }

  private RecordReader getRecordReaderFromCache(Schema readerSchema, Schema writerSchema) {
    return readerCache.computeIfAbsent(readerSchema, k -> new WeakIdentityHashMap<>()).computeIfAbsent(writerSchema,
        k -> new RecordReader());
  }

  private FieldReader applyConversions(Schema readerSchema, FieldReader reader, Conversion<?> explicitConversion) {
    Conversion<?> conversion = explicitConversion;

    if (conversion == null) {
      if (readerSchema.getLogicalType() == null) {
        return reader;
      }
      conversion = data.getConversionFor(readerSchema.getLogicalType());
      if (conversion == null) {
        return reader;
      }
    }

    Conversion<?> finalConversion = conversion;
    return (old, decoder) -> Conversions.convertToLogicalType(reader.read(old, decoder), readerSchema,
        readerSchema.getLogicalType(), finalConversion);
  }

  private FieldReader getNonConvertedReader(Action action) throws IOException {
    switch (action.type) {
    case CONTAINER:
      switch (action.reader.getType()) {
      case MAP:
        return createMapReader(action.reader, (Container) action);
      case ARRAY:
        return createArrayReader(action.reader, (Container) action);
      default:
        throw new IllegalStateException("Error getting reader for action type " + action.getClass());
      }
    case DO_NOTHING:
      return getReaderForBaseType(action.reader, action.writer);
    case RECORD:
      return createRecordReader((RecordAdjust) action);
    case ENUM:
      return createEnumReader((EnumAdjust) action);
    case PROMOTE:
      return createPromotingReader((Promote) action);
    case WRITER_UNION:
      return createUnionReader((WriterUnion) action);
    case READER_UNION:
      return getReaderFor(((ReaderUnion) action).actualAction, null);
    case ERROR:
      return (old, decoder) -> {
        throw new AvroTypeException(action.toString());
      };
    default:
      throw new IllegalStateException("Error getting reader for action type " + action.getClass());
    }
  }

  private FieldReader getReaderForBaseType(Schema readerSchema, Schema writerSchema) throws IOException {
    switch (readerSchema.getType()) {
    case NULL:
      return (old, decoder) -> {
        decoder.readNull();
        return null;
      };
    case BOOLEAN:
      return (old, decoder) -> decoder.readBoolean();
    case STRING:
      return createStringReader(readerSchema, writerSchema);
    case INT:
      return (old, decoder) -> decoder.readInt();
    case LONG:
      return (old, decoder) -> decoder.readLong();
    case FLOAT:
      return (old, decoder) -> decoder.readFloat();
    case DOUBLE:
      return (old, decoder) -> decoder.readDouble();
    case BYTES:
      return createBytesReader();
    case FIXED:
      return createFixedReader(readerSchema, writerSchema);
    case RECORD: // covered by action type
    case UNION: // covered by action type
    case ENUM: // covered by action type
    case MAP: // covered by action type
    case ARRAY: // covered by action type
    default:
      throw new IllegalStateException("Error getting reader for type " + readerSchema.getFullName());
    }
  }

  private FieldReader createPromotingReader(Promote promote) throws IOException {
    switch (promote.reader.getType()) {
    case BYTES:
      return (reuse, decoder) -> ByteBuffer.wrap(decoder.readString(null).getBytes());
    case STRING:
      return createBytesPromotingToStringReader(promote.reader);
    case LONG:
      return (reuse, decoder) -> (long) decoder.readInt();
    case FLOAT:
      switch (promote.writer.getType()) {
      case INT:
        return (reuse, decoder) -> (float) decoder.readInt();
      case LONG:
        return (reuse, decoder) -> (float) decoder.readLong();
      default:
      }
      break;
    case DOUBLE:
      switch (promote.writer.getType()) {
      case INT:
        return (reuse, decoder) -> (double) decoder.readInt();
      case LONG:
        return (reuse, decoder) -> (double) decoder.readLong();
      case FLOAT:
        return (reuse, decoder) -> (double) decoder.readFloat();
      default:
      }
      break;
    default:
    }
    throw new IllegalStateException(
        "No promotion possible for type " + promote.writer.getType() + " to " + promote.reader.getType());
  }

  private FieldReader createStringReader(Schema readerSchema, Schema writerSchema) {
    FieldReader stringReader = createSimpleStringReader(readerSchema);
    if (isClassPropEnabled()) {
      return getTransformingStringReader(readerSchema.getProp(SpecificData.CLASS_PROP), stringReader);
    } else {
      return stringReader;
    }
  }

  private FieldReader createSimpleStringReader(Schema readerSchema) {
    String stringProperty = readerSchema.getProp(GenericData.STRING_PROP);
    if (GenericData.StringType.String.name().equals(stringProperty)) {
      return (old, decoder) -> decoder.readString();
    } else {
      return (old, decoder) -> decoder.readString(old instanceof Utf8 ? (Utf8) old : null);
    }
  }

  private FieldReader createBytesPromotingToStringReader(Schema readerSchema) {
    String stringProperty = readerSchema.getProp(GenericData.STRING_PROP);
    if (GenericData.StringType.String.name().equals(stringProperty)) {
      return (old, decoder) -> getStringFromByteBuffer(decoder.readBytes(null));
    } else {
      return (old, decoder) -> getUtf8FromByteBuffer(old, decoder.readBytes(null));
    }
  }

  private String getStringFromByteBuffer(ByteBuffer buffer) {
    return new String(buffer.array(), buffer.position(), buffer.remaining(), StandardCharsets.UTF_8);
  }

  private Utf8 getUtf8FromByteBuffer(Object old, ByteBuffer buffer) {
    return (old instanceof Utf8) ? ((Utf8) old).set(new Utf8(buffer.array())) : new Utf8(buffer.array());
  }

  private FieldReader createUnionReader(WriterUnion action) throws IOException {
    FieldReader[] unionReaders = new FieldReader[action.actions.length];
    for (int i = 0; i < action.actions.length; i++) {
      unionReaders[i] = getReaderFor(action.actions[i], null);
    }
    return createUnionReader(unionReaders);
  }

  private FieldReader createUnionReader(FieldReader[] unionReaders) {
    return reusingReader((reuse, decoder) -> {
      final int selection = decoder.readIndex();
      return unionReaders[selection].read(null, decoder);
    });

  }

  private FieldReader createMapReader(Schema readerSchema, Container action) throws IOException {
    FieldReader keyReader = createMapKeyReader(readerSchema);
    FieldReader valueReader = getReaderFor(action.elementAction, null);
    return new MapReader(keyReader, valueReader);
  }

  private FieldReader createMapKeyReader(Schema readerSchema) {
    FieldReader stringReader = createSimpleStringReader(readerSchema);
    if (isKeyClassEnabled()) {
      return getTransformingStringReader(readerSchema.getProp(SpecificData.KEY_CLASS_PROP),
          createSimpleStringReader(readerSchema));
    } else {
      return stringReader;
    }
  }

  private FieldReader getTransformingStringReader(String valueClass, FieldReader stringReader) {
    if (valueClass == null) {
      return stringReader;
    } else {
      Function<String, ?> transformer = findClass(valueClass)
          .map(clazz -> ReflectionUtil.getConstructorAsFunction(String.class, clazz)).orElse(null);
      if (transformer != null) {
        return (old, decoder) -> transformer.apply((String) stringReader.read(null, decoder));
      }
    }

    return stringReader;
  }

  private Optional<Class<?>> findClass(String clazz) {
    try {
      return Optional.of(data.getClassLoader().loadClass(clazz));
    } catch (ReflectiveOperationException e) {
      return Optional.empty();
    }
  }

  @SuppressWarnings("unchecked")
  private FieldReader createArrayReader(Schema readerSchema, Container action) throws IOException {
    FieldReader elementReader = getReaderFor(action.elementAction, null);

    return reusingReader((reuse, decoder) -> {
      if (reuse instanceof GenericArray) {
        GenericArray<Object> reuseArray = (GenericArray<Object>) reuse;
        long l = decoder.readArrayStart();
        reuseArray.clear();

        while (l > 0) {
          for (long i = 0; i < l; i++) {
            reuseArray.add(elementReader.read(reuseArray.peek(), decoder));
          }
          l = decoder.arrayNext();
        }
        return reuseArray;
      } else {
        long l = decoder.readArrayStart();
        List<Object> array = (reuse instanceof List) ? (List<Object>) reuse
            : new GenericData.Array<>((int) l, readerSchema);
        array.clear();
        while (l > 0) {
          for (long i = 0; i < l; i++) {
            array.add(elementReader.read(null, decoder));
          }
          l = decoder.arrayNext();
        }
        return array;
      }
    });
  }

  private FieldReader createEnumReader(EnumAdjust action) {
    return reusingReader((reuse, decoder) -> {
      int index = decoder.readEnum();
      Object resultObject = action.values[index];
      if (resultObject == null) {
        throw new AvroTypeException("No match for " + action.writer.getEnumSymbols().get(index));
      }
      return resultObject;
    });
  }

  private FieldReader createFixedReader(Schema readerSchema, Schema writerSchema) {
    return reusingReader((reuse, decoder) -> {
      GenericFixed fixed = (GenericFixed) data.createFixed(reuse, readerSchema);
      decoder.readFixed(fixed.bytes(), 0, readerSchema.getFixedSize());
      return fixed;
    });
  }

  private FieldReader createBytesReader() {
    return reusingReader(
        (reuse, decoder) -> decoder.readBytes(reuse instanceof ByteBuffer ? (ByteBuffer) reuse : null));
  }

  public static FieldReader reusingReader(ReusingFieldReader reader) {
    return reader;
  }

  public interface FieldReader extends DatumReader<Object> {
    @Override
    public Object read(Object reuse, Decoder decoder) throws IOException;

    public default boolean canReuse() {
      return false;
    }

    @Override
    default void setSchema(Schema schema) {
      throw new UnsupportedOperationException();
    }
  }

  public interface ReusingFieldReader extends FieldReader {
    @Override
    public default boolean canReuse() {
      return true;
    }
  }

  public static class RecordReader implements FieldReader {
    public enum Stage {
      NEW, INITIALIZING, INITIALIZED
    }

    private ExecutionStep[] readSteps;
    private InstanceSupplier supplier;
    private Schema schema;
    private Stage stage = Stage.NEW;

    public Stage getInitializationStage() {
      return this.stage;
    }

    public void reset() {
      this.stage = Stage.NEW;
    }

    public void startInitialization() {
      this.stage = Stage.INITIALIZING;
    }

    public void finishInitialization(ExecutionStep[] readSteps, Schema schema, InstanceSupplier supp) {
      this.readSteps = readSteps;
      this.schema = schema;
      this.supplier = supp;
      this.stage = Stage.INITIALIZED;
    }

    @Override
    public boolean canReuse() {
      return true;
    }

    @Override
    public Object read(Object reuse, Decoder decoder) throws IOException {
      Object object = supplier.newInstance(reuse, schema);
      for (ExecutionStep thisStep : readSteps) {
        thisStep.execute(object, decoder);
      }
      return object;
    }
  }

  public static class MapReader implements FieldReader {

    private final FieldReader keyReader;
    private final FieldReader valueReader;

    public MapReader(FieldReader keyReader, FieldReader valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public Object read(Object reuse, Decoder decoder) throws IOException {
      long l = decoder.readMapStart();
      Map<Object, Object> targetMap = new HashMap<>();

      while (l > 0) {
        for (int i = 0; i < l; i++) {
          Object key = keyReader.read(null, decoder);
          Object value = valueReader.read(null, decoder);
          targetMap.put(key, value);
        }
        l = decoder.mapNext();
      }

      return targetMap;
    }
  }

  public interface ExecutionStep {
    public void execute(Object record, Decoder decoder) throws IOException;
  }

}
