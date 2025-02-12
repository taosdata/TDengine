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

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Conversion;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.Protocol;
import org.apache.avro.Protocol.Message;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.ClassUtils;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/** Utilities to use existing Java classes and interfaces via reflection. */
public class ReflectData extends SpecificData {

  private static final String STRING_OUTER_PARENT_REFERENCE = "this$0";

  @Override
  public boolean useCustomCoders() {
    return false;
  }

  /**
   * {@link ReflectData} implementation that permits null field values. The schema
   * generated for each field is a union of its declared type and null.
   */
  public static class AllowNull extends ReflectData {

    private static final AllowNull INSTANCE = new AllowNull();

    /** Return the singleton instance. */
    public static AllowNull get() {
      return INSTANCE;
    }

    @Override
    protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
      Schema schema = super.createFieldSchema(field, names);
      if (field.getType().isPrimitive()) {
        // for primitive values, such as int, a null will result in a
        // NullPointerException at read time
        return schema;
      }
      return makeNullable(schema);
    }
  }

  private static final ReflectData INSTANCE = new ReflectData();

  /** For subclasses. Applications normally use {@link ReflectData#get()}. */
  public ReflectData() {
  }

  /** Construct with a particular classloader. */
  public ReflectData(ClassLoader classLoader) {
    super(classLoader);
  }

  /** Return the singleton instance. */
  public static ReflectData get() {
    return INSTANCE;
  }

  /**
   * Cause a class to be treated as though it had an {@link Stringable} *
   * annotation.
   */
  public ReflectData addStringable(Class c) {
    stringableClasses.add(c);
    return this;
  }

  /**
   * If this flag is set to true, default values for fields will be assigned
   * dynamically using Java reflections. When enabled, defaults are the field
   * values of an instance created with a no-arg constructor.
   *
   * <p>
   * Let's call this feature `default reflection`. Initially this feature is
   * disabled.
   */
  private boolean defaultGenerated = false;

  /**
   * Enable or disable `default reflection`
   *
   * @param enabled set to `true` to enable the feature. This feature is disabled
   *                by default
   * @return The current instance
   */
  public ReflectData setDefaultsGenerated(boolean enabled) {
    this.defaultGenerated = enabled;
    return this;
  }

  private final Map<Type, Object> defaultValues = new WeakHashMap<>();

  /**
   * Set the default value for a type. When encountering such type, we'll use this
   * provided value instead of trying to create a new one.
   *
   * <p>
   * NOTE: This method automatically enable feature `default reflection`.
   *
   * @param type  The type
   * @param value Its default value
   * @return The current instance
   */
  public ReflectData setDefaultGeneratedValue(Type type, Object value) {
    this.defaultValues.put(type, value);
    this.setDefaultsGenerated(true);
    return this;
  }

  /**
   * Get or create new value instance for a field
   *
   * @param type  The current type
   * @param field A child field
   * @return The default field value
   */
  protected Object getOrCreateDefaultValue(Type type, Field field) {
    Object defaultValue = null;
    field.setAccessible(true);
    try {
      Object typeValue = getOrCreateDefaultValue(type);
      if (typeValue != null) {
        defaultValue = field.get(typeValue);
      }
    } catch (Exception e) {

    }
    return defaultValue;
  }

  /**
   * Get or create new value instance for a type.
   *
   * New instances will be instantiated using no-arg constructors. The newly
   * created one will be cached for later use.
   *
   * @param type The type
   * @return The value
   */
  protected Object getOrCreateDefaultValue(Type type) {
    return this.defaultValues.computeIfAbsent(type, ignored -> {
      try {
        Constructor constructor = ((Class) type).getDeclaredConstructor();
        constructor.setAccessible(true);
        return constructor.newInstance();
      } catch (ClassCastException | InstantiationException | IllegalAccessException | NoSuchMethodException
          | InvocationTargetException e) {
        // do nothing
      }
      return null;
    });
  }

  @Override
  public DatumReader createDatumReader(Schema schema) {
    return new ReflectDatumReader(schema, schema, this);
  }

  @Override
  public DatumReader createDatumReader(Schema writer, Schema reader) {
    return new ReflectDatumReader(writer, reader, this);
  }

  @Override
  public DatumWriter createDatumWriter(Schema schema) {
    return new ReflectDatumWriter(schema, this);
  }

  @Override
  public void setField(Object record, String name, int position, Object value) {
    setField(record, name, position, value, null);
  }

  @Override
  protected void setField(Object record, String name, int position, Object value, Object state) {
    if (record instanceof IndexedRecord) {
      super.setField(record, name, position, value);
      return;
    }
    try {
      getAccessorForField(record, name, position, state).set(record, value);
    } catch (IllegalAccessException | IOException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  public Object getField(Object record, String name, int position) {
    return getField(record, name, position, null);
  }

  @Override
  protected Object getField(Object record, String name, int pos, Object state) {
    if (record instanceof IndexedRecord) {
      return super.getField(record, name, pos);
    }
    try {
      return getAccessorForField(record, name, pos, state).get(record);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private FieldAccessor getAccessorForField(Object record, String name, int pos, Object optionalState) {
    if (optionalState != null) {
      return ((FieldAccessor[]) optionalState)[pos];
    }
    return getFieldAccessor(record.getClass(), name);
  }

  @Override
  protected boolean isRecord(Object datum) {
    if (datum == null)
      return false;
    if (super.isRecord(datum))
      return true;
    if (datum instanceof Collection)
      return false;
    if (datum instanceof Map)
      return false;
    if (datum instanceof GenericFixed)
      return false;
    return getSchema(datum.getClass()).getType() == Schema.Type.RECORD;
  }

  /**
   * Returns true for arrays and false otherwise, with the following exceptions:
   *
   * <ul>
   * <li>
   * <p>
   * Returns true for non-string-keyed maps, which are written as an array of
   * key/value pair records.
   * <li>
   * <p>
   * Returns false for arrays of bytes, since those should be treated as byte data
   * type instead.
   * </ul>
   */
  @Override
  protected boolean isArray(Object datum) {
    if (datum == null)
      return false;
    Class c = datum.getClass();
    return (datum instanceof Collection) || (c.isArray() && c.getComponentType() != Byte.TYPE) || isNonStringMap(datum);
  }

  @Override
  protected Collection getArrayAsCollection(Object datum) {
    return (datum instanceof Map) ? ((Map) datum).entrySet() : (Collection) datum;
  }

  @Override
  protected boolean isBytes(Object datum) {
    if (datum == null)
      return false;
    if (super.isBytes(datum))
      return true;
    Class c = datum.getClass();
    return c.isArray() && c.getComponentType() == Byte.TYPE;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    if (record instanceof GenericContainer)
      return super.getRecordSchema(record);
    return getSchema(record.getClass());
  }

  @Override
  public boolean validate(Schema schema, Object datum) {
    switch (schema.getType()) {
    case ARRAY:
      if (!datum.getClass().isArray())
        return super.validate(schema, datum);
      int length = java.lang.reflect.Array.getLength(datum);
      for (int i = 0; i < length; i++)
        if (!validate(schema.getElementType(), java.lang.reflect.Array.get(datum, i)))
          return false;
      return true;
    default:
      return super.validate(schema, datum);
    }
  }

  static final ClassValue<ClassAccessorData> ACCESSOR_CACHE = new ClassValue<ClassAccessorData>() {
    @Override
    protected ClassAccessorData computeValue(Class<?> c) {
      if (!IndexedRecord.class.isAssignableFrom(c)) {
        return new ClassAccessorData(c);
      }
      return null;
    }
  };

  static class ClassAccessorData {
    private final Class<?> clazz;
    private final Map<String, FieldAccessor> byName = new HashMap<>();
    // getAccessorsFor is already synchronized, no need to wrap
    final Map<Schema, FieldAccessor[]> bySchema = new WeakHashMap<>();

    private ClassAccessorData(Class<?> c) {
      clazz = c;
      for (Field f : getFields(c, false)) {
        if (f.isAnnotationPresent(AvroIgnore.class)) {
          continue;
        }
        FieldAccessor accessor = ReflectionUtil.getFieldAccess().getAccessor(f);
        AvroName avroname = f.getAnnotation(AvroName.class);
        byName.put((avroname != null ? avroname.value() : f.getName()), accessor);
      }
    }

    /**
     * Return the field accessors as an array, indexed by the field index of the
     * given schema.
     */
    private synchronized FieldAccessor[] getAccessorsFor(Schema schema) {
      // if synchronized is removed from this method, adjust bySchema appropriately
      FieldAccessor[] result = bySchema.get(schema);
      if (result == null) {
        result = createAccessorsFor(schema);
        bySchema.put(schema, result);
      }
      return result;
    }

    private FieldAccessor[] createAccessorsFor(Schema schema) {
      List<Schema.Field> avroFields = schema.getFields();
      FieldAccessor[] result = new FieldAccessor[avroFields.size()];
      for (Schema.Field avroField : schema.getFields()) {
        result[avroField.pos()] = byName.get(avroField.name());
      }
      return result;
    }

    private FieldAccessor getAccessorFor(String fieldName) {
      FieldAccessor result = byName.get(fieldName);
      if (result == null) {
        throw new AvroRuntimeException("No field named " + fieldName + " in: " + clazz);
      }
      return result;
    }
  }

  private ClassAccessorData getClassAccessorData(Class<?> c) {
    return ACCESSOR_CACHE.get(c);
  }

  private FieldAccessor[] getFieldAccessors(Class<?> c, Schema s) {
    ClassAccessorData data = getClassAccessorData(c);
    if (data != null) {
      return data.getAccessorsFor(s);
    }
    return null;
  }

  private FieldAccessor getFieldAccessor(Class<?> c, String fieldName) {
    ClassAccessorData data = getClassAccessorData(c);
    if (data != null) {
      return data.getAccessorFor(fieldName);
    }
    return null;
  }

  /** @deprecated Replaced by {@link SpecificData#CLASS_PROP} */
  @Deprecated
  static final String CLASS_PROP = "java-class";
  /** @deprecated Replaced by {@link SpecificData#KEY_CLASS_PROP} */
  @Deprecated
  static final String KEY_CLASS_PROP = "java-key-class";
  /** @deprecated Replaced by {@link SpecificData#ELEMENT_PROP} */
  @Deprecated
  static final String ELEMENT_PROP = "java-element-class";

  private static final Map<String, Class> CLASS_CACHE = new ConcurrentHashMap<>();

  static Class getClassProp(Schema schema, String prop) {
    String name = schema.getProp(prop);
    if (name == null)
      return null;
    Class c = CLASS_CACHE.get(name);
    if (c != null)
      return c;
    try {
      c = ClassUtils.forName(name);
      CLASS_CACHE.put(name, c);
    } catch (ClassNotFoundException e) {
      throw new AvroRuntimeException(e);
    }
    return c;
  }

  private static final Class BYTES_CLASS = byte[].class;
  private static final IdentityHashMap<Class, Class> ARRAY_CLASSES;

  static {
    ARRAY_CLASSES = new IdentityHashMap<>();
    ARRAY_CLASSES.put(byte.class, byte[].class);
    ARRAY_CLASSES.put(char.class, char[].class);
    ARRAY_CLASSES.put(short.class, short[].class);
    ARRAY_CLASSES.put(int.class, int[].class);
    ARRAY_CLASSES.put(long.class, long[].class);
    ARRAY_CLASSES.put(float.class, float[].class);
    ARRAY_CLASSES.put(double.class, double[].class);
    ARRAY_CLASSES.put(boolean.class, boolean[].class);
  }

  /**
   * It returns false for non-string-maps because Avro writes out such maps as an
   * array of records. Even their JSON representation is an array.
   */
  @Override
  protected boolean isMap(Object datum) {
    return (datum instanceof Map) && !isNonStringMap(datum);
  }

  /*
   * Without the Field or Schema corresponding to the datum, it is not possible to
   * accurately find out the non-stringable nature of the key. So we check the
   * class of the keys. If the map is empty, then it doesn't matter whether its
   * considered a string-key map or a non-string-key map
   */
  private boolean isNonStringMap(Object datum) {
    if (datum instanceof Map) {
      Map m = (Map) datum;
      if (m.size() > 0) {
        Class keyClass = m.keySet().iterator().next().getClass();
        return !isStringable(keyClass) && !isStringType(keyClass);
      }
    }
    return false;
  }

  @Override
  public Class getClass(Schema schema) {
    // see if the element class will be converted and use that class
    Conversion<?> conversion = getConversionFor(schema.getLogicalType());
    if (conversion != null) {
      return conversion.getConvertedType();
    }

    switch (schema.getType()) {
    case ARRAY:
      Class collectionClass = getClassProp(schema, CLASS_PROP);
      if (collectionClass != null)
        return collectionClass;
      Class elementClass = getClass(schema.getElementType());
      if (elementClass.isPrimitive()) {
        // avoid expensive translation to array type when primitive
        return ARRAY_CLASSES.get(elementClass);
      } else {
        return java.lang.reflect.Array.newInstance(elementClass, 0).getClass();
      }
    case STRING:
      Class stringClass = getClassProp(schema, CLASS_PROP);
      if (stringClass != null)
        return stringClass;
      return String.class;
    case BYTES:
      return BYTES_CLASS;
    case INT:
      String intClass = schema.getProp(CLASS_PROP);
      if (Byte.class.getName().equals(intClass))
        return Byte.TYPE;
      if (Short.class.getName().equals(intClass))
        return Short.TYPE;
      if (Character.class.getName().equals(intClass))
        return Character.TYPE;
    default:
      return super.getClass(schema);
    }
  }

  static final String NS_MAP_ARRAY_RECORD = // record name prefix
      "org.apache.avro.reflect.Pair";
  static final String NS_MAP_KEY = "key"; // name of key field
  static final int NS_MAP_KEY_INDEX = 0; // index of key field
  static final String NS_MAP_VALUE = "value"; // name of value field
  static final int NS_MAP_VALUE_INDEX = 1; // index of value field

  /*
   * Non-string map-keys need special handling and we convert it to an array of
   * records as: [{"key":{...}, "value":{...}}]
   */
  Schema createNonStringMapSchema(Type keyType, Type valueType, Map<String, Schema> names) {
    Schema keySchema = createSchema(keyType, names);
    Schema valueSchema = createSchema(valueType, names);
    Schema.Field keyField = new Schema.Field(NS_MAP_KEY, keySchema, null, null);
    Schema.Field valueField = new Schema.Field(NS_MAP_VALUE, valueSchema, null, null);
    String name = getNameForNonStringMapRecord(keyType, valueType, keySchema, valueSchema);
    Schema elementSchema = Schema.createRecord(name, null, null, false);
    elementSchema.setFields(Arrays.asList(keyField, valueField));
    Schema arraySchema = Schema.createArray(elementSchema);
    return arraySchema;
  }

  /*
   * Gets a unique and consistent name per key-value pair. So if the same
   * key-value are seen in another map, the same name is generated again.
   */
  private String getNameForNonStringMapRecord(Type keyType, Type valueType, Schema keySchema, Schema valueSchema) {

    // Generate a nice name for classes in java* package
    if (keyType instanceof Class && valueType instanceof Class) {

      Class keyClass = (Class) keyType;
      Class valueClass = (Class) valueType;
      Package pkg1 = keyClass.getPackage();
      Package pkg2 = valueClass.getPackage();

      if (pkg1 != null && pkg1.getName().startsWith("java") && pkg2 != null && pkg2.getName().startsWith("java")) {
        return NS_MAP_ARRAY_RECORD + keyClass.getSimpleName() + valueClass.getSimpleName();
      }
    }

    String name = keySchema.getFullName() + valueSchema.getFullName();
    long fingerprint = SchemaNormalization.fingerprint64(name.getBytes(StandardCharsets.UTF_8));

    if (fingerprint < 0)
      fingerprint = -fingerprint; // ignore sign
    String fpString = Long.toString(fingerprint, 16); // hex
    return NS_MAP_ARRAY_RECORD + fpString;
  }

  static boolean isNonStringMapSchema(Schema s) {
    if (s != null && s.getType() == Schema.Type.ARRAY) {
      Class c = getClassProp(s, CLASS_PROP);
      return c != null && Map.class.isAssignableFrom(c);
    }
    return false;
  }

  /**
   * Get default value for a schema field. Derived classes can override this
   * method to provide values based on object instantiation
   *
   * @param type        Type
   * @param field       Field
   * @param fieldSchema Schema of the field
   * @return The default value
   */
  protected Object createSchemaDefaultValue(Type type, Field field, Schema fieldSchema) {
    Object defaultValue;
    if (defaultGenerated) {
      defaultValue = getOrCreateDefaultValue(type, field);
      if (defaultValue != null) {
        return deepCopy(fieldSchema, defaultValue);
      }
      // if we can't get the default value, try to use previous code below
    }

    AvroDefault defaultAnnotation = field.getAnnotation(AvroDefault.class);
    defaultValue = (defaultAnnotation == null) ? null : Schema.parseJsonToObject(defaultAnnotation.value());

    if (defaultValue == null && fieldSchema.getType() == Schema.Type.UNION) {
      Schema defaultType = fieldSchema.getTypes().get(0);
      if (defaultType.getType() == Schema.Type.NULL) {
        defaultValue = JsonProperties.NULL_VALUE;
      }
    }
    return defaultValue;
  }

  @Override
  protected Schema createSchema(Type type, Map<String, Schema> names) {
    if (type instanceof GenericArrayType) { // generic array
      Type component = ((GenericArrayType) type).getGenericComponentType();
      if (component == Byte.TYPE) // byte array
        return Schema.create(Schema.Type.BYTES);
      Schema result = Schema.createArray(createSchema(component, names));
      setElement(result, component);
      return result;
    } else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) type;
      Class raw = (Class) ptype.getRawType();
      Type[] params = ptype.getActualTypeArguments();
      if (Map.class.isAssignableFrom(raw)) { // Map
        Class key = (Class) params[0];
        if (isStringable(key)) { // Stringable key
          Schema schema = Schema.createMap(createSchema(params[1], names));
          schema.addProp(KEY_CLASS_PROP, key.getName());
          return schema;
        } else if (key != String.class) {
          Schema schema = createNonStringMapSchema(params[0], params[1], names);
          schema.addProp(CLASS_PROP, raw.getName());
          return schema;
        }
      } else if (Collection.class.isAssignableFrom(raw)) { // Collection
        if (params.length != 1)
          throw new AvroTypeException("No array type specified.");
        Schema schema = Schema.createArray(createSchema(params[0], names));
        schema.addProp(CLASS_PROP, raw.getName());
        return schema;
      }
    } else if ((type == Byte.class) || (type == Byte.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(CLASS_PROP, Byte.class.getName());
      return result;
    } else if ((type == Short.class) || (type == Short.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(CLASS_PROP, Short.class.getName());
      return result;
    } else if ((type == Character.class) || (type == Character.TYPE)) {
      Schema result = Schema.create(Schema.Type.INT);
      result.addProp(CLASS_PROP, Character.class.getName());
      return result;
    } else if (type instanceof Class) { // Class
      Class<?> c = (Class<?>) type;
      if (c.isPrimitive() || // primitives
          c == Void.class || c == Boolean.class || c == Integer.class || c == Long.class || c == Float.class
          || c == Double.class || c == Byte.class || c == Short.class || c == Character.class)
        return super.createSchema(type, names);
      if (c.isArray()) { // array
        Class component = c.getComponentType();
        if (component == Byte.TYPE) { // byte array
          Schema result = Schema.create(Schema.Type.BYTES);
          result.addProp(CLASS_PROP, c.getName());
          return result;
        }
        Schema result = Schema.createArray(createSchema(component, names));
        result.addProp(CLASS_PROP, c.getName());
        setElement(result, component);
        return result;
      }
      AvroSchema explicit = c.getAnnotation(AvroSchema.class);
      if (explicit != null) // explicit schema
        return new Schema.Parser().parse(explicit.value());
      if (CharSequence.class.isAssignableFrom(c)) // String
        return Schema.create(Schema.Type.STRING);
      if (ByteBuffer.class.isAssignableFrom(c)) // bytes
        return Schema.create(Schema.Type.BYTES);
      if (Collection.class.isAssignableFrom(c)) // array
        throw new AvroRuntimeException("Can't find element type of Collection");
      Conversion<?> conversion = getConversionByClass(c);
      if (conversion != null) {
        return conversion.getRecommendedSchema();
      }
      String fullName = c.getName();
      Schema schema = names.get(fullName);
      if (schema == null) {
        AvroDoc annotatedDoc = c.getAnnotation(AvroDoc.class); // Docstring
        String doc = (annotatedDoc != null) ? annotatedDoc.value() : null;
        String name = c.getSimpleName();
        String space = c.getPackage() == null ? "" : c.getPackage().getName();
        if (c.getEnclosingClass() != null) // nested class
          space = c.getEnclosingClass().getName().replace('$', '.');
        Union union = c.getAnnotation(Union.class);
        if (union != null) { // union annotated
          return getAnnotatedUnion(union, names);
        } else if (isStringable(c)) { // Stringable
          Schema result = Schema.create(Schema.Type.STRING);
          result.addProp(CLASS_PROP, c.getName());
          return result;
        } else if (c.isEnum()) { // Enum
          List<String> symbols = new ArrayList<>();
          Enum[] constants = (Enum[]) c.getEnumConstants();
          for (Enum constant : constants)
            symbols.add(constant.name());
          schema = Schema.createEnum(name, doc, space, symbols);
          consumeAvroAliasAnnotation(c, schema);
        } else if (GenericFixed.class.isAssignableFrom(c)) { // fixed
          int size = c.getAnnotation(FixedSize.class).value();
          schema = Schema.createFixed(name, doc, space, size);
          consumeAvroAliasAnnotation(c, schema);
        } else if (IndexedRecord.class.isAssignableFrom(c)) { // specific
          return super.createSchema(type, names);
        } else { // record
          List<Schema.Field> fields = new ArrayList<>();
          boolean error = Throwable.class.isAssignableFrom(c);
          schema = Schema.createRecord(name, doc, space, error);
          consumeAvroAliasAnnotation(c, schema);
          names.put(c.getName(), schema);
          for (Field field : getCachedFields(c))
            if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0
                && !field.isAnnotationPresent(AvroIgnore.class)) {
              Schema fieldSchema = createFieldSchema(field, names);
              annotatedDoc = field.getAnnotation(AvroDoc.class); // Docstring
              doc = (annotatedDoc != null) ? annotatedDoc.value() : null;

              Object defaultValue = createSchemaDefaultValue(type, field, fieldSchema);

              AvroName annotatedName = field.getAnnotation(AvroName.class); // Rename fields
              String fieldName = (annotatedName != null) ? annotatedName.value() : field.getName();
              if (STRING_OUTER_PARENT_REFERENCE.equals(fieldName)) {
                throw new AvroTypeException("Class " + fullName + " must be a static inner class");
              }
              Schema.Field recordField = new Schema.Field(fieldName, fieldSchema, doc, defaultValue);

              AvroMeta[] metadata = field.getAnnotationsByType(AvroMeta.class); // add metadata
              for (AvroMeta meta : metadata) {
                if (recordField.getObjectProps().containsKey(meta.key())) {
                  throw new AvroTypeException("Duplicate field prop key: " + meta.key());
                }
                recordField.addProp(meta.key(), meta.value());
              }
              for (Schema.Field f : fields) {
                if (f.name().equals(fieldName))
                  throw new AvroTypeException("double field entry: " + fieldName);
              }

              consumeFieldAlias(field, recordField);

              fields.add(recordField);
            }
          if (error) // add Throwable message
            fields.add(new Schema.Field("detailMessage", THROWABLE_MESSAGE, null, null));
          schema.setFields(fields);
          AvroMeta[] metadata = c.getAnnotationsByType(AvroMeta.class);
          for (AvroMeta meta : metadata) {
            if (schema.getObjectProps().containsKey(meta.key())) {
              throw new AvroTypeException("Duplicate type prop key: " + meta.key());
            }
            schema.addProp(meta.key(), meta.value());
          }
        }
        names.put(fullName, schema);
      }
      return schema;
    }
    return super.createSchema(type, names);
  }

  @Override
  protected boolean isStringable(Class<?> c) {
    return c.isAnnotationPresent(Stringable.class) || super.isStringable(c);
  }

  private static final Schema THROWABLE_MESSAGE = makeNullable(Schema.create(Schema.Type.STRING));

  // if array element type is a class with a union annotation, note it
  // this is required because we cannot set a property on the union itself
  private void setElement(Schema schema, Type element) {
    if (!(element instanceof Class))
      return;
    Class<?> c = (Class<?>) element;
    Union union = c.getAnnotation(Union.class);
    if (union != null) // element is annotated union
      schema.addProp(ELEMENT_PROP, c.getName());
  }

  // construct a schema from a union annotation
  private Schema getAnnotatedUnion(Union union, Map<String, Schema> names) {
    List<Schema> branches = new ArrayList<>();
    for (Class branch : union.value())
      branches.add(createSchema(branch, names));
    return Schema.createUnion(branches);
  }

  /** Create and return a union of the null schema and the provided schema. */
  public static Schema makeNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      // check to see if the union already contains NULL
      for (Schema subType : schema.getTypes()) {
        if (subType.getType() == Schema.Type.NULL) {
          return schema;
        }
      }
      // add null as the first type in a new union
      List<Schema> withNull = new ArrayList<>();
      withNull.add(Schema.create(Schema.Type.NULL));
      withNull.addAll(schema.getTypes());
      return Schema.createUnion(withNull);
    } else {
      // create a union with null
      return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
    }
  }

  private static final Map<Class<?>, Field[]> FIELDS_CACHE = new ConcurrentHashMap<>();

  // Return of this class and its superclasses to serialize.
  private static Field[] getCachedFields(Class<?> recordClass) {
    return FIELDS_CACHE.computeIfAbsent(recordClass, rc -> getFields(rc, true));
  }

  private static Field[] getFields(Class<?> recordClass, boolean excludeJava) {
    Field[] fieldsList;
    Map<String, Field> fields = new LinkedHashMap<>();
    Class<?> c = recordClass;
    do {
      if (excludeJava && c.getPackage() != null && c.getPackage().getName().startsWith("java."))
        break; // skip java built-in classes
      Field[] declaredFields = c.getDeclaredFields();
      Arrays.sort(declaredFields, Comparator.comparing(Field::getName));
      for (Field field : declaredFields)
        if ((field.getModifiers() & (Modifier.TRANSIENT | Modifier.STATIC)) == 0)
          if (fields.put(field.getName(), field) != null)
            throw new AvroTypeException(c + " contains two fields named: " + field);
      c = c.getSuperclass();
    } while (c != null);
    fieldsList = fields.values().toArray(new Field[0]);
    return fieldsList;
  }

  /** Create a schema for a field. */
  protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
    AvroEncode enc = field.getAnnotation(AvroEncode.class);
    if (enc != null)
      try {
        return enc.using().getDeclaredConstructor().newInstance().getSchema();
      } catch (Exception e) {
        throw new AvroRuntimeException("Could not create schema from custom serializer for " + field.getName());
      }

    AvroSchema explicit = field.getAnnotation(AvroSchema.class);
    if (explicit != null) // explicit schema
      return new Schema.Parser().parse(explicit.value());

    Union union = field.getAnnotation(Union.class);
    if (union != null)
      return getAnnotatedUnion(union, names);

    Schema schema = createSchema(field.getGenericType(), names);
    if (field.isAnnotationPresent(Stringable.class)) { // Stringable
      schema = Schema.create(Schema.Type.STRING);
    }
    if (field.isAnnotationPresent(Nullable.class)) // nullable
      schema = makeNullable(schema);
    return schema;
  }

  /**
   * Return the protocol for a Java interface.
   *
   * <p>
   * The correct name of the method parameters needs the <code>-parameters</code>
   * java compiler argument. More info at https://openjdk.java.net/jeps/118
   */
  @Override
  public Protocol getProtocol(Class iface) {
    Protocol protocol = new Protocol(iface.getSimpleName(),
        iface.getPackage() == null ? "" : iface.getPackage().getName());
    Map<String, Schema> names = new LinkedHashMap<>();
    Map<String, Message> messages = protocol.getMessages();
    Map<TypeVariable<?>, Type> genericTypeVariableMap = ReflectionUtil.resolveTypeVariables(iface);
    for (Method method : iface.getMethods()) {
      if ((method.getModifiers() & Modifier.STATIC) == 0) {
        String name = method.getName();
        if (messages.containsKey(name))
          throw new AvroTypeException("Two methods with same name: " + name);
        messages.put(name, getMessage(method, protocol, names, genericTypeVariableMap));
      }
    }

    // reverse types, since they were defined in reference order
    List<Schema> types = new ArrayList<>(names.values());
    Collections.reverse(types);
    protocol.setTypes(types);

    return protocol;
  }

  private Message getMessage(Method method, Protocol protocol, Map<String, Schema> names,
      Map<? extends Type, Type> genericTypeMap) {
    List<Schema.Field> fields = new ArrayList<>();
    for (Parameter parameter : method.getParameters()) {
      Schema paramSchema = getSchema(genericTypeMap.getOrDefault(parameter.getParameterizedType(), parameter.getType()),
          names);
      for (Annotation annotation : parameter.getAnnotations()) {
        if (annotation instanceof AvroSchema) // explicit schema
          paramSchema = new Schema.Parser().parse(((AvroSchema) annotation).value());
        else if (annotation instanceof Union) // union
          paramSchema = getAnnotatedUnion(((Union) annotation), names);
        else if (annotation instanceof Nullable) // nullable
          paramSchema = makeNullable(paramSchema);
      }
      fields.add(new Schema.Field(unmangle(parameter.getName()), paramSchema, null /* doc */, null));
    }

    Schema request = Schema.createRecord(fields);

    Type genericReturnType = method.getGenericReturnType();
    Type returnType = genericTypeMap.getOrDefault(genericReturnType, genericReturnType);
    Union union = method.getAnnotation(Union.class);
    Schema response = union == null ? getSchema(returnType, names) : getAnnotatedUnion(union, names);
    if (method.isAnnotationPresent(Nullable.class)) // nullable
      response = makeNullable(response);

    AvroSchema explicit = method.getAnnotation(AvroSchema.class);
    if (explicit != null) // explicit schema
      response = new Schema.Parser().parse(explicit.value());

    List<Schema> errs = new ArrayList<>();
    errs.add(Protocol.SYSTEM_ERROR); // every method can throw
    for (Type err : method.getGenericExceptionTypes())
      errs.add(getSchema(err, names));
    Schema errors = Schema.createUnion(errs);
    return protocol.createMessage(method.getName(), null /* doc */, Collections.emptyMap() /* propMap */, request,
        response, errors);
  }

  private Schema getSchema(Type type, Map<String, Schema> names) {
    try {
      return createSchema(type, names);
    } catch (AvroTypeException e) { // friendly exception
      throw new AvroTypeException("Error getting schema for " + type + ": " + e.getMessage(), e);
    }
  }

  @Override
  protected int compare(Object o1, Object o2, Schema s, boolean equals) {
    switch (s.getType()) {
    case ARRAY:
      if (!o1.getClass().isArray())
        break;
      Schema elementType = s.getElementType();
      int l1 = java.lang.reflect.Array.getLength(o1);
      int l2 = java.lang.reflect.Array.getLength(o2);
      int l = Math.min(l1, l2);
      for (int i = 0; i < l; i++) {
        int compare = compare(java.lang.reflect.Array.get(o1, i), java.lang.reflect.Array.get(o2, i), elementType,
            equals);
        if (compare != 0)
          return compare;
      }
      return Integer.compare(l1, l2);
    case BYTES:
      if (!o1.getClass().isArray())
        break;
      byte[] b1 = (byte[]) o1;
      byte[] b2 = (byte[]) o2;
      return BinaryData.compareBytes(b1, 0, b1.length, b2, 0, b2.length);
    }
    return super.compare(o1, o2, s, equals);
  }

  @Override
  protected Object getRecordState(Object record, Schema schema) {
    return getFieldAccessors(record.getClass(), schema);
  }

  private void consumeAvroAliasAnnotation(Class<?> c, Schema schema) {
    AvroAlias[] aliases = c.getAnnotationsByType(AvroAlias.class);
    for (AvroAlias alias : aliases) {
      String space = alias.space();
      if (AvroAlias.NULL.equals(space))
        space = null;
      schema.addAlias(alias.alias(), space);
    }
  }

  private void consumeFieldAlias(Field field, Schema.Field recordField) {
    AvroAlias[] aliases = field.getAnnotationsByType(AvroAlias.class);
    for (AvroAlias alias : aliases) {
      if (!alias.space().equals(AvroAlias.NULL)) {
        throw new AvroRuntimeException(
            "Namespaces are not allowed on field aliases. " + "Offending field: " + recordField.name());
      }
      recordField.addAlias(alias.alias());
    }
  }

  @Override
  public Object createFixed(Object old, Schema schema) {
    // SpecificData will try to instantiate the type returned by getClass, but
    // that is the converted class and can't be constructed.
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      Conversion<?> conversion = getConversionFor(schema.getLogicalType());
      if (conversion != null) {
        return new GenericData.Fixed(schema);
      }
    }
    return super.createFixed(old, schema);
  }

  @Override
  public Object newRecord(Object old, Schema schema) {
    // SpecificData will try to instantiate the type returned by getClass, but
    // that is the converted class and can't be constructed.
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null) {
      Conversion<?> conversion = getConversionFor(schema.getLogicalType());
      if (conversion != null) {
        return new GenericData.Record(schema);
      }
    }
    return super.newRecord(old, schema);
  }
}
