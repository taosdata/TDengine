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
package org.apache.avro.specific;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.ClassUtils;
import org.apache.avro.util.internal.ClassValueCache;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/** Utilities for generated Java classes and interfaces. */
public class SpecificData extends GenericData {

  private static final SpecificData INSTANCE = new SpecificData();

  private static final Class<?>[] NO_ARG = new Class[] {};
  private static final Class<?>[] SCHEMA_ARG = new Class[] { Schema.class };

  private static final Function<Class<?>, Constructor<?>> CTOR_CACHE = new ClassValueCache<>(c -> {
    boolean useSchema = SchemaConstructable.class.isAssignableFrom(c);
    try {
      Constructor<?> meth = c.getDeclaredConstructor(useSchema ? SCHEMA_ARG : NO_ARG);
      meth.setAccessible(true);
      return meth;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  });

  private static final Function<Class<?>, SpecificData> MODEL_CACHE = new ClassValueCache<>(c -> {
    Field specificDataField;
    try {
      specificDataField = c.getDeclaredField("MODEL$");
      specificDataField.setAccessible(true);
      return (SpecificData) specificDataField.get(null);
    } catch (NoSuchFieldException e) {
      // Return default instance
      return SpecificData.get();
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException("while trying to access field MODEL$ on " + c.getCanonicalName(), e);
    }
  });

  public static final String CLASS_PROP = "java-class";
  public static final String KEY_CLASS_PROP = "java-key-class";
  public static final String ELEMENT_PROP = "java-element-class";

  /**
   * Reserved words from
   * https://docs.oracle.com/javase/specs/jls/se16/html/jls-3.html require
   * mangling in order to be used in generated Java code.
   */
  public static final Set<String> RESERVED_WORDS = new HashSet<>(Arrays.asList(
      // Keywords from Section 3.9 can't be used as identifiers.
      "_", "abstract", "assert", "boolean", "break", "byte", "case", "catch", "char", "class", "const", "continue",
      "default", "do", "double", "else", "enum", "extends", "final", "finally", "float", "for", "goto", "if",
      "implements", "import", "instanceof", "int", "interface", "long", "native", "new", "package", "private",
      "protected", "public", "return", "short", "static", "strictfp", "super", "switch", "synchronized", "this",
      "throw", "throws", "transient", "try", "void", "volatile", "while",
      // Literals from Section 3.10 can't be used as identifiers.
      "true", "false", "null",
      // Some keywords from Section 3.8 can't be used as type identifiers.
      "var", "yield", "record",
      // Note that module-related restricted keywords can still be used.
      // Class names used internally by the avro code generator
      "Builder"));

  /**
   * Read/write some common builtin classes as strings. Representing these as
   * strings isn't always best, as they aren't always ordered ideally, but at
   * least they're stored. Also note that, for compatibility, only classes that
   * wouldn't be otherwise correctly readable or writable should be added here,
   * e.g., those without a no-arg constructor or those whose fields are all
   * transient.
   */
  protected Set<Class> stringableClasses = new HashSet<>(Arrays.asList(java.math.BigDecimal.class,
      java.math.BigInteger.class, java.net.URI.class, java.net.URL.class, java.io.File.class));

  /** For subclasses. Applications normally use {@link SpecificData#get()}. */
  public SpecificData() {
  }

  /** Construct with a specific classloader. */
  public SpecificData(ClassLoader classLoader) {
    super(classLoader);
  }

  @Override
  public DatumReader createDatumReader(Schema schema) {
    return createDatumReader(schema, schema);
  }

  @Override
  public DatumReader createDatumReader(Schema writer, Schema reader) {
    return new SpecificDatumReader(writer, reader, this);
  }

  @Override
  public DatumWriter createDatumWriter(Schema schema) {
    return new SpecificDatumWriter(schema, this);
  }

  /** Return the singleton instance. */
  public static SpecificData get() {
    return INSTANCE;
  }

  /**
   * For RECORD type schemas, this method returns the SpecificData instance of the
   * class associated with the schema, in order to get the right conversions for
   * any logical types used.
   *
   * @param reader the reader schema
   * @return the SpecificData associated with the schema's class, or the default
   *         instance.
   */
  public static SpecificData getForSchema(Schema reader) {
    if (reader != null && reader.getType() == Type.RECORD) {
      final Class<?> clazz = SpecificData.get().getClass(reader);
      if (clazz != null) {
        return getForClass(clazz);
      }
    }
    return SpecificData.get();
  }

  /**
   * If the given class is assignable to {@link SpecificRecordBase}, this method
   * returns the SpecificData instance from the field {@code MODEL$}, in order to
   * get the correct {@link org.apache.avro.Conversion} instances for the class.
   * Falls back to the default instance {@link SpecificData#get()} for other
   * classes or if the field is not found.
   *
   * @param c   A class
   * @param <T> .
   * @return The SpecificData from the SpecificRecordBase instance, or the default
   *         SpecificData instance.
   */
  public static <T> SpecificData getForClass(Class<T> c) {
    if (SpecificRecordBase.class.isAssignableFrom(c)) {
      return MODEL_CACHE.apply(c);
    }
    return SpecificData.get();
  }

  private boolean useCustomCoderFlag = Boolean
      .parseBoolean(System.getProperty("org.apache.avro.specific.use_custom_coders", "false"));

  /**
   * Retrieve the current value of the custom-coders feature flag. Defaults to
   * <code>true</code>, but this default can be overriden using the system
   * property <code>org.apache.avro.specific.use_custom_coders</code>, and can be
   * set dynamically by {@link SpecificData#useCustomCoders()}. See <a
   * href="https://avro.apache.org/docs/current/gettingstartedjava.html#Beta+feature:+Generating+faster+code"Getting
   * started with Java</a> for more about this feature flag.
   */
  public boolean useCustomCoders() {
    return useCustomCoderFlag;
  }

  /**
   * Dynamically set the value of the custom-coder feature flag. See
   * {@link SpecificData#useCustomCoders()}.
   */
  public void setCustomCoders(boolean flag) {
    useCustomCoderFlag = flag;
  }

  @Override
  protected boolean isEnum(Object datum) {
    return datum instanceof Enum || super.isEnum(datum);
  }

  @Override
  public Object createEnum(String symbol, Schema schema) {
    Class c = getClass(schema);
    if (c == null)
      return super.createEnum(symbol, schema); // punt to generic
    if (RESERVED_WORDS.contains(symbol))
      symbol += "$";
    return Enum.valueOf(c, symbol);
  }

  @Override
  protected Schema getEnumSchema(Object datum) {
    return (datum instanceof Enum) ? getSchema(datum.getClass()) : super.getEnumSchema(datum);
  }

  private Map<String, Class> classCache = new ConcurrentHashMap<>();

  private static final Class NO_CLASS = new Object() {
  }.getClass();
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);

  /** Undoes mangling for reserved words. */
  protected static String unmangle(String word) {
    while (word.endsWith("$")) {
      word = word.substring(0, word.length() - 1);
    }
    return word;
  }

  /** Return the class that implements a schema, or null if none exists. */
  public Class getClass(Schema schema) {
    switch (schema.getType()) {
    case FIXED:
    case RECORD:
    case ENUM:
      String name = schema.getFullName();
      if (name == null)
        return null;
      Class<?> c = classCache.computeIfAbsent(name, n -> {
        try {
          return ClassUtils.forName(getClassLoader(), getClassName(schema));
        } catch (ClassNotFoundException e) {
          // This might be a nested namespace. Try using the last tokens in the
          // namespace as an enclosing class by progressively replacing period
          // delimiters with $
          StringBuilder nestedName = new StringBuilder(n);
          int lastDot = n.lastIndexOf('.');
          while (lastDot != -1) {
            nestedName.setCharAt(lastDot, '$');
            try {
              return ClassUtils.forName(getClassLoader(), nestedName.toString());
            } catch (ClassNotFoundException ignored) {
            }
            lastDot = n.lastIndexOf('.', lastDot - 1);
          }
          return NO_CLASS;
        }
      });
      return c == NO_CLASS ? null : c;
    case ARRAY:
      return List.class;
    case MAP:
      return Map.class;
    case UNION:
      List<Schema> types = schema.getTypes(); // elide unions with null
      if ((types.size() == 2) && types.contains(NULL_SCHEMA))
        return getWrapper(types.get(types.get(0).equals(NULL_SCHEMA) ? 1 : 0));
      return Object.class;
    case STRING:
      if (STRING_TYPE_STRING.equals(schema.getProp(STRING_PROP)))
        return String.class;
      return CharSequence.class;
    case BYTES:
      return ByteBuffer.class;
    case INT:
      return Integer.TYPE;
    case LONG:
      return Long.TYPE;
    case FLOAT:
      return Float.TYPE;
    case DOUBLE:
      return Double.TYPE;
    case BOOLEAN:
      return Boolean.TYPE;
    case NULL:
      return Void.TYPE;
    default:
      throw new AvroRuntimeException("Unknown type: " + schema);
    }
  }

  private Class getWrapper(Schema schema) {
    switch (schema.getType()) {
    case INT:
      return Integer.class;
    case LONG:
      return Long.class;
    case FLOAT:
      return Float.class;
    case DOUBLE:
      return Double.class;
    case BOOLEAN:
      return Boolean.class;
    default:
      return getClass(schema);
    }
  }

  /** Returns the Java class name indicated by a schema's name and namespace. */
  public static String getClassName(Schema schema) {
    String namespace = schema.getNamespace();
    String name = schema.getName();
    if (namespace == null || "".equals(namespace))
      return name;
    String dot = namespace.endsWith("$") ? "" : "."; // back-compatibly handle $
    return namespace + dot + name;
  }

  // cache for schemas created from Class objects. Use ClassValue to avoid
  // locking classloaders and is GC and thread safe.
  private final ClassValueCache<Schema> schemaClassCache = new ClassValueCache<>(c -> createSchema(c, new HashMap<>()));
  // for non-class objects, use a WeakHashMap, but this needs a sync block around
  // it
  private final Map<java.lang.reflect.Type, Schema> schemaTypeCache = Collections.synchronizedMap(new WeakHashMap<>());

  /** Find the schema for a Java type. */
  public Schema getSchema(java.lang.reflect.Type type) {
    try {
      if (type instanceof Class) {
        return schemaClassCache.apply((Class<?>) type);
      }
      return schemaTypeCache.computeIfAbsent(type, t -> createSchema(t, new HashMap<>()));
    } catch (Exception e) {
      throw (e instanceof AvroRuntimeException) ? (AvroRuntimeException) e : new AvroRuntimeException(e);
    }
  }

  /** Create the schema for a Java type. */
  @SuppressWarnings(value = "unchecked")
  protected Schema createSchema(java.lang.reflect.Type type, Map<String, Schema> names) {
    if (type instanceof Class && CharSequence.class.isAssignableFrom((Class) type))
      return Schema.create(Type.STRING);
    else if (type == ByteBuffer.class)
      return Schema.create(Type.BYTES);
    else if ((type == Integer.class) || (type == Integer.TYPE))
      return Schema.create(Type.INT);
    else if ((type == Long.class) || (type == Long.TYPE))
      return Schema.create(Type.LONG);
    else if ((type == Float.class) || (type == Float.TYPE))
      return Schema.create(Type.FLOAT);
    else if ((type == Double.class) || (type == Double.TYPE))
      return Schema.create(Type.DOUBLE);
    else if ((type == Boolean.class) || (type == Boolean.TYPE))
      return Schema.create(Type.BOOLEAN);
    else if ((type == Void.class) || (type == Void.TYPE))
      return Schema.create(Type.NULL);
    else if (type instanceof ParameterizedType) {
      ParameterizedType ptype = (ParameterizedType) type;
      Class raw = (Class) ptype.getRawType();
      java.lang.reflect.Type[] params = ptype.getActualTypeArguments();
      if (Collection.class.isAssignableFrom(raw)) { // array
        if (params.length != 1)
          throw new AvroTypeException("No array type specified.");
        return Schema.createArray(createSchema(params[0], names));
      } else if (Map.class.isAssignableFrom(raw)) { // map
        java.lang.reflect.Type key = params[0];
        java.lang.reflect.Type value = params[1];
        if (!(key instanceof Class && CharSequence.class.isAssignableFrom((Class) key)))
          throw new AvroTypeException("Map key class not CharSequence: " + key);
        return Schema.createMap(createSchema(value, names));
      } else {
        return createSchema(raw, names);
      }
    } else if (type instanceof Class) { // class
      Class c = (Class) type;
      String fullName = c.getName();
      Schema schema = names.get(fullName);
      if (schema == null)
        try {
          schema = (Schema) (c.getDeclaredField("SCHEMA$").get(null));

          if (!fullName.equals(getClassName(schema)))
            // HACK: schema mismatches class. maven shade plugin? try replacing.
            schema = new Schema.Parser()
                .parse(schema.toString().replace(schema.getNamespace(), c.getPackage().getName()));
        } catch (NoSuchFieldException e) {
          throw new AvroRuntimeException("Not a Specific class: " + c);
        } catch (IllegalAccessException e) {
          throw new AvroRuntimeException(e);
        }
      names.put(fullName, schema);
      return schema;
    }
    throw new AvroTypeException("Unknown type: " + type);
  }

  @Override
  protected String getSchemaName(Object datum) {
    if (datum != null) {
      Class c = datum.getClass();
      if (isStringable(c))
        return Schema.Type.STRING.getName();
    }
    return super.getSchemaName(datum);
  }

  /** True if a class should be serialized with toString(). */
  protected boolean isStringable(Class<?> c) {
    return stringableClasses.contains(c);
  }

  /** True if a class IS a string type */
  protected boolean isStringType(Class<?> c) {
    // this will return true for String, Utf8, CharSequence
    return CharSequence.class.isAssignableFrom(c);
  }

  /** Return the protocol for a Java interface. */
  public Protocol getProtocol(Class iface) {
    try {
      Protocol p = (Protocol) (iface.getDeclaredField("PROTOCOL").get(null));
      if (!p.getNamespace().equals(iface.getPackage().getName()))
        // HACK: protocol mismatches iface. maven shade plugin? try replacing.
        p = Protocol.parse(p.toString().replace(p.getNamespace(), iface.getPackage().getName()));
      return p;
    } catch (NoSuchFieldException e) {
      throw new AvroRuntimeException("Not a Specific protocol: " + iface);
    } catch (IllegalAccessException e) {
      throw new AvroRuntimeException(e);
    }
  }

  @Override
  protected int compare(Object o1, Object o2, Schema s, boolean eq) {
    switch (s.getType()) {
    case ENUM:
      if (o1 instanceof Enum)
        return ((Enum) o1).ordinal() - ((Enum) o2).ordinal();
    default:
      return super.compare(o1, o2, s, eq);
    }
  }

  /**
   * Create an instance of a class. If the class implements
   * {@link SchemaConstructable}, call a constructor with a
   * {@link org.apache.avro.Schema} parameter, otherwise use a no-arg constructor.
   */
  @SuppressWarnings("unchecked")
  public static Object newInstance(Class c, Schema s) {
    boolean useSchema = SchemaConstructable.class.isAssignableFrom(c);
    Object result;
    try {
      Constructor<?> meth = CTOR_CACHE.apply(c);
      result = meth.newInstance(useSchema ? new Object[] { s } : null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public Object createFixed(Object old, Schema schema) {
    Class c = getClass(schema);
    if (c == null)
      return super.createFixed(old, schema); // punt to generic
    return c.isInstance(old) ? old : newInstance(c, schema);
  }

  @Override
  public Object newRecord(Object old, Schema schema) {
    Class c = getClass(schema);
    if (c == null)
      return super.newRecord(old, schema); // punt to generic
    return (c.isInstance(old) ? old : newInstance(c, schema));
  }

  @SuppressWarnings("rawtypes")
  @Override
  /**
   * Create an InstanceSupplier that caches all information required for the
   * creation of a schema record instance rather than having to look them up for
   * each call (as newRecord would)
   */
  public InstanceSupplier getNewRecordSupplier(Schema schema) {
    Class c = getClass(schema);
    if (c == null) {
      return super.getNewRecordSupplier(schema);
    }

    boolean useSchema = SchemaConstructable.class.isAssignableFrom(c);
    Constructor<?> meth = CTOR_CACHE.apply(c);
    Object[] params = useSchema ? new Object[] { schema } : (Object[]) null;

    return (old, sch) -> {
      try {
        return c.isInstance(old) ? old : meth.newInstance(params);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    };
  }

  /**
   * Tag interface that indicates that a class has a one-argument constructor that
   * accepts a Schema.
   *
   * @see #newInstance
   */
  public interface SchemaConstructable {
  }

  /** Runtime utility used by generated classes. */
  public static BinaryDecoder getDecoder(ObjectInput in) {
    return DecoderFactory.get().directBinaryDecoder(new ExternalizableInput(in), null);
  }

  /** Runtime utility used by generated classes. */
  public static BinaryEncoder getEncoder(ObjectOutput out) {
    return EncoderFactory.get().directBinaryEncoder(new ExternalizableOutput(out), null);
  }

  @Override
  public Object createString(Object value) {
    // Many times the use is String.Priority processing
    if (value instanceof String) {
      return value;
    } else if (isStringable(value.getClass())) {
      return value;
    }
    return super.createString(value);
  }

}
