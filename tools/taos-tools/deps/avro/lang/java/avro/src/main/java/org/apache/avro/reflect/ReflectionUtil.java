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

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A few utility methods for using @link{java.misc.Unsafe}, mostly for private
 * use.
 *
 * Use of Unsafe on Android is forbidden, as Android provides only a very
 * limited functionality for this class compared to the JDK version.
 *
 * InterfaceAudience.Private
 */
public class ReflectionUtil {

  private ReflectionUtil() {
  }

  private static FieldAccess fieldAccess;
  static {
    resetFieldAccess();
  }

  static void resetFieldAccess() {
    // load only one implementation of FieldAccess
    // so it is monomorphic and the JIT can inline
    FieldAccess access = null;
    try {
      if (null == System.getProperty("avro.disable.unsafe")) {
        FieldAccess unsafeAccess = load("org.apache.avro.reflect.FieldAccessUnsafe", FieldAccess.class);
        if (validate(unsafeAccess)) {
          access = unsafeAccess;
        }
      }
    } catch (Throwable ignored) {
    }
    if (access == null) {
      try {
        FieldAccess reflectAccess = load("org.apache.avro.reflect.FieldAccessReflect", FieldAccess.class);
        if (validate(reflectAccess)) {
          access = reflectAccess;
        }
      } catch (Throwable oops) {
        throw new AvroRuntimeException("Unable to load a functional FieldAccess class!");
      }
    }
    fieldAccess = access;
  }

  private static <T> T load(String name, Class<T> type) throws Exception {
    return ReflectionUtil.class.getClassLoader().loadClass(name).asSubclass(type).getDeclaredConstructor()
        .newInstance();
  }

  public static FieldAccess getFieldAccess() {
    return fieldAccess;
  }

  private static boolean validate(FieldAccess access) throws Exception {
    return new AccessorTestClass().validate(access);
  }

  private static final class AccessorTestClass {
    private boolean b = true;
    protected byte by = 0xf;
    public char c = 'c';
    short s = 123;
    int i = 999;
    long l = 12345L;
    float f = 2.2f;
    double d = 4.4d;
    Object o = "foo";
    Integer i2 = 555;

    private boolean validate(FieldAccess access) throws Exception {
      boolean valid = true;
      valid &= validField(access, "b", b, false);
      valid &= validField(access, "by", by, (byte) 0xaf);
      valid &= validField(access, "c", c, 'C');
      valid &= validField(access, "s", s, (short) 321);
      valid &= validField(access, "i", i, 111);
      valid &= validField(access, "l", l, 54321L);
      valid &= validField(access, "f", f, 0.2f);
      valid &= validField(access, "d", d, 0.4d);
      valid &= validField(access, "o", o, new Object());
      valid &= validField(access, "i2", i2, -555);
      return valid;
    }

    private boolean validField(FieldAccess access, String name, Object original, Object toSet) throws Exception {
      FieldAccessor a;
      boolean valid = true;
      a = accessor(access, name);
      valid &= original.equals(a.get(this));
      a.set(this, toSet);
      valid &= !original.equals(a.get(this));
      return valid;
    }

    private FieldAccessor accessor(FieldAccess access, String name) throws Exception {
      return access.getAccessor(this.getClass().getDeclaredField(name));
    }
  }

  /**
   * For an interface, get a map of any {@link TypeVariable}s to their actual
   * types.
   *
   * @param iface interface to resolve types for.
   * @return a map of {@link TypeVariable}s to actual types.
   */
  protected static Map<TypeVariable<?>, Type> resolveTypeVariables(Class<?> iface) {
    return resolveTypeVariables(iface, new IdentityHashMap<>());
  }

  private static Map<TypeVariable<?>, Type> resolveTypeVariables(Class<?> iface, Map<TypeVariable<?>, Type> reuse) {

    for (Type type : iface.getGenericInterfaces()) {
      if (type instanceof ParameterizedType) {
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Type rawType = parameterizedType.getRawType();
        if (rawType instanceof Class<?>) {
          Class<?> classType = (Class<?>) rawType;
          TypeVariable<? extends Class<?>>[] typeParameters = classType.getTypeParameters();
          Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
          for (int i = 0; i < typeParameters.length; i++) {
            reuse.putIfAbsent(typeParameters[i], reuse.getOrDefault(actualTypeArguments[i], actualTypeArguments[i]));
          }
          resolveTypeVariables(classType, reuse);
        }
      }
    }
    return reuse;
  }

  private static <D> Supplier<D> getConstructorAsSupplier(Class<D> clazz) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandle constructorHandle = lookup.findConstructor(clazz, MethodType.methodType(void.class));

      CallSite site = LambdaMetafactory.metafactory(lookup, "get", MethodType.methodType(Supplier.class),
          constructorHandle.type().generic(), constructorHandle, constructorHandle.type());

      return (Supplier<D>) site.getTarget().invokeExact();
    } catch (Throwable t) {
      // if anything goes wrong, don't provide a Supplier
      return null;
    }
  }

  private static <V, R> Supplier<R> getOneArgConstructorAsSupplier(Class<R> clazz, Class<V> argumentClass, V argument) {
    Function<V, R> supplierFunction = getConstructorAsFunction(argumentClass, clazz);
    if (supplierFunction != null) {
      return () -> supplierFunction.apply(argument);
    } else {
      return null;
    }
  }

  public static <V, R> Function<V, R> getConstructorAsFunction(Class<V> parameterClass, Class<R> clazz) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      MethodHandle constructorHandle = lookup.findConstructor(clazz, MethodType.methodType(void.class, parameterClass));

      CallSite site = LambdaMetafactory.metafactory(lookup, "apply", MethodType.methodType(Function.class),
          constructorHandle.type().generic(), constructorHandle, constructorHandle.type());

      return (Function<V, R>) site.getTarget().invokeExact();
    } catch (Throwable t) {
      // if something goes wrong, do not provide a Function instance
      return null;
    }
  }

}
