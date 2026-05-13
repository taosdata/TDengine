package com.taosdata.jdbc.tmq;

import com.google.common.collect.Lists;
import com.taosdata.jdbc.TaosGlobalConfig;
import com.taosdata.jdbc.utils.Utils;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class ReferenceDeserializer<V> implements Deserializer<V> {
    private Param[] params;

    @Override
    public void configure(Map<?, ?> configs) {
        Object encodingValue = configs.get(TMQConstants.VALUE_DESERIALIZER_ENCODING);
        if (encodingValue instanceof String)
            TaosGlobalConfig.setCharset(((String) encodingValue).trim());
    }

    @Override
    public V deserialize(ResultSet data, String topic, String dbName) throws DeserializerException, SQLException {

        Class<V> clazz = getGenericType();
        V t = null;
        try {
            t = Utils.newInstance(clazz);
            if (params == null) {
                BeanInfo beanInfo = Introspector.getBeanInfo(clazz);
                List<Param> lists = Lists.newArrayListWithExpectedSize(beanInfo.getPropertyDescriptors().length);
                for (PropertyDescriptor property : beanInfo.getPropertyDescriptors()) {
                    String name = property.getName();
                    if ("class".equals(name))
                        continue;
                    Method method = property.getWriteMethod();
                    if (null != method) {
                        method.setAccessible(true);
                        Param param = new Param();
                        param.name = name;
                        param.method = method;
                        param.clazz = method.getParameterTypes()[0];
                        lists.add(param);
                    }
                }
                params = lists.toArray(new Param[0]);
            }
        } catch (IntrospectionException e) {
            throw new SQLException(this.getClass().getSimpleName() + " get BeanInfo error!", e);
        }


        for (Param param : params) {
            try {
                // string
                if (param.clazz.isAssignableFrom(String.class)) {
                    String string = data.getString(param.name);
                    param.method.invoke(t, data.wasNull() ? null : string);

                    // int
                } else if (param.clazz.isAssignableFrom(Integer.class)) {
                    int i = data.getInt(param.name);
                    param.method.invoke(t, data.wasNull() ? null : i);
                } else if (param.clazz.isAssignableFrom(int.class)) {
                    param.method.invoke(t, data.getInt(param.name));

                    // short
                } else if (param.clazz.isAssignableFrom(Short.class)) {
                    short s = data.getShort(param.name);
                    param.method.invoke(t, data.wasNull() ? null : s);
                } else if (param.clazz.isAssignableFrom(short.class)) {
                    param.method.invoke(t, data.getShort(param.name));

                    // byte
                } else if (param.clazz.isAssignableFrom(Byte.class)) {
                    byte b = data.getByte(param.name);
                    param.method.invoke(t, data.wasNull() ? null : b);
                } else if (param.clazz.isAssignableFrom(byte.class)) {
                    param.method.invoke(t, data.getByte(param.name));

                    // char
                } else if (param.clazz.isAssignableFrom(Character.class)) {
                    char c = (char) data.getByte(param.name);
                    param.method.invoke(t, data.wasNull() ? null : c);
                } else if (param.clazz.isAssignableFrom(char.class)) {
                    param.method.invoke(t, (char) data.getByte(param.name));

                    // float
                } else if (param.clazz.isAssignableFrom(Float.class)) {
                    float f = data.getFloat(param.name);
                    param.method.invoke(t, data.wasNull() ? null : f);
                } else if (param.clazz.isAssignableFrom(float.class)) {
                    param.method.invoke(t, data.getFloat(param.name));

                    // double
                } else if (param.clazz.isAssignableFrom(Double.class)) {
                    double d = data.getDouble(param.name);
                    param.method.invoke(t, data.wasNull() ? null : d);
                } else if (param.clazz.isAssignableFrom(double.class)) {
                    param.method.invoke(t, data.getDouble(param.name));

                    // long
                } else if (param.clazz.isAssignableFrom(Long.class)) {
                    long l = data.getLong(param.name);
                    param.method.invoke(t, data.wasNull() ? null : l);
                } else if (param.clazz.isAssignableFrom(long.class)) {
                    param.method.invoke(t, data.getLong(param.name));

                    // boolean
                } else if (param.clazz.isAssignableFrom(Boolean.class)) {
                    boolean b = data.getBoolean(param.name);
                    param.method.invoke(t, data.wasNull() ? null : b);
                } else if (param.clazz.isAssignableFrom(boolean.class)) {
                    param.method.invoke(t, data.getBoolean(param.name));

                    // timestamp
                } else if (param.clazz.isAssignableFrom(Timestamp.class)) {
                    Timestamp ts = data.getTimestamp(param.name);
                    param.method.invoke(t, data.wasNull() ? null : ts);

                    // bytes
                } else if (param.clazz.isAssignableFrom(Byte[].class)
                        || param.clazz.isAssignableFrom(byte[].class)) {
                    byte[] bytes = data.getBytes(param.name);
                    param.method.invoke(t, data.wasNull() ? null : bytes);
                } else if (param.clazz.isAssignableFrom(BigDecimal.class)) {
                    BigDecimal bigDecimal = data.getBigDecimal(param.name);
                    param.method.invoke(t, data.wasNull() ? null : bigDecimal);
                } else if (param.clazz.isAssignableFrom(BigInteger.class)) {
                    BigInteger bigInteger = (BigInteger) data.getObject(param.name);
                    param.method.invoke(t, data.wasNull() ? null : bigInteger);
                } else if (param.clazz.isAssignableFrom(Blob.class)) {
                    Blob blob = (Blob) data.getObject(param.name);
                    param.method.invoke(t, data.wasNull() ? null : blob);
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new SQLException(this.getClass().getSimpleName() + ": " + param.name
                        + " through method:" + param.method.getName() + " get Data error: ", e);
            }
        }
        return t;
    }

    private Class<V> getGenericType() {
        Type type = getClass().getGenericSuperclass();
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            return (Class<V>) parameterizedType.getActualTypeArguments()[0];
        }
        throw new RuntimeException("ReferenceDeserializer getGenericType error!, maybe not extends ReferenceDeserializer");
    }

    private static class Param {
        String name;
        Method method;
        Class<?> clazz;
    }
}
