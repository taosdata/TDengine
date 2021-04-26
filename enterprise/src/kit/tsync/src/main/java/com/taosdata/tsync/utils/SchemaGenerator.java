package com.taosdata.tsync.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.lang.reflect.Field;

public class SchemaGenerator {

    public static String build(Class clazz) {
        JSONObject schema = new JSONObject();
        schema.put("namespace", clazz.getPackage().getName());
        schema.put("type", "record");
        schema.put("name", clazz.getSimpleName());
        // fields
        JSONArray fields = new JSONArray();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : declaredFields) {
            SerializeIgnore ignore = field.getAnnotation(SerializeIgnore.class);
            if (ignore != null) {
                continue;
            }
            JSONObject f = new JSONObject();

            String name = field.getName();
            String type = getFieldType(field.getType());
            if (type == null)
                continue;
            f.put("name", name);
            JSONArray t = new JSONArray();
            t.add(type);
            t.add("null");
            f.put("type", t);

            fields.add(f);
        }
        schema.put("fields", fields);
        return schema.toJSONString();
    }

    private static String getFieldType(Class type) {
        if (type == boolean.class || type == Boolean.class)
            return "boolean";
        if (type == byte.class || type == Byte.class || type == short.class || type == Short.class || type == int.class || type == Integer.class)
            return "int";
        if (type == long.class || type == Long.class)
            return "long";
        if (type == float.class || type == Float.class)
            return "float";
        if (type == double.class || type == Double.class)
            return "double";
        if (type == byte[].class)
            return "bytes";
        if (type == String.class)
            return "string";
        return null;
    }

}
