package com.taosdata.utils;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TypeNameResolver {

    private static final Map<Class<?>, String> TYPE_MAPPING = new HashMap<>();

    static {
        // 基本类型映射
        TYPE_MAPPING.put(boolean.class, "bool");
        TYPE_MAPPING.put(Boolean.class, "bool");

        TYPE_MAPPING.put(byte.class, "int");
        TYPE_MAPPING.put(Byte.class, "int");
        TYPE_MAPPING.put(short.class, "int");
        TYPE_MAPPING.put(Short.class, "int");
        TYPE_MAPPING.put(int.class, "int");
        TYPE_MAPPING.put(Integer.class, "int");

        TYPE_MAPPING.put(long.class, "long");
        TYPE_MAPPING.put(Long.class, "long");

        TYPE_MAPPING.put(float.class, "float");
        TYPE_MAPPING.put(Float.class, "float");

        TYPE_MAPPING.put(double.class, "double");
        TYPE_MAPPING.put(Double.class, "double");
        // 字符串和日期类型
        TYPE_MAPPING.put(String.class, "string");
        TYPE_MAPPING.put(Date.class, "date");
        TYPE_MAPPING.put(LocalDate.class, "date");
        TYPE_MAPPING.put(LocalDateTime.class, "date");
    }

    /**
     * 获取 Object 的类型名字符串
     * @param value 任意对象，可以为 null
     * @return 类型名字符串（null 返回 "string"）
     */
    public static String resolveTypeName(Object value) {
        if (value == null) return "string";
        // 先尝试快速映射
        String typeName = TYPE_MAPPING.get(value.getClass());
        if (typeName != null) {
            return typeName;
        }
        // 未命中再使用 instanceof 判断
        return resolveTypeNameAdvanced(value);
    }

    /**
     * 处理复杂类型
     */
    private static String resolveComplexType(Object value) {
        // 处理枚举类型
        if (value instanceof Enum) {
            return "string";
        }
        // 处理集合类型
        if (value instanceof Iterable) {
            return "array";
        }
        // 处理 Map 类型
        if (value instanceof Map) {
            return "object";
        }
        // 默认返回类名的小写形式
        return "string"; // 保守起见，未知类型返回 string
    }

    /**
     * instanceof 判断
     */
    public static String resolveTypeNameAdvanced(Object value) {
        if (value == null) {
            return "string";
        }
        // 按优先级顺序判断
        if (value instanceof Boolean) {
            return "bool";
        }
        if (value instanceof Number) {
            if (value instanceof Integer || value instanceof Byte || value instanceof Short) {
                return "int";
            }
            if (value instanceof Long) {
                return "long";
            }
            if (value instanceof Float) {
                return "float";
            }
            if (value instanceof Double) {
                return "double";
            }
            // BigDecimal, BigInteger 等
            return "string";
        }
        if (value instanceof String) {
            return "string";
        }
        // 日期类型判断
        if (value instanceof Date ||
                value instanceof LocalDate ||
                value instanceof LocalDateTime ||
                value instanceof java.sql.Date ||
                value instanceof java.sql.Timestamp) {
            return "date";
        }
        // 其他复杂类型处理
        return resolveComplexType(value);
    }
}