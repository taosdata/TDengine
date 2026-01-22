package com.taosdata.utils;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TypeNameResolver 单元测试
 *
 * @author Test
 */
class TypeNameResolverTest {

    @Test
    void testResolveTypeNameWithNull() {
        // 测试 null 值
        String typeName = TypeNameResolver.resolveTypeName(null);
        assertEquals("string", typeName);
    }

    @Test
    void testResolveTypeNameWithBoolean() {
        // 测试 boolean 基本类型和包装类型
        assertEquals("bool", TypeNameResolver.resolveTypeName(true));
        assertEquals("bool", TypeNameResolver.resolveTypeName(false));
        assertEquals("bool", TypeNameResolver.resolveTypeName(Boolean.TRUE));
        assertEquals("bool", TypeNameResolver.resolveTypeName(Boolean.FALSE));
    }

    @Test
    void testResolveTypeNameWithInt() {
        // 测试 int 基本类型和包装类型
        assertEquals("int", TypeNameResolver.resolveTypeName(123));
        assertEquals("int", TypeNameResolver.resolveTypeName(Integer.valueOf(456)));
        assertEquals("int", TypeNameResolver.resolveTypeName((byte) 10));
        assertEquals("int", TypeNameResolver.resolveTypeName(Byte.valueOf((byte) 20)));
        assertEquals("int", TypeNameResolver.resolveTypeName((short) 30));
        assertEquals("int", TypeNameResolver.resolveTypeName(Short.valueOf((short) 40)));
    }

    @Test
    void testResolveTypeNameWithLong() {
        // 测试 long 基本类型和包装类型
        assertEquals("long", TypeNameResolver.resolveTypeName(123L));
        assertEquals("long", TypeNameResolver.resolveTypeName(Long.valueOf(456L)));
    }

    @Test
    void testResolveTypeNameWithFloat() {
        // 测试 float 基本类型和包装类型
        assertEquals("float", TypeNameResolver.resolveTypeName(1.23f));
        assertEquals("float", TypeNameResolver.resolveTypeName(Float.valueOf(4.56f)));
    }

    @Test
    void testResolveTypeNameWithDouble() {
        // 测试 double 基本类型和包装类型
        assertEquals("double", TypeNameResolver.resolveTypeName(1.23));
        assertEquals("double", TypeNameResolver.resolveTypeName(Double.valueOf(4.56)));
    }

    @Test
    void testResolveTypeNameWithString() {
        // 测试 String 类型
        assertEquals("string", TypeNameResolver.resolveTypeName("test"));
        assertEquals("string", TypeNameResolver.resolveTypeName(""));
    }

    @Test
    void testResolveTypeNameWithDate() {
        // 测试日期类型
        assertEquals("date", TypeNameResolver.resolveTypeName(new Date()));
        assertEquals("date", TypeNameResolver.resolveTypeName(LocalDate.now()));
        assertEquals("date", TypeNameResolver.resolveTypeName(LocalDateTime.now()));
        assertEquals("date", TypeNameResolver.resolveTypeName(new java.sql.Date(System.currentTimeMillis())));
        assertEquals("date", TypeNameResolver.resolveTypeName(new Timestamp(System.currentTimeMillis())));
    }

    // 测试枚举类型
    enum TestEnum {
        VALUE1, VALUE2
    }
    @Test
    void testResolveTypeNameWithEnum() {
        assertEquals("string", TypeNameResolver.resolveTypeName(TestEnum.VALUE1));
    }

    @Test
    void testResolveTypeNameWithIterable() {
        // 测试集合类型
        assertEquals("array", TypeNameResolver.resolveTypeName(new ArrayList<>()));
        assertEquals("array", TypeNameResolver.resolveTypeName(new LinkedList<>()));
        assertEquals("array", TypeNameResolver.resolveTypeName(Arrays.asList(1, 2, 3)));
    }

    @Test
    void testResolveTypeNameWithMap() {
        // 测试 Map 类型
        assertEquals("object", TypeNameResolver.resolveTypeName(new HashMap<>()));
        assertEquals("object", TypeNameResolver.resolveTypeName(new LinkedHashMap<>()));
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value");
        assertEquals("object", TypeNameResolver.resolveTypeName(map));
    }

    @Test
    void testResolveTypeNameWithBigNumber() {
        // 测试 BigDecimal 和 BigInteger，应该返回 string
        assertEquals("string", TypeNameResolver.resolveTypeName(BigDecimal.valueOf(123.45)));
        assertEquals("string", TypeNameResolver.resolveTypeName(BigInteger.valueOf(123456)));
    }

    @Test
    void testResolveTypeNameAdvancedWithNull() {
        // 测试 resolveTypeNameAdvanced 的 null 值
        String typeName = TypeNameResolver.resolveTypeNameAdvanced(null);
        assertEquals("string", typeName);
    }

    @Test
    void testResolveTypeNameAdvancedWithBoolean() {
        // 测试 resolveTypeNameAdvanced 的 Boolean 类型
        assertEquals("bool", TypeNameResolver.resolveTypeNameAdvanced(true));
        assertEquals("bool", TypeNameResolver.resolveTypeNameAdvanced(Boolean.FALSE));
    }

    @Test
    void testResolveTypeNameAdvancedWithNumber() {
        // 测试 resolveTypeNameAdvanced 的 Number 类型
        assertEquals("int", TypeNameResolver.resolveTypeNameAdvanced(Integer.valueOf(123)));
        assertEquals("int", TypeNameResolver.resolveTypeNameAdvanced(Byte.valueOf((byte) 10)));
        assertEquals("int", TypeNameResolver.resolveTypeNameAdvanced(Short.valueOf((short) 30)));
        assertEquals("long", TypeNameResolver.resolveTypeNameAdvanced(Long.valueOf(123L)));
        assertEquals("float", TypeNameResolver.resolveTypeNameAdvanced(Float.valueOf(1.23f)));
        assertEquals("double", TypeNameResolver.resolveTypeNameAdvanced(Double.valueOf(4.56)));
        assertEquals("string", TypeNameResolver.resolveTypeNameAdvanced(BigDecimal.valueOf(123.45)));
        assertEquals("string", TypeNameResolver.resolveTypeNameAdvanced(BigInteger.valueOf(123456)));
    }

    @Test
    void testResolveTypeNameAdvancedWithString() {
        // 测试 resolveTypeNameAdvanced 的 String 类型
        assertEquals("string", TypeNameResolver.resolveTypeNameAdvanced("test"));
    }

    @Test
    void testResolveTypeNameAdvancedWithDate() {
        // 测试 resolveTypeNameAdvanced 的日期类型
        assertEquals("date", TypeNameResolver.resolveTypeNameAdvanced(new Date()));
        assertEquals("date", TypeNameResolver.resolveTypeNameAdvanced(LocalDate.now()));
        assertEquals("date", TypeNameResolver.resolveTypeNameAdvanced(LocalDateTime.now()));
        assertEquals("date", TypeNameResolver.resolveTypeNameAdvanced(new java.sql.Date(System.currentTimeMillis())));
        assertEquals("date", TypeNameResolver.resolveTypeNameAdvanced(new Timestamp(System.currentTimeMillis())));
    }


    @Test
    void testResolveTypeNameAdvancedWithEnum() {
        assertEquals("string", TypeNameResolver.resolveTypeNameAdvanced(TestEnum.VALUE1));
    }

    @Test
    void testResolveTypeNameAdvancedWithIterable() {
        // 测试 resolveTypeNameAdvanced 的集合类型
        assertEquals("array", TypeNameResolver.resolveTypeNameAdvanced(new ArrayList<>()));
        assertEquals("array", TypeNameResolver.resolveTypeNameAdvanced(Arrays.asList("a", "b")));
    }

    @Test
    void testResolveTypeNameAdvancedWithMap() {
        // 测试 resolveTypeNameAdvanced 的 Map 类型
        assertEquals("object", TypeNameResolver.resolveTypeNameAdvanced(new HashMap<>()));
        Map<String, String> map = new LinkedHashMap<>();
        map.put("test", "value");
        assertEquals("object", TypeNameResolver.resolveTypeNameAdvanced(map));
    }

    @Test
    void testResolveTypeNameAdvancedWithUnknownType() {
        // 测试 resolveTypeNameAdvanced 的未知类型
        class UnknownClass {
            private String name = "unknown";
        }
        assertEquals("string", TypeNameResolver.resolveTypeNameAdvanced(new UnknownClass()));
    }

    @Test
    void testConsistencyBetweenResolveAndAdvanced() {
        // 测试 resolveTypeName 和 resolveTypeNameAdvanced 的一致性
        Object[] testValues = {
                null,
                true,
                123,
                123L,
                1.23f,
                4.56,
                "test",
                new Date(),
                LocalDate.now(),
                LocalDateTime.now()
        };

        for (Object value : testValues) {
            String type1 = TypeNameResolver.resolveTypeName(value);
            String type2 = TypeNameResolver.resolveTypeNameAdvanced(value);
            assertEquals(type1, type2, "Type mismatch for value: " + value);
        }
    }
}
