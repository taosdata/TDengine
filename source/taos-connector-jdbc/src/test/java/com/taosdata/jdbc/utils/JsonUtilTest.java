package com.taosdata.jdbc.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;

public class JsonUtilTest {
    private static final ObjectMapper objectMapper = JsonUtil.getObjectMapper();

    @Test
    public void testIgnoreUnknownProperties() throws Exception {
        String json = "{\"knownProperty\":\"value\", \"unknownProperty\":\"value\"}";
        TestKnownPropertyClass result = objectMapper.readValue(json, TestKnownPropertyClass.class);
        Assert.assertEquals("value", result.knownProperty);
    }

    @Test
    public void testSerializationWithoutNull() throws Exception {
        TestNullPropertyClass testClass = new TestNullPropertyClass();
        testClass.name = "John";
        testClass.age = null;
        String json = objectMapper.writeValueAsString(testClass);
        Assert.assertFalse(json.contains("age"));
    }

    @Test
    public void testDateFormat() throws Exception {
        TestClassWithDate testClass = new TestClassWithDate();
        testClass.date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2023-10-01 12:00:00");
        String json = objectMapper.writeValueAsString(testClass);
        Assert.assertTrue(json.contains("\"date\":\"2023-10-01 12:00:00\""));
    }

    @Test
    public void testJavaTimeModule() throws Exception {
        String json = "{\"localDateTime\":\"2023-10-01T12:00:00\"}";
        TestClassWithJavaTime result = objectMapper.readValue(json, TestClassWithJavaTime.class);
        Assert.assertEquals("2023-10-01T12:00", result.localDateTime.toString());
    }

    // 测试类
    static class TestKnownPropertyClass {
        @JsonProperty("knownProperty")
        public String knownProperty;
    }

    static class TestClassWithDate {
        @JsonProperty("date")
        public java.util.Date date;
    }

    static class TestClassWithArray {
        @JsonProperty("values")
        public String[] values;
    }

    static class TestClassWithJavaTime {
        @JsonProperty("localDateTime")
        public java.time.LocalDateTime localDateTime;
    }
    static class TestNullPropertyClass {
        @JsonProperty("name")
        public String name;
        @JsonProperty("age")
        public Integer age;
    }
}