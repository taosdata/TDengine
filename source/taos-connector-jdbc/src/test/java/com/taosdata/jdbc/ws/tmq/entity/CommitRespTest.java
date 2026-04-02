package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.taosdata.jdbc.utils.UInt64Deserializer;
import com.taosdata.jdbc.ws.entity.CommonResp;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CommitRespTest {
    private CommitResp commitResp;
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        commitResp = new CommitResp();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testDefaultValues() {
        assertEquals(0L, commitResp.getMessageId());
    }

    @Test
    public void testMessageIdGetterAndSetter() {
        long expectedMessageId = 123456789L;
        commitResp.setMessageId(expectedMessageId);
        assertEquals(expectedMessageId, commitResp.getMessageId());

        commitResp.setMessageId(-100L);
        assertEquals(-100L, commitResp.getMessageId());

        commitResp.setMessageId(0L);
        assertEquals(0L, commitResp.getMessageId());
    }

    @Test
    public void testJsonPropertyAnnotation() throws Exception {
        commitResp.setMessageId(999L);

        String json = objectMapper.writeValueAsString(commitResp);
        assertTrue(json.contains("\"stmt_id\":999"));
    }

    @Test
    public void testJsonDeserializeAnnotation() throws Exception {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Long.class, new UInt64Deserializer());
        module.addDeserializer(long.class, new UInt64Deserializer());
        objectMapper.registerModule(module);

        String json = "{\"stmt_id\":18446744073709551615}";
        CommitResp deserialized = objectMapper.readValue(json, CommitResp.class);

        assertEquals(-1L, deserialized.getMessageId());
    }

    @Test
    public void testInheritanceFromCommonResp() {
        assertNotNull(commitResp);
        assertTrue(commitResp instanceof CommonResp);
    }

    @Test
    public void testBoundaryValues() {
        commitResp.setMessageId(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, commitResp.getMessageId());

        commitResp.setMessageId(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, commitResp.getMessageId());
    }

    @Test
    public void testSerializationAndDeserializationCycle() throws Exception {
        CommitResp original = new CommitResp();
        original.setMessageId(987654321L);

        String json = objectMapper.writeValueAsString(original);
        CommitResp deserialized = objectMapper.readValue(json, CommitResp.class);

        assertEquals(original.getMessageId(), deserialized.getMessageId());
    }

    @Test
    public void testJsonDeserializeWithMissingField() throws Exception {
        String json = "{}";
        CommitResp deserialized = objectMapper.readValue(json, CommitResp.class);

        assertEquals(0L, deserialized.getMessageId());
    }

    @Test
    public void testCustomUInt64DeserializerIntegration() throws Exception {
        SimpleModule module = new SimpleModule();
        UInt64Deserializer deserializer = new UInt64Deserializer();
        module.addDeserializer(Long.class, deserializer);
        module.addDeserializer(long.class, deserializer);
        objectMapper.registerModule(module);

        // Verify the deserializer is being used by checking the class has the annotation
        assertNotNull(CommitResp.class.getDeclaredField("messageId")
                .getAnnotation(com.fasterxml.jackson.databind.annotation.JsonDeserialize.class));
    }

    @Test
    public void testMultipleInstances() {
        CommitResp resp1 = new CommitResp();
        resp1.setMessageId(100L);

        CommitResp resp2 = new CommitResp();
        resp2.setMessageId(200L);

        assertNotEquals(resp1.getMessageId(), resp2.getMessageId());

        resp2.setMessageId(100L);
        assertEquals(resp1.getMessageId(), resp2.getMessageId());
    }

    @Test
    public void testJsonPropertyNameConsistency() throws Exception {
        // Ensure the JSON property name matches the annotation
        commitResp.setMessageId(555L);
        String json = objectMapper.writeValueAsString(commitResp);

        // The JSON should contain the exact property name from @JsonProperty
        assertTrue(json.contains("\"stmt_id\""));
        assertFalse(json.contains("\"messageId\""));
    }
}