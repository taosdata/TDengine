package com.taosdata.jdbc.ws.tmq.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.taosdata.jdbc.ws.entity.CommonResp;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CommitOffsetRespTest {
    private CommitOffsetResp commitOffsetResp;

    @Before
    public void setUp() {
        commitOffsetResp = new CommitOffsetResp();
    }

    @Test
    public void testDefaultValues() {
        assertEquals(0L, commitOffsetResp.getTiming());
        assertNull(commitOffsetResp.getTopic());
        assertEquals(0, commitOffsetResp.getVgId());
        assertEquals(0L, commitOffsetResp.getOffset());
    }

    @Test
    public void testTimingGetterAndSetter() {
        long expectedTiming = 1634567890123L;
        commitOffsetResp.setTiming(expectedTiming);
        assertEquals(expectedTiming, commitOffsetResp.getTiming());
    }

    @Test
    public void testTopicGetterAndSetter() {
        String expectedTopic = "test-topic";
        commitOffsetResp.setTopic(expectedTopic);
        assertEquals(expectedTopic, commitOffsetResp.getTopic());

        commitOffsetResp.setTopic(null);
        assertNull(commitOffsetResp.getTopic());
    }

    @Test
    public void testVgIdGetterAndSetter() {
        int expectedVgId = 42;
        commitOffsetResp.setVgId(expectedVgId);
        assertEquals(expectedVgId, commitOffsetResp.getVgId());

        commitOffsetResp.setVgId(-1);
        assertEquals(-1, commitOffsetResp.getVgId());
    }

    @Test
    public void testOffsetGetterAndSetter() {
        long expectedOffset = 123456789L;
        commitOffsetResp.setOffset(expectedOffset);
        assertEquals(expectedOffset, commitOffsetResp.getOffset());

        commitOffsetResp.setOffset(-100L);
        assertEquals(-100L, commitOffsetResp.getOffset());
    }

    @Test
    public void testJsonPropertyAnnotations() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        // Test serialization
        commitOffsetResp.setTiming(1634567890123L);
        commitOffsetResp.setTopic("test-topic");
        commitOffsetResp.setVgId(5);
        commitOffsetResp.setOffset(12345L);

        String json = mapper.writeValueAsString(commitOffsetResp);
        assertTrue(json.contains("\"timing\":1634567890123"));
        assertTrue(json.contains("\"topic\":\"test-topic\""));
        assertTrue(json.contains("\"vg_id\":5"));
        assertTrue(json.contains("\"offset\":12345"));

        // Test deserialization
        String inputJson = "{\"timing\":987654321000,\"topic\":\"deserialized-topic\",\"vg_id\":7,\"offset\":54321}";
        CommitOffsetResp deserialized = mapper.readValue(inputJson, CommitOffsetResp.class);

        assertEquals(987654321000L, deserialized.getTiming());
        assertEquals("deserialized-topic", deserialized.getTopic());
        assertEquals(7, deserialized.getVgId());
        assertEquals(54321L, deserialized.getOffset());
    }

    @Test
    public void testInheritanceFromCommonResp() {
        assertNotNull(commitOffsetResp);
        assertTrue(commitOffsetResp instanceof CommonResp);
    }

    @Test
    public void testMultipleFieldUpdates() {
        commitOffsetResp.setTiming(1000L);
        commitOffsetResp.setTopic("topic1");
        commitOffsetResp.setVgId(1);
        commitOffsetResp.setOffset(100L);

        assertEquals(1000L, commitOffsetResp.getTiming());
        assertEquals("topic1", commitOffsetResp.getTopic());
        assertEquals(1, commitOffsetResp.getVgId());
        assertEquals(100L, commitOffsetResp.getOffset());

        // Update all fields
        commitOffsetResp.setTiming(2000L);
        commitOffsetResp.setTopic("topic2");
        commitOffsetResp.setVgId(2);
        commitOffsetResp.setOffset(200L);

        assertEquals(2000L, commitOffsetResp.getTiming());
        assertEquals("topic2", commitOffsetResp.getTopic());
        assertEquals(2, commitOffsetResp.getVgId());
        assertEquals(200L, commitOffsetResp.getOffset());
    }

    @Test
    public void testEqualsAndHashCodeConsistency() {
        CommitOffsetResp resp1 = new CommitOffsetResp();
        resp1.setTiming(1000L);
        resp1.setTopic("topic");
        resp1.setVgId(1);
        resp1.setOffset(100L);

        CommitOffsetResp resp2 = new CommitOffsetResp();
        resp2.setTiming(1000L);
        resp2.setTopic("topic");
        resp2.setVgId(1);
        resp2.setOffset(100L);

        // Test field equality
        assertEquals(resp1.getTiming(), resp2.getTiming());
        assertEquals(resp1.getTopic(), resp2.getTopic());
        assertEquals(resp1.getVgId(), resp2.getVgId());
        assertEquals(resp1.getOffset(), resp2.getOffset());
    }

    @Test
    public void testJsonPropertyNameMatching() throws Exception {
        // Verify that JSON property names match the @JsonProperty annotations
        ObjectMapper mapper = new ObjectMapper();

        CommitOffsetResp resp = new CommitOffsetResp();
        resp.setTiming(12345L);
        resp.setTopic("test");
        resp.setVgId(3);
        resp.setOffset(999L);

        String json = mapper.writeValueAsString(resp);

        // Check that the JSON uses the annotated property names
        assertTrue(json.contains("\"vg_id\":3"));
        assertTrue(json.contains("\"timing\":12345"));
        assertTrue(json.contains("\"topic\":\"test\""));
        assertTrue(json.contains("\"offset\":999"));
    }

    @Test
    public void testDeserializationWithMissingFields() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        // Test with missing optional fields (should use defaults for missing fields)
        String jsonWithMissingFields = "{\"topic\":\"partial-topic\"}";
        CommitOffsetResp deserialized = mapper.readValue(jsonWithMissingFields, CommitOffsetResp.class);

        assertEquals("partial-topic", deserialized.getTopic());
        assertEquals(0L, deserialized.getTiming());
        assertEquals(0, deserialized.getVgId());
        assertEquals(0L, deserialized.getOffset());
    }

    @Test
    public void testBoundaryValues() {
        // Test with boundary values for each field
        commitOffsetResp.setTiming(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, commitOffsetResp.getTiming());

        commitOffsetResp.setTiming(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, commitOffsetResp.getTiming());

        commitOffsetResp.setVgId(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, commitOffsetResp.getVgId());

        commitOffsetResp.setVgId(Integer.MIN_VALUE);
        assertEquals(Integer.MIN_VALUE, commitOffsetResp.getVgId());

        commitOffsetResp.setOffset(Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, commitOffsetResp.getOffset());

        commitOffsetResp.setOffset(Long.MIN_VALUE);
        assertEquals(Long.MIN_VALUE, commitOffsetResp.getOffset());
    }
}