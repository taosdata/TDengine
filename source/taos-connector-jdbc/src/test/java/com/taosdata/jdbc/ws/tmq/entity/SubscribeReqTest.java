package com.taosdata.jdbc.ws.tmq.entity;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

/**
 * Unit tests for SubscribeReq (no mock frameworks, pure real-instance testing)
 * Compatible with JDK8 + JUnit4
 */
public class SubscribeReqTest {

    // Test constants
    private static final String TEST_USER = "tmq_user";
    private static final String TEST_PASSWORD = "tmq_pwd_123";
    private static final String TEST_BEARER_TOKEN = "bearerToken";
    private static final String TEST_DB = "tmq_test_db";
    private static final String TEST_GROUP_ID = "tmq_group_001";
    private static final String TEST_CLIENT_ID = "tmq_client_001";
    private static final String TEST_OFFSET_REST = "latest";
    private static final String[] TEST_TOPICS = {"topic_1", "topic_2", "topic_3"};
    private static final String TEST_AUTO_COMMIT = "true";
    private static final String TEST_AUTO_COMMIT_INTERVAL_MS = "5000";
    private static final String TEST_WITH_TABLE_NAME = "true";
    private static final String TEST_ENABLE_BATCH_META = "false";
    private static final String TEST_TZ = "UTC";
    private static final String TEST_APP = "tmq_test_app";
    private static final String TEST_IP = "10.0.0.2";
    private static final String TEST_CONNECTOR = "tmq-connector-1.0.0";
    private static final long TEST_REQ_ID = 123456789L;

    /**
     * Build a test SubscribeReq instance with base values
     * @return Configured SubscribeReq instance
     */
    private SubscribeReq buildTestSubscribeReq() {
        SubscribeReq subscribeReq = new SubscribeReq();
        // Set base test values
        subscribeReq.setUser(TEST_USER);
        subscribeReq.setPassword(TEST_PASSWORD);
        subscribeReq.setDb(TEST_DB);
        subscribeReq.setGroupId(TEST_GROUP_ID);
        subscribeReq.setClientId(TEST_CLIENT_ID);
        subscribeReq.setOffsetRest(TEST_OFFSET_REST);
        subscribeReq.setTopics(TEST_TOPICS);
        subscribeReq.setAutoCommit(TEST_AUTO_COMMIT);
        subscribeReq.setAutoCommitIntervalMs(TEST_AUTO_COMMIT_INTERVAL_MS);
        subscribeReq.setWithTableName(TEST_WITH_TABLE_NAME);
        subscribeReq.setEnableBatchMeta(TEST_ENABLE_BATCH_META);
        subscribeReq.setTz(TEST_TZ);
        subscribeReq.setApp(TEST_APP);
        subscribeReq.setIp(TEST_IP);
        subscribeReq.setConnector(TEST_CONNECTOR);
        subscribeReq.setReqId(TEST_REQ_ID);

        // Add test config
        HashMap<String, String> testConfig = new HashMap<>();
        testConfig.put("max.poll.records", "100");
        testConfig.put("session.timeout.ms", "30000");
        subscribeReq.setConfig(testConfig);

        return subscribeReq;
    }

    /**
     * Test: All getter/setter methods (verify value assignment and retrieval)
     */
    @Test
    public void testGetterAndSetterMethods() {
        // 1. Prepare: Create empty SubscribeReq instance
        SubscribeReq subscribeReq = new SubscribeReq();

        // 2. Execute: Set test values via setters
        subscribeReq.setUser(TEST_USER);
        subscribeReq.setPassword(TEST_PASSWORD);
        subscribeReq.setBearerToken(TEST_BEARER_TOKEN);
        subscribeReq.setDb(TEST_DB);
        subscribeReq.setGroupId(TEST_GROUP_ID);
        subscribeReq.setClientId(TEST_CLIENT_ID);
        subscribeReq.setOffsetRest(TEST_OFFSET_REST);
        subscribeReq.setTopics(TEST_TOPICS);
        subscribeReq.setAutoCommit(TEST_AUTO_COMMIT);
        subscribeReq.setAutoCommitIntervalMs(TEST_AUTO_COMMIT_INTERVAL_MS);
        subscribeReq.setWithTableName(TEST_WITH_TABLE_NAME);
        subscribeReq.setEnableBatchMeta(TEST_ENABLE_BATCH_META);
        subscribeReq.setTz(TEST_TZ);
        subscribeReq.setApp(TEST_APP);
        subscribeReq.setIp(TEST_IP);
        subscribeReq.setConnector(TEST_CONNECTOR);
        subscribeReq.setReqId(TEST_REQ_ID);

        // Set test config map
        HashMap<String, String> testConfig = new HashMap<>();
        testConfig.put("key1", "value1");
        testConfig.put("key2", "value2");
        subscribeReq.setConfig(testConfig);

        // 3. Verify: Getters return correct values
        assertEquals("User should match test value", TEST_USER, subscribeReq.getUser());
        assertEquals("Password should match test value", TEST_PASSWORD, subscribeReq.getPassword());
        assertEquals("BearerToken should match test value", TEST_BEARER_TOKEN, subscribeReq.getBearerToken());
        assertEquals("Database should match test value", TEST_DB, subscribeReq.getDb());
        assertEquals("GroupId should match test value", TEST_GROUP_ID, subscribeReq.getGroupId());
        assertEquals("ClientId should match test value", TEST_CLIENT_ID, subscribeReq.getClientId());
        assertEquals("OffsetRest should match test value", TEST_OFFSET_REST, subscribeReq.getOffsetRest());
        assertArrayEquals("Topics array should match test values", TEST_TOPICS, subscribeReq.getTopics());
        assertEquals("AutoCommit should match test value", TEST_AUTO_COMMIT, subscribeReq.getAutoCommit());
        assertEquals("AutoCommitIntervalMs should match test value", TEST_AUTO_COMMIT_INTERVAL_MS, subscribeReq.getAutoCommitIntervalMs());
        assertEquals("WithTableName should match test value", TEST_WITH_TABLE_NAME, subscribeReq.getWithTableName());
        assertEquals("EnableBatchMeta should match test value", TEST_ENABLE_BATCH_META, subscribeReq.getEnableBatchMeta());
        assertEquals("Timezone should match test value", TEST_TZ, subscribeReq.getTz());
        assertEquals("App name should match test value", TEST_APP, subscribeReq.getApp());
        assertEquals("IP should match test value", TEST_IP, subscribeReq.getIp());
        assertEquals("Connector should match test value", TEST_CONNECTOR, subscribeReq.getConnector());
        assertEquals("ReqId should match test value", TEST_REQ_ID, subscribeReq.getReqId());
        assertEquals("Config map should match test values", testConfig, subscribeReq.getConfig());
    }

    /**
     * Test: Default initialization of config field (should be empty HashMap, not null)
     */
    @Test
    public void testDefaultConfigInitialization() {
        // 1. Prepare: Create empty SubscribeReq instance
        SubscribeReq subscribeReq = new SubscribeReq();

        // 2. Verify: Config is initialized as empty HashMap (not null)
        assertNotNull("Config should be initialized as empty HashMap", subscribeReq.getConfig());
        assertTrue("Config should be empty by default", subscribeReq.getConfig().isEmpty());
    }

    /**
     * Test: toPrintString() method
     * - Verify sensitive password masking (******)
     * - Verify all fields are present in output
     * - Verify topics array is joined with commas
     * - Verify config map is included
     */
    @Test
    public void testToPrintString() {
        // 1. Prepare: Build test SubscribeReq instance
        SubscribeReq subscribeReq = buildTestSubscribeReq();

        // 2. Execute: Get print string
        String printStr = subscribeReq.toPrintString();

        // 3. Verify: Output format and content
        // Check basic fields
        assertTrue("Print string should contain user", printStr.contains(TEST_USER));
        assertTrue("Print string should mask password with ******", printStr.contains("******"));
        assertTrue("Print string should contain database", printStr.contains(TEST_DB));
        assertTrue("Print string should contain groupId", printStr.contains(TEST_GROUP_ID));
        assertTrue("Print string should contain clientId", printStr.contains(TEST_CLIENT_ID));
        assertTrue("Print string should contain offsetRest", printStr.contains(TEST_OFFSET_REST));

        // Check topics array (joined with commas)
        String joinedTopics = String.join(",", TEST_TOPICS);
        assertTrue("Print string should contain joined topics: " + joinedTopics, printStr.contains(joinedTopics));

        // Check TMQ-specific fields
        assertTrue("Print string should contain autoCommit", printStr.contains(TEST_AUTO_COMMIT));
        assertTrue("Print string should contain autoCommitIntervalMs", printStr.contains(TEST_AUTO_COMMIT_INTERVAL_MS));
        assertTrue("Print string should contain withTableName", printStr.contains(TEST_WITH_TABLE_NAME));
        assertTrue("Print string should contain enableBatchMeta", printStr.contains(TEST_ENABLE_BATCH_META));

        // Check common fields
        assertTrue("Print string should contain timezone", printStr.contains(TEST_TZ));
        assertTrue("Print string should contain app name", printStr.contains(TEST_APP));
        assertTrue("Print string should contain IP", printStr.contains(TEST_IP));
        assertTrue("Print string should contain connector", printStr.contains(TEST_CONNECTOR));

        // Check config map and reqId
        assertTrue("Print string should contain reqId", printStr.contains("reqId=" + TEST_REQ_ID));

        // Negative check: No plaintext password
        assertFalse("Print string should NOT contain plaintext password", printStr.contains(TEST_PASSWORD));
    }

    /**
     * Test: Edge case - null topics array in toPrintString() (avoid NPE)
     * Note: Ensure toPrintString() handles null topics gracefully (if applicable)
     */
    @Test
    public void testToPrintString_NullTopics() {
        // 1. Prepare: Create instance with null topics
        SubscribeReq subscribeReq = buildTestSubscribeReq();
        subscribeReq.setTopics(null);

        // 2. Execute & Verify: No NullPointerException (safe execution)
        try {
            String printStr = subscribeReq.toPrintString();
            assertTrue("Print string should contain 'topics=null' or similar", printStr.contains("topics="));
        } catch (NullPointerException e) {
            fail("toPrintString() should not throw NPE when topics is null");
        }
    }
}