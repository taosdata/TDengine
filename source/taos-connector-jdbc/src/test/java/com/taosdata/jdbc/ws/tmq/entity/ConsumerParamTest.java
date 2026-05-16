package com.taosdata.jdbc.ws.tmq.entity;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.tmq.TMQConstants;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.*;

public class ConsumerParamTest {
    private Properties validProperties;

    @Before
    public void setUp() {
        validProperties = new Properties();
        validProperties.setProperty(TMQConstants.CONNECT_USER, "testUser");
        validProperties.setProperty(TMQConstants.CONNECT_PASS, "testPass");
        validProperties.setProperty(TMQConstants.CONNECT_IP, "localhost");
        validProperties.setProperty(TMQConstants.CONNECT_PORT, "6041");
        validProperties.setProperty(TMQConstants.GROUP_ID, "testGroup");
        validProperties.setProperty(TMQConstants.CLIENT_ID, "testClient");
        validProperties.setProperty(TMQConstants.AUTO_OFFSET_RESET, "earliest");
        validProperties.setProperty(TMQConstants.AUTO_COMMIT_INTERVAL, "10000");
        validProperties.setProperty(TMQConstants.MSG_WITH_TABLE_NAME, "true");
        validProperties.setProperty(TMQConstants.MSG_ENABLE_BATCH_META, "false");
        validProperties.setProperty("unknown.config.key", "unknownValue");
    }

    @Test
    public void testConstructorWithValidProperties() throws SQLException {
        ConsumerParam param = new ConsumerParam(validProperties);

        assertEquals("testUser", param.getConnectionParam().getUser());
        assertEquals("testPass", param.getConnectionParam().getPassword());
        assertEquals("testGroup", param.getGroupId());
        assertEquals("testClient", param.getClientId());
        assertEquals("earliest", param.getOffsetRest());
        assertTrue(param.isAutoCommit());
        assertEquals(10000L, param.getAutoCommitInterval());
        assertEquals("true", param.getMsgWithTableName());
        assertEquals("false", param.getEnableBatchMeta());
        assertEquals("unknownValue", param.getConfig().get("unknown.config.key"));
    }

    @Test
    public void testConstructorWithConnectUrl() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_URL,
                "jdbc:TAOS-RS://localhost:6041/test?user=urlUser&password=urlPass");
        props.setProperty(TMQConstants.GROUP_ID, "testGroup");

        ConsumerParam param = new ConsumerParam(props);

        assertNotNull(param.getConnectionParam());
        assertEquals("testGroup", param.getGroupId());
    }

    @Test
    public void testConstructorWithDefaultAutoCommit() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");

        ConsumerParam param = new ConsumerParam(props);

        assertTrue(param.isAutoCommit());
    }

    @Test
    public void testConstructorWithAutoCommitDisabled() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");
        props.setProperty(TMQConstants.ENABLE_AUTO_COMMIT, "false");

        ConsumerParam param = new ConsumerParam(props);

        assertFalse(param.isAutoCommit());
    }

    @Test
    public void testConstructorWithDefaultAutoCommitInterval() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");

        ConsumerParam param = new ConsumerParam(props);

        assertEquals(5000L, param.getAutoCommitInterval());
    }

    @Test(expected = SQLException.class)
    public void testConstructorWithInvalidAutoCommitInterval() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");
        props.setProperty(TMQConstants.AUTO_COMMIT_INTERVAL, "-1000");

        new ConsumerParam(props);
    }

    @Test(expected = SQLException.class)
    public void testConstructorWithSlaveClusterHost() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");
        props.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "slave.host");

        new ConsumerParam(props);
    }

    @Test
    public void testPropertyMappingFromTMQToTSDB() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "tmqUser");
        props.setProperty(TMQConstants.CONNECT_PASS, "tmqPass");
        props.setProperty(TMQConstants.CONNECT_IP, "tmqHost");
        props.setProperty(TMQConstants.CONNECT_PORT, "1234");
        props.setProperty(TMQConstants.GROUP_ID, "testGroup");

        ConsumerParam param = new ConsumerParam(props);

        ConnectionParam connParam = param.getConnectionParam();
        assertEquals("tmqUser", connParam.getUser());
        assertEquals("tmqPass", connParam.getPassword());
        assertEquals("tmqHost", connParam.getEndpoints().get(0).getHost());
        assertEquals(1234, connParam.getEndpoints().get(0).getPort());
    }

    @Test
    public void testKnownKeysAreNotAddedToConfig() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");
        props.setProperty(TSDBDriver.PROPERTY_KEY_HOST, "host1");
        props.setProperty("custom.key", "customValue");

        ConsumerParam param = new ConsumerParam(props);

        assertFalse(param.getConfig().containsKey(TMQConstants.GROUP_ID));
        assertFalse(param.getConfig().containsKey(TSDBDriver.PROPERTY_KEY_HOST));
        assertEquals("customValue", param.getConfig().get("custom.key"));
    }

    @Test
    public void testSettersAndGetters() throws SQLException {
        ConsumerParam param = new ConsumerParam(validProperties);

        param.setGroupId("newGroup");
        assertEquals("newGroup", param.getGroupId());

        param.setClientId("newClient");
        assertEquals("newClient", param.getClientId());

        param.setOffsetRest("latest");
        assertEquals("latest", param.getOffsetRest());

        param.setAutoCommitInterval(20000L);
        assertEquals(20000L, param.getAutoCommitInterval());

        param.setMsgWithTableName("false");
        assertEquals("false", param.getMsgWithTableName());

        param.setEnableBatchMeta("true");
        assertEquals("true", param.getEnableBatchMeta());

        ConnectionParam newConnParam = new ConnectionParam.Builder(null).build();
        param.setConnectionParam(newConnParam);
        assertEquals(newConnParam, param.getConnectionParam());
    }

    @Test
    public void testConfigMapIsMutable() throws SQLException {
        ConsumerParam param = new ConsumerParam(validProperties);

        param.getConfig().put("additional.key", "additionalValue");
        assertEquals("additionalValue", param.getConfig().get("additional.key"));
    }

    @Test
    public void testConstructorWithMinimalProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "minUser");
        props.setProperty(TMQConstants.CONNECT_PASS, "minPass");
        props.setProperty(TMQConstants.GROUP_ID, "minGroup");

        ConsumerParam param = new ConsumerParam(props);

        assertNotNull(param.getConnectionParam());
        assertEquals("minGroup", param.getGroupId());
        assertNull(param.getClientId());
        assertNull(param.getOffsetRest());
        assertTrue(param.isAutoCommit());
        assertEquals(5000L, param.getAutoCommitInterval());
        assertNull(param.getMsgWithTableName());
        assertNull(param.getEnableBatchMeta());
        assertTrue(param.getConfig().isEmpty());
    }

    @Test
    public void testEnableBatchMetaDefaultNull() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");

        ConsumerParam param = new ConsumerParam(props);

        assertNull(param.getEnableBatchMeta());
    }

    @Test
    public void testTMQConnectToken() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");
        props.setProperty(TMQConstants.TMQ_CONNECT_TOKEN, "test-token-abc123");

        ConsumerParam param = new ConsumerParam(props);

        assertEquals("test-token-abc123", param.getConfig().get(TMQConstants.TMQ_CONNECT_TOKEN));
    }

    @Test
    public void testTMQConnectTokenEmpty() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");
        props.setProperty(TMQConstants.TMQ_CONNECT_TOKEN, "");

        ConsumerParam param = new ConsumerParam(props);

        assertNull(param.getConfig().get(TMQConstants.TMQ_CONNECT_TOKEN));
    }

    @Test
    public void testTMQConnectTokenNull() throws SQLException {
        Properties props = new Properties();
        props.setProperty(TMQConstants.CONNECT_USER, "user");
        props.setProperty(TMQConstants.CONNECT_PASS, "pass");
        props.setProperty(TMQConstants.GROUP_ID, "group1");

        ConsumerParam param = new ConsumerParam(props);

        assertNull(param.getConfig().get(TMQConstants.TMQ_CONNECT_TOKEN));
    }
}