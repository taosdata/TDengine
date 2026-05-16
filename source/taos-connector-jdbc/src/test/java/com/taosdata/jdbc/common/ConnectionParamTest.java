package com.taosdata.jdbc.common;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.TestEnvUtil;
import io.netty.buffer.ByteBuf;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class ConnectionParamTest {
    private ConnectionParam connectionParam;

    @Before
    public void setUp() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        connectionParam = ConnectionParam.getParam(properties);
    }

    @Test
    public void testSettersAndGetters() {
        // Original parameter tests
        connectionParam.setEndpoints(null);
        assertNull(connectionParam.getEndpoints());

        connectionParam.setDatabase("testDB");
        assertEquals("testDB", connectionParam.getDatabase());

        connectionParam.setCloudToken("testToken");
        assertEquals("testToken", connectionParam.getCloudToken());

        connectionParam.setUser("testUser");
        assertEquals("testUser", connectionParam.getUser());

        connectionParam.setPassword("testPassword");
        assertEquals("testPassword", connectionParam.getPassword());

        connectionParam.setBearerToken("bearerToken");
        assertEquals("bearerToken", connectionParam.getBearerToken());

        connectionParam.setTz("UTC");
        assertEquals("UTC", connectionParam.getTz());

        connectionParam.setUseSsl(true);
        assertTrue(connectionParam.isUseSsl());

        connectionParam.setMaxRequest(100);
        assertEquals(100, connectionParam.getMaxRequest());

        connectionParam.setConnectTimeout(5000);
        assertEquals(5000, connectionParam.getConnectTimeout());

        connectionParam.setRequestTimeout(3000);
        assertEquals(3000, connectionParam.getRequestTimeout());

        connectionParam.setConnectMode(1);
        assertEquals(1, connectionParam.getConnectMode());

        connectionParam.setEnableCompression(true);
        assertTrue(connectionParam.isEnableCompression());

        connectionParam.setSlaveClusterHost("slaveHost");
        assertEquals("slaveHost", connectionParam.getSlaveClusterHost());

        connectionParam.setSlaveClusterPort(8081);
        assertEquals(8081, connectionParam.getSlaveClusterPort());

        connectionParam.setReconnectIntervalMs(2000);
        assertEquals(2000, connectionParam.getReconnectIntervalMs());

        connectionParam.setReconnectRetryCount(5);
        assertEquals(5, connectionParam.getReconnectRetryCount());

        connectionParam.setEnableAutoConnect(true);
        assertTrue(connectionParam.isEnableAutoConnect());

        connectionParam.setDisableSslCertValidation(true);
        assertTrue(connectionParam.isDisableSslCertValidation());

        connectionParam.setAppName("testAppName");
        assertEquals("testAppName", connectionParam.getAppName());

        connectionParam.setAppIp("testAppIp");
        assertEquals("testAppIp", connectionParam.getAppIp());

        connectionParam.setCopyData(true);
        assertTrue(connectionParam.isCopyData());

        connectionParam.setBatchSizeByRow(100);
        assertEquals(100, connectionParam.getBatchSizeByRow());

        connectionParam.setCacheSizeByRow(1000);
        assertEquals(1000, connectionParam.getCacheSizeByRow());

        connectionParam.setBackendWriteThreadNum(4);
        assertEquals(4, connectionParam.getBackendWriteThreadNum());

        connectionParam.setStrictCheck(true);
        assertTrue(connectionParam.isStrictCheck());

        connectionParam.setRetryTimes(3);
        assertEquals(3, connectionParam.getRetryTimes());

        connectionParam.setAsyncWrite("stmt");
        assertEquals("stmt", connectionParam.getAsyncWrite());

        connectionParam.setPbsMode("line");
        assertEquals("line", connectionParam.getPbsMode());

        connectionParam.setWsKeepAlive(300);
        assertEquals(300, connectionParam.getWsKeepAlive());

        // Health check parameter Setter/Getter tests
        connectionParam.setHealthCheckInitInterval(10);
        assertEquals(10, connectionParam.getHealthCheckInitInterval());

        connectionParam.setHealthCheckMaxInterval(300);
        assertEquals(300, connectionParam.getHealthCheckMaxInterval());

        connectionParam.setHealthCheckConTimeout(1);
        assertEquals(1, connectionParam.getHealthCheckConTimeout());

        connectionParam.setHealthCheckCmdTimeout(5);
        assertEquals(5, connectionParam.getHealthCheckCmdTimeout());

        connectionParam.setHealthCheckRecoveryCount(3);
        assertEquals(3, connectionParam.getHealthCheckRecoveryCount());

        connectionParam.setHealthCheckRecoveryInterval(60);
        assertEquals(60, connectionParam.getHealthCheckRecoveryInterval());

        // Rebalance parameter Setter/Getter tests
        connectionParam.setRebalanceThreshold(20);
        assertEquals(20, connectionParam.getRebalanceThreshold());

        connectionParam.setRebalanceConBaseCount(30);
        assertEquals(30, connectionParam.getRebalanceConBaseCount());
    }

    // Health check parameter invalid value tests
    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckInitIntervalZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckInitIntervalNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckMaxIntervalLessThanInit() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "10");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "5");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckMaxIntervalZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckMaxIntervalNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "-10");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckConTimeoutZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CON_TIMEOUT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckConTimeoutNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CON_TIMEOUT, "-2");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckCmdTimeoutZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CMD_TIMEOUT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckCmdTimeoutNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_CMD_TIMEOUT, "-3");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckRecoveryCountZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckRecoveryCountNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "-4");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidHealthCheckRecoveryIntervalNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_INTERVAL, "-10");
        ConnectionParam.getParam(properties);
    }

    // Rebalance parameter invalid value tests
    @Test(expected = SQLException.class)
    public void testInvalidRebalanceThresholdLessThan10() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "9");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceThresholdGreaterThan50() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "55");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceThresholdNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "-20");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceConBaseCountZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidRebalanceConBaseCountNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, "-15");
        ConnectionParam.getParam(properties);
    }

    // Test invalid connectMode values (negative or greater than 1)
    @Test(expected = SQLException.class)
    public void testInvalidConnectModeNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONNECT_MODE, "-1");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidConnectModeGreaterThan1() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONNECT_MODE, "2");
        ConnectionParam.getParam(properties);
    }

    // Test invalid slaveClusterPort values (zero or negative)
    @Test(expected = SQLException.class)
    public void testInvalidSlaveClusterPortZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidSlaveClusterPortNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, "-6041");
        ConnectionParam.getParam(properties);
    }

    // Test conflict: slaveClusterHost with multiple endpoints
    @Test(expected = SQLException.class)
    public void testSlaveClusterHostWithMultipleEndpoints() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS, "host1:6041,host2:6041");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, "slaveHost");
        ConnectionParam.getParam(properties);
    }

    // Test invalid reconnectIntervalMs (negative)
    @Test(expected = SQLException.class)
    public void testInvalidReconnectIntervalMsNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "-1000");
        ConnectionParam.getParam(properties);
    }

    // Test invalid reconnectRetryCount (negative)
    @Test(expected = SQLException.class)
    public void testInvalidReconnectRetryCountNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "-5");
        ConnectionParam.getParam(properties);
    }

    // Test appName exceeding max length (23 bytes)
    @Test(expected = SQLException.class)
    public void testInvalidAppNameTooLong() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_NAME, "123456789012345678901234"); // 24 chars
        ConnectionParam.getParam(properties);
    }

    // Test invalid appIp format
    @Test(expected = SQLException.class)
    public void testInvalidAppIpFormat() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_APP_IP, "256.0.0.1"); // Invalid IP
        ConnectionParam.getParam(properties);
    }

    // Test invalid batchSizeByRow (negative)
    @Test(expected = SQLException.class)
    public void testInvalidBatchSizeByRowNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "-100");
        ConnectionParam.getParam(properties);
    }

    // Test invalid cacheSizeByRow (negative)
    @Test(expected = SQLException.class)
    public void testInvalidCacheSizeByRowNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, "-1000");
        ConnectionParam.getParam(properties);
    }

    // Test batchSizeByRow greater than cacheSizeByRow
    @Test(expected = SQLException.class)
    public void testBatchSizeGreaterThanCacheSize() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, "1000");
        ConnectionParam.getParam(properties);
    }

    // Test cacheSizeByRow not multiple of batchSizeByRow
    @Test(expected = SQLException.class)
    public void testCacheSizeNotMultipleOfBatchSize() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_SIZE_BY_ROW, "300");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CACHE_SIZE_BY_ROW, "1000"); // 1000 % 300 != 0
        ConnectionParam.getParam(properties);
    }

    // Test invalid backendWriteThreadNum (negative)
    @Test(expected = SQLException.class)
    public void testInvalidBackendWriteThreadNumNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BACKEND_WRITE_THREAD_NUM, "-5");
        ConnectionParam.getParam(properties);
    }

    // Test invalid retryTimes (negative)
    @Test(expected = SQLException.class)
    public void testInvalidRetryTimesNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RETRY_TIMES, "-3");
        ConnectionParam.getParam(properties);
    }

    // Test invalid asyncWrite value (not empty or "STMT")
    @Test(expected = SQLException.class)
    public void testInvalidAsyncWriteValue() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ASYNC_WRITE, "INVALID");
        ConnectionParam.getParam(properties);
    }

    // Test invalid pbsMode value (not empty or "line")
    @Test(expected = SQLException.class)
    public void testInvalidPbsModeValue() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PBS_MODE, "INVALID");
        ConnectionParam.getParam(properties);
    }

    // Test invalid wsKeepAlive (zero or negative)
    @Test(expected = SQLException.class)
    public void testInvalidWsKeepAliveZero() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_WS_KEEP_ALIVE_SECONDS, "0");
        ConnectionParam.getParam(properties);
    }

    @Test(expected = SQLException.class)
    public void testInvalidWsKeepAliveNegative() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_WS_KEEP_ALIVE_SECONDS, "-300");
        ConnectionParam.getParam(properties);
    }

    // Test both user and cloudToken are null
    @Test(expected = SQLException.class)
    public void testUserAndCloudTokenBothNull() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        ConnectionParam.getParam(properties);
    }

    // Test both password and cloudToken are null
    @Test(expected = SQLException.class)
    public void testPasswordAndCloudTokenBothNull() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        ConnectionParam.getParam(properties);
    }

    // Test getParamWs: time zone contains '+'
    @Test
    public void testParamWsTimeZoneWithPlus() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC+8");
        ConnectionParam param = ConnectionParam.getParamWs(properties);
        assertEquals("", param.getTz());
    }

    // Test getParamWs: time zone contains '-'
    @Test
    public void testParamWsTimeZoneWithMinus() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        ConnectionParam param = ConnectionParam.getParamWs(properties);
        assertEquals("", param.getTz());
    }

    // Test getParamWs: time zone without '/'
    @Test
    public void testParamWsTimeZoneWithoutSlash() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC");
        ConnectionParam param = ConnectionParam.getParamWs(properties);
        assertEquals("", param.getTz());
    }

    // Test getParamWs: invalid time zone
    @Test(expected = SQLException.class)
    public void testParamWsInvalidTimeZone() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "Invalid/TimeZone");
        ConnectionParam.getParamWs(properties);
    }

    // Test varcharAsString getter
    @Test
    public void testVarcharAsStringGetter() {
        assertFalse(connectionParam.isVarcharAsString());

        List<Endpoint> endpoints = new ArrayList<>();
        ConnectionParam param = new ConnectionParam.Builder(endpoints)
                .setVarcharAsString(true)
                .build();
        assertTrue(param.isVarcharAsString());
    }

    // Test textMessageHandler and binaryMessageHandler setter/getter
    @Test
    public void testMessageHandlers() {
        java.util.function.Consumer<String> textHandler = s -> {};
        connectionParam.setTextMessageHandler(textHandler);
        assertSame(textHandler, connectionParam.getTextMessageHandler());

        java.util.function.Consumer<ByteBuf> binaryHandler = buf -> {};
        connectionParam.setBinaryMessageHandler(binaryHandler);
        assertSame(binaryHandler, connectionParam.getBinaryMessageHandler());
    }

    // Test copyToBuilder method (verify all fields are copied correctly)
    @Test
    public void testCopyToBuilder() {
        // Set test values to original param
        connectionParam.setDatabase("testDB");
        connectionParam.setCloudToken("testToken");
        connectionParam.setUser("testUser");
        connectionParam.setPassword("testPassword");
        connectionParam.setTz("UTC");
        connectionParam.setUseSsl(true);
        connectionParam.setMaxRequest(100);
        connectionParam.setConnectTimeout(5000);
        connectionParam.setRequestTimeout(3000);
        connectionParam.setConnectMode(1);
        connectionParam.setEnableCompression(true);
        connectionParam.setSlaveClusterHost("slaveHost");
        connectionParam.setSlaveClusterPort(8081);
        connectionParam.setReconnectIntervalMs(2000);
        connectionParam.setReconnectRetryCount(5);
        connectionParam.setEnableAutoConnect(true);
        connectionParam.setDisableSslCertValidation(true);
        connectionParam.setAppName("testAppName");
        connectionParam.setAppIp("127.0.0.1");
        connectionParam.setCopyData(true);
        connectionParam.setBatchSizeByRow(100);
        connectionParam.setCacheSizeByRow(1000);
        connectionParam.setBackendWriteThreadNum(4);
        connectionParam.setStrictCheck(true);
        connectionParam.setRetryTimes(3);
        connectionParam.setAsyncWrite("stmt");
        connectionParam.setPbsMode("line");
        connectionParam.setWsKeepAlive(300);
        connectionParam.setHealthCheckInitInterval(10);
        connectionParam.setHealthCheckMaxInterval(300);
        connectionParam.setHealthCheckConTimeout(1);
        connectionParam.setHealthCheckCmdTimeout(5);
        connectionParam.setHealthCheckRecoveryCount(3);
        connectionParam.setHealthCheckRecoveryInterval(60);
        connectionParam.setRebalanceThreshold(20);
        connectionParam.setRebalanceConBaseCount(30);

        // Add message handlers
        java.util.function.Consumer<String> textHandler = s -> {};
        java.util.function.Consumer<ByteBuf> binaryHandler = buf -> {};
        connectionParam.setTextMessageHandler(textHandler);
        connectionParam.setBinaryMessageHandler(binaryHandler);

        // Copy via builder
        ConnectionParam.Builder builder = ConnectionParam.copyToBuilder(connectionParam);
        ConnectionParam copiedParam = builder.build();

        // Verify copied fields
        assertEquals(connectionParam.getDatabase(), copiedParam.getDatabase());
        assertEquals(connectionParam.getCloudToken(), copiedParam.getCloudToken());
        assertEquals(connectionParam.getUser(), copiedParam.getUser());
        assertEquals(connectionParam.getPassword(), copiedParam.getPassword());
        assertEquals(connectionParam.getTz(), copiedParam.getTz());
        assertEquals(connectionParam.isUseSsl(), copiedParam.isUseSsl());
        assertEquals(connectionParam.getMaxRequest(), copiedParam.getMaxRequest());
        assertEquals(connectionParam.getConnectTimeout(), copiedParam.getConnectTimeout());
        assertEquals(connectionParam.getRequestTimeout(), copiedParam.getRequestTimeout());
        assertEquals(connectionParam.getConnectMode(), copiedParam.getConnectMode());
        assertEquals(connectionParam.isVarcharAsString(), copiedParam.isVarcharAsString());
        assertEquals(connectionParam.isEnableCompression(), copiedParam.isEnableCompression());
        assertEquals(connectionParam.getSlaveClusterHost(), copiedParam.getSlaveClusterHost());
        assertEquals(connectionParam.getSlaveClusterPort(), copiedParam.getSlaveClusterPort());
        assertEquals(connectionParam.getReconnectIntervalMs(), copiedParam.getReconnectIntervalMs());
        assertEquals(connectionParam.getReconnectRetryCount(), copiedParam.getReconnectRetryCount());
        assertEquals(connectionParam.isEnableAutoConnect(), copiedParam.isEnableAutoConnect());
        assertEquals(connectionParam.isDisableSslCertValidation(), copiedParam.isDisableSslCertValidation());
        assertEquals(connectionParam.getAppName(), copiedParam.getAppName());
        assertEquals(connectionParam.getAppIp(), copiedParam.getAppIp());
        assertEquals(connectionParam.isCopyData(), copiedParam.isCopyData());
        assertEquals(connectionParam.getBatchSizeByRow(), copiedParam.getBatchSizeByRow());
        assertEquals(connectionParam.getCacheSizeByRow(), copiedParam.getCacheSizeByRow());
        assertEquals(connectionParam.getBackendWriteThreadNum(), copiedParam.getBackendWriteThreadNum());
        assertEquals(connectionParam.isStrictCheck(), copiedParam.isStrictCheck());
        assertEquals(connectionParam.getRetryTimes(), copiedParam.getRetryTimes());
        assertEquals(connectionParam.getAsyncWrite(), copiedParam.getAsyncWrite());
        assertEquals(connectionParam.getPbsMode(), copiedParam.getPbsMode());
        assertEquals(connectionParam.getWsKeepAlive(), copiedParam.getWsKeepAlive());
        assertEquals(connectionParam.getHealthCheckInitInterval(), copiedParam.getHealthCheckInitInterval());
        assertEquals(connectionParam.getHealthCheckMaxInterval(), copiedParam.getHealthCheckMaxInterval());
        assertEquals(connectionParam.getHealthCheckConTimeout(), copiedParam.getHealthCheckConTimeout());
        assertEquals(connectionParam.getHealthCheckCmdTimeout(), copiedParam.getHealthCheckCmdTimeout());
        assertEquals(connectionParam.getHealthCheckRecoveryCount(), copiedParam.getHealthCheckRecoveryCount());
        assertEquals(connectionParam.getHealthCheckRecoveryInterval(), copiedParam.getHealthCheckRecoveryInterval());
        assertEquals(connectionParam.getRebalanceThreshold(), copiedParam.getRebalanceThreshold());
        assertEquals(connectionParam.getRebalanceConBaseCount(), copiedParam.getRebalanceConBaseCount());
        assertSame(connectionParam.getTextMessageHandler(), copiedParam.getTextMessageHandler());
        assertSame(connectionParam.getBinaryMessageHandler(), copiedParam.getBinaryMessageHandler());
    }

    // Test valid rebalanceThreshold (boundary values: 10, 50)
    @Test
    public void testValidRebalanceThresholdBoundary() throws SQLException {
        Properties properties1 = new Properties();
        properties1.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties1.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties1.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "10");
        ConnectionParam param1 = ConnectionParam.getParam(properties1);
        assertEquals(10, param1.getRebalanceThreshold());

        Properties properties2 = new Properties();
        properties2.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties2.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties2.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "50");
        ConnectionParam param2 = ConnectionParam.getParam(properties2);
        assertEquals(50, param2.getRebalanceThreshold());
    }

    // Test zoneId setter/getter
    @Test
    public void testZoneIdSetterGetter() {
        ZoneId zoneId = ZoneId.of("Asia/Shanghai");
        connectionParam.setZoneId(zoneId);
        assertEquals(zoneId, connectionParam.getZoneId());
    }

    // Test valid healthCheck parameters (equal init and max interval)
    @Test
    public void testHealthCheckInitEqualsMaxInterval() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "30");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "30");
        ConnectionParam param = ConnectionParam.getParam(properties);
        assertEquals(30, param.getHealthCheckInitInterval());
        assertEquals(30, param.getHealthCheckMaxInterval());
    }

    // Test default values of optional parameters
    @Test
    public void testDefaultParameterValues() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        ConnectionParam param = ConnectionParam.getParam(properties);

        assertEquals(false, param.isVarcharAsString());
        assertEquals(false, param.isEnableCompression());
        assertEquals("", param.getSlaveClusterHost());
        assertEquals(6041, param.getSlaveClusterPort());
        assertEquals(2000, param.getReconnectIntervalMs());
        assertEquals(3, param.getReconnectRetryCount());
        assertEquals(false, param.isEnableAutoConnect());
        assertEquals(false, param.isDisableSslCertValidation());
        assertEquals("java", param.getAppName());
        assertEquals("", param.getAppIp());
        assertEquals(false, param.isCopyData());
        assertEquals(1000, param.getBatchSizeByRow());
        assertEquals(10000, param.getCacheSizeByRow());
        assertEquals(10, param.getBackendWriteThreadNum());
        assertEquals(false, param.isStrictCheck());
        assertEquals(3, param.getRetryTimes());
        assertEquals("", param.getAsyncWrite());
        assertEquals("", param.getPbsMode());
        assertEquals(300, param.getWsKeepAlive());
        assertEquals(10, param.getHealthCheckInitInterval());
        assertEquals(300, param.getHealthCheckMaxInterval());
        assertEquals(1, param.getHealthCheckConTimeout());
        assertEquals(5, param.getHealthCheckCmdTimeout());
        assertEquals(3, param.getHealthCheckRecoveryCount());
        assertEquals(60, param.getHealthCheckRecoveryInterval());
        assertEquals(20, param.getRebalanceThreshold());
        assertEquals(30, param.getRebalanceConBaseCount());
    }
}