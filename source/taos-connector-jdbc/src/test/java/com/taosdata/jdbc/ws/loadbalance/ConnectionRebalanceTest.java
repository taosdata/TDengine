package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.common.ConnectionParam;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import com.taosdata.jdbc.ws.WSConnection;
import org.junit.*;
import org.junit.runner.RunWith;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
    * Test class for connection rebalance mechanism in WebSocket load balancing
    * Covers trigger conditions, connection migration rules, and concurrent scenarios
    */
@RunWith(CatalogRunner.class)
@FixMethodOrder
public class ConnectionRebalanceTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(ConnectionRebalanceTest.class);
    private static final String DB_NAME = TestUtils.camelToSnake(ConnectionRebalanceTest.class);
    private static final String TABLE_NAME = "meters";
    private static Connection connection;

    private static TaosAdapterMock mockA;
    private static TaosAdapterMock mockB;
    private static TaosAdapterMock mockC;
    private final RebalanceManager rebalanceManager = RebalanceManager.getInstance();

    // Rebalance configuration constants
    private static final int REBALANCE_CON_BASE_COUNT = 2; // Minimum total connections to trigger rebalance
    private static final int REBALANCE_THRESHOLD = 50; // Percentage threshold for connection ratio (50%)

    /**
        * Test: Rebalance is triggered when all trigger conditions are met after node recovery
        * Verify rebalance execution when total connections > threshold AND connection ratio > threshold
        */
    @Description("test rebalance trigger when all conditions are satisfied")
    @Test
    public void testRebalanceTriggerOnAllConditionsMet() throws Exception {
        // 1. Configure JDBC properties (including rebalance thresholds and health check params)
        Properties properties = getRebalanceProperties();
        String url = getClusterUrl();

        System.out.println("ENV_TAOS_JDBC_NO_HEALTH_CHECK: " + System.getProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK"));

        // 2. Simulate node failure: stop mockA, create connections to migrate traffic to mockB/mockC
        mockA.stop();
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 3; i++) { // Create 3 connections (total > REBALANCE_CON_BASE_COUNT=2)
            connections.add(DriverManager.getConnection(url, properties));
        }
        ConnectionParam param = ((WSConnection) connections.get(0)).getParam();

        // 3. Verify post-failure connection distribution: mockB/mockC have 1-2 connections each
        int countB = rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount();
        int countC = rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount();
        Assert.assertTrue("Total connections should exceed threshold " + REBALANCE_CON_BASE_COUNT,
                countB + countC >= REBALANCE_CON_BASE_COUNT);
        Assert.assertTrue("Connection ratio should exceed threshold " + REBALANCE_THRESHOLD + "%",
                countB >= 1 * (1 + REBALANCE_THRESHOLD / 100.0) || countC >= 1 * (1 + REBALANCE_THRESHOLD / 100.0));

        // 4. Recover failed node mockA, wait for health check thread detection
        mockA.start();
        RebalanceTestUtil.waitHealthCheckFinished(new Endpoint(HOST, mockA.getListenPort(), false)); // Wait for health check and rebalance trigger
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());

        // trigger rebalance
        try(Statement stmt = connections.get(0).createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.next();
            Assert.assertEquals(1, rs.getInt(1));
        }

        // 5. Verify rebalance triggered: mockA has at least 1 migrated connection
        int countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        Assert.assertTrue("Rebalance should be triggered, mockA connection count ≥ 1", countA >= 1);

        // 6. Clean up resources
        closeConnections(connections);
    }

    /**
        * Test: Rebalance is NOT triggered when total connections are below threshold
        * Verify no rebalance execution even if node recovers (total connections < REBALANCE_CON_BASE_COUNT)
        */
    @Description("test no rebalance when total connections are below threshold")
    @Test
    public void testNoRebalanceOnTotalConnectionBelowThreshold() throws Exception {
        Properties properties = getRebalanceProperties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, "30");
        String url = getClusterUrl();

        // 1. Simulate node failure: stop mockA, create 2 connections (total < REBALANCE_CON_BASE_COUNT=3)
        mockA.stop();
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            connections.add(DriverManager.getConnection(url, properties));
        }
        ConnectionParam param = ((WSConnection) connections.get(0)).getParam();
        Assert.assertFalse(rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).isOnline());

        // 2. Recover mockA, wait for health check
        mockA.start();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(HOST, mockA.getListenPort(), false));
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());

        // assert background healthcheck work
        Assert.assertTrue(rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).isOnline());

        // trigger rebalance
        try(Statement stmt = connections.get(0).createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.next();
            Assert.assertEquals(1, rs.getInt(1));
        }

        // 3. Verify rebalance not triggered: mockA connection count remains 0
        int countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        Assert.assertEquals("Rebalance not triggered, mockA connection count should be 0", 0, countA);

        closeConnections(connections);
    }

    /**
        * Test: Rebalance is NOT triggered when connection ratio is below threshold
        * Verify no rebalance execution even if total connections meet threshold (ratio < REBALANCE_THRESHOLD)
        */
    @Description("test no rebalance when connection ratio is below threshold")
    @Test
    public void testNoRebalanceOnRatioBelowThreshold() throws Exception {
        Properties properties = getRebalanceProperties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, "50"); // Adjust threshold to 200%
        String url = getClusterUrl();

        // 1. Simulate node failure: stop mockA, create 3 connections (total ≥ threshold, ratio < 100%)
        mockA.stop();
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            connections.add(DriverManager.getConnection(url, properties));
        }

        // 2. Recover mockA, wait for health check
        mockA.start();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(HOST, mockA.getListenPort(), false));
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());

        // new connection, choose node A
        connections.add(DriverManager.getConnection(url, properties));
        ConnectionParam param = ((WSConnection) connections.get(0)).getParam();
        int countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        Assert.assertEquals("Rebalance not triggered, mockA connection count should be 1", 1, countA);

        // will not trigger rebalance
        try(Statement stmt = connections.get(0).createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.next();
            Assert.assertEquals(1, rs.getInt(1));
        }

        // 3. Verify rebalance not triggered: mockA connection count is 1
        countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        Assert.assertEquals("Rebalance not triggered, mockA connection count should be 1", 1, countA);

        closeConnections(connections);
    }

    /**
        * Test: Non-idle connections (with unfinished queries) do not participate in migration
        * Verify connections with pending result sets/statements are excluded from rebalance
        */
    @Description("test non-idle connection (with unfinished query) does not migrate")
    @Test
    public void testNonIdleConnectionWithQueryNotMigrate() throws Exception {
        // 1. Configure JDBC properties (including rebalance thresholds and health check params)
        Properties properties = getRebalanceProperties();
        String url = getClusterUrl();

        // 2. Simulate node failure: stop mockA, create connections to migrate traffic to mockB/mockC
        mockA.stop();
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 3; i++) { // Create 3 connections (total > REBALANCE_CON_BASE_COUNT=2)
            connections.add(DriverManager.getConnection(url, properties));
        }
        ConnectionParam param = ((WSConnection) connections.get(0)).getParam();

        Statement stmt = connections.get(0).createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");

        // 3. Recover failed node mockA, wait for health check thread detection
        mockA.start();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(HOST, mockA.getListenPort(), false)); // Wait for health check and rebalance trigger
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());

        // 4. trigger rebalance
        rs.next();
        Assert.assertEquals(1, rs.getInt(1));
        rs.close();

        // 5. Verify rebalance triggered: mockA has at least 1 migrated connection
        int countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        Assert.assertEquals("Rebalance should not be triggered, mockA connection count == 0", 0, countA);

        stmt.close();
        // 6. Clean up resources
        closeConnections(connections);
    }

    /**
        * Test: No connection conflicts/leaks in concurrent rebalance scenarios
        * Verify connection migration works correctly with concurrent node recovery and connection creation
        */
    @Description("test concurrent rebalance without connection conflicts or leaks")
    @Test
    public void testConcurrentRebalance() throws Exception {
        Properties properties = getRebalanceProperties();
        String url = getClusterUrl();

        // 1. Simulate node failure + create batch idle connections
        mockA.stop();
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            connections.add(DriverManager.getConnection(url, properties));
        }

        // 2. Concurrent node recovery + rebalance trigger
        CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            try {
                mockA.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            latch.countDown();
        }).start();
        new Thread(() -> {
            try {
                for (int i = 0; i < 3; i++) {
                    DriverManager.getConnection(url, properties);
                }
            } catch (SQLException e) {
                Assert.fail("Failed to create connection concurrently: " + e.getMessage());
            }
            latch.countDown();
        }).start();
        latch.await(30, TimeUnit.SECONDS);
        RebalanceTestUtil.waitHealthCheckFinished(new Endpoint(HOST, mockA.getListenPort(), false));
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());

        connections.parallelStream().forEach(conn -> checkConnection(conn));

        // 3. Verify no concurrent conflicts: correct total connections, mockA has migrated connections
        ConnectionParam param = ((WSConnection) connections.get(0)).getParam();
        int countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        int countB = rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount();
        int countC = rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount();
        Assert.assertEquals("Total connection count should be correct", 9, countA + countB + countC);
        Assert.assertTrue("mockA should have migrated connections", countA >= 2);

        closeConnections(connections);
    }

    @Test
    public void testOtherClusterNoRebalance() throws Exception {
        Properties propertiesA = getRebalanceProperties();
        Properties propertiesB = getRebalanceProperties();
        String clusterA =  "jdbc:TAOS-WS://" + HOST + ":" + mockA.getListenPort() + "," + HOST + ":" + mockB.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        String clusterB =  "jdbc:TAOS-WS://" + HOST + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        // 1. Simulate node failure + create batch idle connections
        mockA.stop();
        List<Connection> connections = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            connections.add(DriverManager.getConnection(clusterA, propertiesA));
            connections.add(DriverManager.getConnection(clusterB, propertiesB));
        }

        mockA.start();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(HOST, mockA.getListenPort(), false));
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());

        Connection connection1 = connections.get(5);
        checkConnection(connection1);

        // 3. Verify no rebalance for other cluster
        ConnectionParam paramA = ((WSConnection) connections.get(0)).getParam();
        int countA = rebalanceManager.getEndpointInfo(paramA.getEndpoints().get(1)).getConnectCount();
        Assert.assertEquals(3, countA);

        ConnectionParam paramB = ((WSConnection) connections.get(1)).getParam();
        int countB = rebalanceManager.getEndpointInfo(paramB.getEndpoints().get(0)).getConnectCount();
        Assert.assertEquals(3, countB);

        closeConnections(connections);
    }

    private static void checkConnection(Connection conn) {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.next();
            Assert.assertEquals(1, rs.getInt(1));
            System.out.printf("Connection check succeeded, thread: %s%n", Thread.currentThread().getName());
        } catch (Exception e) {
            throw new RuntimeException("Connection check failed", e);
        } catch (AssertionError e) {
            throw new AssertionError("Check result does not meet expectations", e);
        }
    }

    // -------------------------- Utility Methods --------------------------
    /**
        * Get JDBC properties with rebalance configuration
        * Includes auto-reconnect, health check, and rebalance threshold settings
        * @return Configured Properties object
        */
    private Properties getRebalanceProperties() {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");
        // Rebalance configuration
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_CON_BASE_COUNT, String.valueOf(REBALANCE_CON_BASE_COUNT));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_REBALANCE_THRESHOLD, String.valueOf(REBALANCE_THRESHOLD));
        // Health check configuration (accelerate node status detection)
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");
        return properties;
    }

    /**
        * Get JDBC URL for 3-node cluster (mockA, mockB, mockC)
        * @return Formatted cluster JDBC URL
        */
    private String getClusterUrl() {
        return "jdbc:TAOS-WS://" + HOST + ":" + mockA.getListenPort()
                + "," + HOST + ":" + mockB.getListenPort()
                + "," + HOST + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
    }

    /**
        * Batch close connections to clean up resources
        * Logs warnings for failed connection closures
        * @param connections List of connections to close
        */
    private void closeConnections(List<Connection> connections) {
        for (Connection conn : connections) {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    log.warn("Failed to close connection", e);
                }
            }
        }
    }

    // -------------------------- Lifecycle Methods --------------------------
    /**
        * One-time setup before all test methods
        * Initializes test database and table for WS connection testing
        */
    @BeforeClass
    public static void beforeClass() throws SQLException, InterruptedException, IOException, URISyntaxException {
        TestUtils.runInMain();
        System.setProperty("ENV_TAOS_JDBC_TEST", "test");
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + DB_NAME);
        statement.execute("create database " + DB_NAME);
        statement.execute("use " + DB_NAME);
        statement.execute("create table if not exists " + DB_NAME + "." + TABLE_NAME + "(ts timestamp, f int)");
        statement.execute("insert into " + DB_NAME + "." + TABLE_NAME + " values (now, 1)");
        statement.close();
    }

    /**
        * One-time teardown after all test methods
        * Drops test database and closes main connection
        */
    @AfterClass
    public static void afterClass() throws SQLException {
        if (null != connection) {
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + DB_NAME);
            statement.close();
            connection.close();
        }
    }

    /**
        * Setup before each test method
        * Initializes and starts 3 mock TaosAdapter nodes (mockA, mockB, mockC)
        */
    @Before
    public void setUp() throws IOException {
        mockA = new TaosAdapterMock();
        mockB = new TaosAdapterMock();
        mockC = new TaosAdapterMock();
        mockA.start();
        mockB.start();
        mockC.start();
    }

    /**
        * Teardown after each test method
        * Stops all mock TaosAdapter nodes to clean up resources
        */
    @After
    public void tearDown() {
        if (mockA != null) mockA.stop();
        if (mockB != null) mockB.stop();
        if (mockC != null) mockC.stop();

        RebalanceManager.getInstance().clearAllForTest();
    }
}