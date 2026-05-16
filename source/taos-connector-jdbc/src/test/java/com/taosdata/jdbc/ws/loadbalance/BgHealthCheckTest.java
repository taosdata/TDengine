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

@RunWith(CatalogRunner.class)
@FixMethodOrder
public class BgHealthCheckTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(BgHealthCheckTest.class);
    private static final String DB_NAME = TestUtils.camelToSnake(BgHealthCheckTest.class);
    private static final String TABLE_NAME = "meters";
    private static Connection connection;

    private static TaosAdapterMock mockA;
    private static TaosAdapterMock mockB;
    private static TaosAdapterMock mockC;
    private final RebalanceManager rebalanceManager = RebalanceManager.getInstance();

    @Description("test connection count")
    @Test
    public void bgCheckOnDisconnection() throws Exception  {
        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://" + HOST + ":" + mockA.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");

        Connection connection1 = DriverManager.getConnection(url, properties);
        ConnectionParam param = ((WSConnection)connection1).getParam();
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());
        // stop one node
        mockA.stop();

        Thread.sleep(500);

        // trigger background health check
        try( Statement statement1 = connection1.createStatement();
        ResultSet rs = statement1.executeQuery("show databases;")) {

        } catch (SQLException e) {
            // do nothing
        }

        connection1.close();
        Assert.assertEquals(1, rebalanceManager.getBgHealthCheckInstanceCount());

        mockA.start();
        // wait for health check
        Thread.sleep(5000);
        Assert.assertEquals(0, rebalanceManager.getBgHealthCheckInstanceCount());

        Assert.assertTrue(rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).isOnline());
    }

    @Description("test connection count")
    @Test
    public void bgCheckOnConnectionFailed() throws Exception  {
        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://" + HOST + ":" + mockA.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");

        mockA.stop();

        try (Connection connection1 = DriverManager.getConnection(url, properties)) {
            boolean isvalid = connection1.isValid(0);
        } catch (SQLException e) {
            // do nothing
        }

        mockA.start();
        // wait for health check
        Thread.sleep(5000);
        Assert.assertTrue(rebalanceManager.getEndpointInfo(new Endpoint(HOST, mockA.getListenPort(), false)).isOnline());
    }

    @Description("test connection count after node down")
    @Test
    public void connectionCountAfterNodeDownTest() throws Exception  {
        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://" + HOST + ":" + mockA.getListenPort()
                + "," + HOST + ":" + mockB.getListenPort()
                + "," + HOST + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");

        Connection connection1 = DriverManager.getConnection(url, properties);
        ConnectionParam param = ((WSConnection)connection1).getParam();
        Connection connection2 = DriverManager.getConnection(url, properties);
        Connection connection3 = DriverManager.getConnection(url, properties);

        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        mockC.stop();
        // wait for connection lost
        Thread.sleep(1000);
        try (Statement statement = connection3.createStatement();ResultSet rs = statement.executeQuery("show databases;")) {
            while (rs.next()) {
                String dbName = rs.getString(1);
            }
        } catch (SQLException e) {
            log.info("Expected exception when node C is down", e);
        }

        Assert.assertEquals(2, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(0, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());
        mockC.start();
        // wait for health check
        Thread.sleep(3000);

        Connection connection4 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        connection1.close();
        connection2.close();
        connection3.close();
        connection4.close();
    }

    @Description("test connection count after node down")
    @Test
    public void connectionCountAfterCloseTest() throws Exception  {
        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://" + HOST + ":" + mockA.getListenPort()
                + "," + HOST + ":" + mockB.getListenPort()
                + "," + HOST + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");

        Connection connection1 = DriverManager.getConnection(url, properties);
        ConnectionParam param = ((WSConnection)connection1).getParam();
        Connection connection2 = DriverManager.getConnection(url, properties);
        Connection connection3 = DriverManager.getConnection(url, properties);

        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        connection1.close();
        Assert.assertEquals(0, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        connection1 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());

        connection1.close();
        connection2.close();
        connection3.close();
    }

    @Test
    public void testConcurrentConnections() throws Exception {
        String url = "jdbc:TAOS-WS://" +
                HOST + ":" + mockA.getListenPort() + "," +
                HOST + ":" + mockB.getListenPort() + "," +
                HOST + ":" + mockC.getListenPort() +
                "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        Assert.assertNull(rebalanceManager.getEndpointInfo(new Endpoint(HOST, mockA.getListenPort(), false)));
        Assert.assertNull(rebalanceManager.getEndpointInfo(new Endpoint(HOST, mockB.getListenPort(), false)));
        Assert.assertNull(rebalanceManager.getEndpointInfo(new Endpoint(HOST, mockC.getListenPort(), false)));

        int threadCount = 15;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Connection> connections = new ArrayList<>();

        // create and start 15 threads
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    Connection conn = DriverManager.getConnection(url, properties);
                    synchronized (connections) {
                        connections.add(conn);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail("create thread failed, " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(30, java.util.concurrent.TimeUnit.SECONDS);

        Assert.assertEquals("total connections must be 15", 15, connections.size());

        // get connections from every node
        ConnectionParam param = ((WSConnection)connections.get(0)).getParam();
        int countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        int countB = rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount();
        int countC = rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount();

        System.out.println("after concurrent connections：A=" + countA + ", B=" + countB + ", C=" + countC);
        Assert.assertEquals(15, countA + countB + countC);
        Assert.assertEquals(5, countA);
        Assert.assertEquals(5, countB);
        Assert.assertEquals(5, countC);

        // close all connections
        for (Connection conn : connections) {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @BeforeClass
    static public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        TestUtils.runInMain();
        System.setProperty("ENV_TAOS_JDBC_TEST", "test");
        String url;
        url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
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

    @AfterClass
    static public void after() throws SQLException {
        if (null != connection) {
            Statement statement = connection.createStatement();
            statement.execute("drop database if exists " + DB_NAME);
            statement.close();
            connection.close();
        }
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
    }

    @Before
    public void setUp() throws IOException {
        mockA = new TaosAdapterMock();
        mockB = new TaosAdapterMock();
        mockC = new TaosAdapterMock();
        mockA.start();
        mockB.start();
        mockC.start();
    }

    @After
    public void tearDown() {
        if (mockA != null) {
            mockA.stop();
        }
        if (mockB != null) {
            mockB.stop();
        }
        if (mockC != null) {
            mockC.stop();
        }

        RebalanceManager.getInstance().clearAllForTest();
    }
}