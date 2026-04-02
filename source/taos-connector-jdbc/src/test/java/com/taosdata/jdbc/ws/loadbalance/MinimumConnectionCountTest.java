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
public class MinimumConnectionCountTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(MinimumConnectionCountTest.class);
    private static final String DB_NAME = TestUtils.camelToSnake(MinimumConnectionCountTest.class);
    private static final String TABLE_NAME = "meters";
    private static Connection connection;

    private static TaosAdapterMock mockA;
    private static TaosAdapterMock mockB;
    private static TaosAdapterMock mockC;
    private final RebalanceManager rebalanceManager = RebalanceManager.getInstance();

    @Description("test connection count")
    @Test
    public void connectionCountTest() throws Exception  {
        Properties properties = new Properties();
        String url = "jdbc:TAOS-WS://" + HOST + ":" + mockA.getListenPort()
                + "," + HOST + ":" + mockB.getListenPort()
                + "," + HOST + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        Connection connection1 = DriverManager.getConnection(url, properties);
        ConnectionParam param = ((WSConnection)connection1).getParam();
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(0, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(0, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        Connection connection2 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(0, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        Connection connection3 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        String url2 = "jdbc:TAOS-WS://"
                + "," + HOST + ":" + mockA.getListenPort()
                + "," + HOST + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        Connection connection4 = DriverManager.getConnection(url2, properties);
        Assert.assertEquals(2, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Connection connection5 = DriverManager.getConnection(url2, properties);
        Assert.assertEquals(2, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        Assert.assertEquals(2, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());
        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());

        Connection connection6 = DriverManager.getConnection(url, properties);
        Assert.assertEquals(2, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());

        // stop one node
        mockA.stop();

        Thread.sleep(500);
        // check the connection count
        Statement statement1 = connection1.createStatement();
        ResultSet rs = statement1.executeQuery("show databases;");
        rs.close();
        statement1.close();

        Statement statement2 = connection4.createStatement();
        ResultSet rs2 = statement2.executeQuery("show databases;");
        rs2.close();
        statement2.close();

        Assert.assertEquals(3, rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount());
        Assert.assertEquals(3, rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount());

        connection1.close();
        connection2.close();
        connection3.close();
        connection4.close();
        connection5.close();
        connection6.close();
        // restart mockA
        mockA.start();
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

        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
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
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(param.getEndpoints().get(2));

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

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    Connection conn = DriverManager.getConnection(url, properties);
                    synchronized (connections) {
                        connections.add(conn);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail("create thread failed：" + e.getMessage());
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        latch.await(30, java.util.concurrent.TimeUnit.SECONDS);

        Assert.assertEquals("total connections must be 15", 15, connections.size());

        ConnectionParam param = ((WSConnection)connections.get(0)).getParam();
        int countA = rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount();
        int countB = rebalanceManager.getEndpointInfo(param.getEndpoints().get(1)).getConnectCount();
        int countC = rebalanceManager.getEndpointInfo(param.getEndpoints().get(2)).getConnectCount();

        System.out.println("after concurrent connections：A=" + countA + ", B=" + countB + ", C=" + countC);
        Assert.assertEquals(15, countA + countB + countC);
        Assert.assertEquals(5, countA);
        Assert.assertEquals(5, countB);
        Assert.assertEquals(5, countC);

        for (Connection conn : connections) {
            if (conn != null) {
                conn.close();
            }
        }
    }

    @Test
    public void testAutoReconnectOff() throws Exception {
        String url = "jdbc:TAOS-WS://" +
                HOST + ":" + mockA.getListenPort() + "," +
                HOST + ":" + mockB.getListenPort() + "," +
                HOST + ":" + mockC.getListenPort() +
                "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();

        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        Assert.assertNull(rebalanceManager.getEndpointInfo(new Endpoint(HOST, mockA.getListenPort(), false)));
        Assert.assertNull(rebalanceManager.getEndpointInfo(new Endpoint(HOST, mockB.getListenPort(), false)));
        Assert.assertNull(rebalanceManager.getEndpointInfo(new Endpoint(HOST, mockC.getListenPort(), false)));

        Connection conn = DriverManager.getConnection(url, properties);
        ConnectionParam param = ((WSConnection)conn).getParam();

        Assert.assertEquals(1, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        conn.close();
        Assert.assertEquals(0, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());

        conn = DriverManager.getConnection(url, properties);
        mockA.stop();
        Thread.sleep(100);

        try (Statement statement = conn.createStatement();ResultSet rs = statement.executeQuery("show databases;")) {
            while (rs.next()) {
                String dbName = rs.getString(1);
            }
        } catch (SQLException e) {
           // ignore
        }

        Assert.assertEquals(0, rebalanceManager.getEndpointInfo(param.getEndpoints().get(0)).getConnectCount());
        conn.close();
    }

    @BeforeClass
    static public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
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
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }

    @AfterClass
    public static void cleanUp() {
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
    }
}