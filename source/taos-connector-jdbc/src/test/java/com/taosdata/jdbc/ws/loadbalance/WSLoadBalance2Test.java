package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@FixMethodOrder
public class WSLoadBalance2Test {


    @Description("query")
    @Test(expected = SQLException.class)
    public void queryBlockWithMasterSlaveDown() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

        mockB.start();
        mockC.start();
        mockB.stop();
        mockC.stop();

        Properties properties = new Properties();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + mockB.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, TestEnvUtil.getHost());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(mockC.getListenPort()));

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");

        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select 1;")) {
            resultSet.next();
            System.out.println(resultSet.getLong(1));
        }
    }

    @Description("query")
    @Test
    public void queryBlock() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

        mockB.start();
        mockC.start();

        Properties properties = new Properties();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + mockB.getListenPort() + "," + TestEnvUtil.getHost() + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }

        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select 1")) {

        }
        mockB.stop();
        mockC.stop();
    }

    @Description("query")
    @Test(expected = SQLException.class)
    public void loadBalanceAndSlave() throws Exception  {
        TaosAdapterMock mockB = new TaosAdapterMock();
        TaosAdapterMock mockC = new TaosAdapterMock();

        mockB.start();
        mockC.start();

        Properties properties = new Properties();
        String url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + TestEnvUtil.getHost() + ":" + mockB.getListenPort() + "," + TestEnvUtil.getHost() + ":" + mockC.getListenPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }

        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, TestEnvUtil.getHost());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(mockC.getListenPort()));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "5000");

        try (Connection connection = DriverManager.getConnection(url, properties);
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery("select 1")) {

        }
        mockB.stop();
        mockC.stop();
    }

    @BeforeClass
    static public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        System.setProperty("ENV_TAOS_JDBC_TEST", "false");
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "TRUE");
    }
    @AfterClass
    static public void after() {
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        System.setProperty("ENV_TAOS_JDBC_NO_HEALTH_CHECK", "");
        RebalanceManager.getInstance().clearAllForTest();
    }

}