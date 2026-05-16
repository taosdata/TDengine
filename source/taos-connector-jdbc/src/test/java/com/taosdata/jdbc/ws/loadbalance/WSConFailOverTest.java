package com.taosdata.jdbc.ws.loadbalance;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.common.Endpoint;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import com.taosdata.jdbc.ws.TaosAdapterMock;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@FixMethodOrder
public class WSConFailOverTest {
    private static final String HOST_A = TestEnvUtil.getHost();
    private static final int PORT_A = TestEnvUtil.getWsPort();

    private static final String HOST_B = TestEnvUtil.getHost();
    private static final int PORT_B = 9041;
    private final String dbName = TestUtils.camelToSnake(WSConFailOverTest.class);
    private static final String TABLE_NAME = "meters";
    private Connection connection;
    private TaosAdapterMock taosAdapterMock;

    @Description("query")
    @Test
    public void queryBlock() throws Exception  {
        try (Statement statement = connection.createStatement()) {

            ResultSet resultSet;
            for (int i = 0; i < 4; i++){
                try {
                    if (i == 2){
                        taosAdapterMock.stop();
                    }
                    resultSet = statement.executeQuery("select ts from " + dbName + "." + TABLE_NAME + " limit 1;");

                }catch (SQLException e){
                    if (e.getErrorCode() == TSDBErrorNumbers.ERROR_RESULTSET_CLOSED){
                        System.out.println("connection closed");
                        break;
                    }

                    if (e.getErrorCode() ==  TSDBErrorNumbers.ERROR_QUERY_TIMEOUT){
                        System.out.println("req timeout, will be continue");
                        continue;
                    }

                    System.out.println(e.getMessage());
                    continue;
                }
                resultSet.next();
                System.out.println(resultSet.getLong(1));
                Thread.sleep(2000);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        taosAdapterMock.start();
        RebalanceTestUtil.waitHealthCheckFinishedIgnoreException(new Endpoint(HOST_A, taosAdapterMock.getListenPort(), false));
    }

    @Before
    public void before() throws SQLException, InterruptedException, IOException, URISyntaxException {
        taosAdapterMock = new TaosAdapterMock(9041);
        taosAdapterMock.start();

        String url;
        url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST_A + ":" + PORT_A + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_INIT_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_MAX_INTERVAL, "1");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HEALTH_CHECK_RECOVERY_COUNT, "1");

        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database " + dbName);
        statement.execute("use " + dbName);
        statement.execute("create table if not exists " + dbName + "." + TABLE_NAME + "(ts timestamp, f int)");
        statement.execute("insert into " + dbName + "." + TABLE_NAME + " values (now, 1)");
        statement.close();
        connection.close();

        url = SpecifyAddress.getInstance().getWebSocketWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST_B + ":" + PORT_B + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_HOST, HOST_A);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_SLAVE_CLUSTER_PORT, String.valueOf(PORT_A));
        properties.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, "true");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_INTERVAL_MS, "2000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_RECONNECT_RETRY_COUNT, "3");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "10000");
        connection = DriverManager.getConnection(url, properties);
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(60);
            statement.execute("drop database if exists " + dbName);
            statement.close();
            connection.close();
        }
        taosAdapterMock.stop();
        Assert.assertEquals(0, RebalanceManager.getInstance().getBgHealthCheckInstanceCount());
        RebalanceManager.getInstance().clearAllForTest();
    }
}