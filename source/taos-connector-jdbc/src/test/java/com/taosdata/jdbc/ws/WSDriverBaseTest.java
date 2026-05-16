package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.annotation.CatalogRunner;
import com.taosdata.jdbc.annotation.Description;
import com.taosdata.jdbc.annotation.TestTarget;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Properties;

@RunWith(CatalogRunner.class)
@TestTarget(alias = "websocket query test", author = "huolibo", version = "2.0.38")
@FixMethodOrder
public class WSDriverBaseTest {

        static final String HOST = TestEnvUtil.getHost();
        static final int PORT = TestEnvUtil.getWsPort();
    private final String db_name = TestUtils.camelToSnake(WSDriverBaseTest.class);
    private static final String TABLE_NAME = "wq";
    private Connection connection;

    @Description("query")
    @Test
    public void queryBlock() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("insert into " + db_name + "." + TABLE_NAME + " values(now+100s, 100)");
            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + TABLE_NAME)) {
                resultSet.next();
                Assert.assertEquals(100, resultSet.getInt(2));
            }

            statement.execute("select 1 where 0");
            try (ResultSet resultSet = statement.executeQuery("select * from " + db_name + "." + TABLE_NAME)) {
                resultSet.next();
                Assert.assertEquals(100, resultSet.getInt(2));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Before
    public void before() throws SQLException {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "80");
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-WS://" + HOST + ":" + TestEnvUtil.getWsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "";
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "false");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + db_name);
        statement.execute("create database " + db_name);
        statement.execute("use " + db_name);
        statement.execute("create table if not exists " + db_name + "." + TABLE_NAME + "(ts timestamp, f int)");
        statement.close();
    }

    @After
    public void after() throws SQLException {
        if (null != connection) {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("drop database if exists " + db_name);
            } catch (SQLException e) {
                // do nothing
            }
            connection.close();
        }
    }
}

