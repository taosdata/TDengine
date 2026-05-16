package com.taosdata.jdbc.ws;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WSIpv6Test {
    private static final String HOST = "[::1]";
    static final int PORT = TestEnvUtil.getWsPort();
    private static Connection connection;
    private static final String DATABASE_NAME = TestUtils.camelToSnake(WSIpv6Test.class);

    private static void testInsert() throws SQLException {
        Statement statement = connection.createStatement();
        long cur = System.currentTimeMillis();
        List<String> timeList = new ArrayList<>();
        for (long i = 0L; i < 30; i++) {
            long t = cur + i;
            timeList.add("insert into " + DATABASE_NAME + ".alltype_query values(" + t + ",1,1,1)");
        }
        for (int i = 0; i < 30; i++) {
            statement.execute(timeList.get(i));
        }
        statement.close();
    }

    @Test
    public void testWSSelect() throws SQLException {
        Statement statement = connection.createStatement();
        int count = 0;
        long start = System.nanoTime();
        for (int i = 0; i < 1; i++) {
            ResultSet resultSet = statement.executeQuery("select ts,c1,c2,c3 from " + DATABASE_NAME + ".alltype_query limit 3000");
            while (resultSet.next()) {
                count++;
                resultSet.getTimestamp(1);
                assertTrue(resultSet.getBoolean(2));
                assertEquals(1, resultSet.getInt(3));
                assertEquals(1, resultSet.getInt(4));
            }
        }
        long d = System.nanoTime() - start;
        statement.close();
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestUtils.runInMain();

        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "";
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_MESSAGE_WAIT_TIMEOUT, "10000");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");
        connection = DriverManager.getConnection(url, properties);
        Statement statement = connection.createStatement();
        statement.execute("drop database if exists " + DATABASE_NAME);
        statement.execute("create database " + DATABASE_NAME);
        statement.execute("create table " + DATABASE_NAME + ".alltype_query(ts timestamp, c1 bool,c2 tinyint, c3 smallint)");
        statement.close();
        testInsert();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (connection != null){
            try(Statement statement = connection.createStatement()) {
                statement.execute("drop database if exists " + DATABASE_NAME);
            }
            connection.close();
        }
    }
}

