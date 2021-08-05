package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SubscribeTest {

    Connection connection;
    Statement statement;
    String dbName = "test";
    String tName = "t0";
    String host = "127.0.0.1";
    String topic = "test";
    private long ts;

    @Test
    public void subscribe() {
        try {
            String rawSql = "select * from " + dbName + "." + tName + ";";
            TSDBConnection conn = connection.unwrap(TSDBConnection.class);
            TSDBSubscribe subscribe = conn.subscribe(topic, rawSql, false);

            for (int j = 0; j < 10; j++) {
                TimeUnit.SECONDS.sleep(1);
                TSDBResultSet resSet = subscribe.consume();

                int rowCnt = 0;
                while (resSet.next()) {
                    if (rowCnt == 0) {
                        long cur_ts = resSet.getTimestamp(1).getTime();
                        int k = resSet.getInt(2);
                        int v = resSet.getInt(3);
                        Assert.assertEquals(ts, cur_ts);
                        Assert.assertEquals(100, k);
                        Assert.assertEquals(1, v);
                    }
                    if (rowCnt == 1) {
                        long cur_ts = resSet.getTimestamp(1).getTime();
                        int k = resSet.getInt(2);
                        int v = resSet.getInt(3);
                        Assert.assertEquals(ts + 1, cur_ts);
                        Assert.assertEquals(101, k);
                        Assert.assertEquals(2, v);

                    }
                    rowCnt++;
                }
                if (j == 0)
                    Assert.assertEquals(2, rowCnt);
                resSet.close();
            }
            subscribe.close(true);


        } catch (SQLException | InterruptedException throwables) {
            throwables.printStackTrace();
        }
    }

    @Before
    public void createDatabase() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

        statement = connection.createStatement();
        statement.execute("drop database if exists " + dbName);
        statement.execute("create database if not exists " + dbName);
        statement.execute("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
        ts = System.currentTimeMillis();
        statement.executeUpdate("insert into " + dbName + "." + tName + " values (" + ts + ", 100, 1)");
        statement.executeUpdate("insert into " + dbName + "." + tName + " values (" + (ts + 1) + ", 101, 2)");
    }

    @After
    public void close() {
        try {
            statement.execute("drop database " + dbName);
            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}