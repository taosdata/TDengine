package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

public class TSDBAsyncSubscribeTest {
    Connection connection = null;
    Statement statement = null;
    String dbName = "test";
    String tName = "t0";
    String host = "localhost";
    String topic = "test";
    long subscribId = 0;

    @Before
    public void createDatabase() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + "?user=root&password=taosdata"
                , properties);

        statement = connection.createStatement();
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 5; i++) {
            ts += i;
            statement.executeUpdate("insert into \" + dbName + \".\" + tName + \" values (" + ts + ", " + (100 + i) + ", " + i + ")");
        }
    }

    @Test
    public void subscribe() throws Exception {
        TSDBSubscribe subscribe = null;
        try {
            String rawSql = "select * from " + dbName + "." + tName + ";";
            System.out.println(rawSql);
            subscribe = ((TSDBConnection) connection).createSubscribe();
            subscribId = subscribe.subscribe(topic, rawSql, false, 1000, new CallBack("first"));

            assertTrue(subscribId > 0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Thread.sleep(2000);
        subscribe.unsubscribe(subscribId, true);
    }

    private static class CallBack implements TSDBSubscribeCallBack {
        private String name = "";

        public CallBack(String name) {
            this.name = name;
        }

        @Override
        public void invoke(TSDBResultSet resultSet) {
            try {
                while (null != resultSet && resultSet.next()) {
                    System.out.print("callback_" + name + ": ");
                    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                        System.out.printf(i + ": " + resultSet.getString(i) + "\t");
                    }
                    System.out.println();
                }
                resultSet.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @After
    public void close() throws Exception {
        statement.executeQuery("drop database test");
        statement.close();
        connection.close();
    }
}