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

public class SubscribeTest extends BaseTest {
    Connection connection = null;
    Statement statement = null;
    String dbName = "test";
    String tName = "t0";
    String host = "localhost";
    String topic = "test";

    @Before
    public void createDatabase() throws SQLException {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
        } catch (ClassNotFoundException e) {
            return;
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/" + "?user=root&password=taosdata"
                , properties);

        statement = connection.createStatement();
        statement.executeUpdate("create database if not exists " + dbName);
        statement.executeUpdate("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 2; i++) {
            ts += i;
            statement.executeUpdate("insert into \" + dbName + \".\" + tName + \" values (" + ts + ", " + (100 + i) + ", " + i + ")");
        }
    }

    @Test
    public void subscribe() throws Exception {
        TSDBSubscribe subscribe = null;
        long subscribId = 0;
        try {

            String rawSql = "select * from " + dbName + "." + tName + ";";
            System.out.println(rawSql);
            subscribe = ((TSDBConnection) connection).createSubscribe();
            subscribId = subscribe.subscribe(topic, rawSql, false, 1000);

            assertTrue(subscribId > 0);

            int a = 0;
            while (true) {
                Thread.sleep(900);
                TSDBResultSet resSet = subscribe.consume(subscribId);

                while (resSet.next()) {
                    for (int i = 1; i <= resSet.getMetaData().getColumnCount(); i++) {
                        System.out.printf(i + ": " + resSet.getString(i) + "\t");
                    }
                    System.out.println("\n======" + a + "==========");
                }
                resSet.close();
                a++;
                if (a >= 2) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != subscribe && 0 != subscribId) {
                subscribe.unsubscribe(subscribId, true);
            }
        }
    }

    @After
    public void close() throws Exception {
        statement.executeQuery("drop database " + dbName);
        statement.close();
        connection.close();
        Thread.sleep(10);
    }
}