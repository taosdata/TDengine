package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SubscribeTest {
    Connection connection;
    Statement statement;
    String dbName = "test";
    String tName = "t0";
    String host = "localhost";
    String topic = "test";

    @Before
    public void createDatabase() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_HOST, host);
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            connection = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/", properties);

            statement = connection.createStatement();
            statement.executeUpdate("create database if not exists " + dbName);
            statement.executeUpdate("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
            long ts = System.currentTimeMillis();
            for (int i = 0; i < 2; i++) {
                ts += i;
                String sql = "insert into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")";
                statement.executeUpdate(sql);
            }

        } catch (ClassNotFoundException | SQLException e) {
            return;
        }
    }

    @Test
    public void subscribe() throws Exception {
        TSDBSubscribe subscribe = null;
        try {

            String rawSql = "select * from " + dbName + "." + tName + ";";
            System.out.println(rawSql);
            subscribe = ((TSDBConnection) connection).subscribe(topic, rawSql, false);

            int a = 0;
            while (true) {
                Thread.sleep(900);
                TSDBResultSet resSet = subscribe.consume();

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
            if (null != subscribe) {
                subscribe.close(true);
            }
        }
    }

    @After
    public void close() {
        try {
            statement.executeQuery("drop database " + dbName);
            if (statement != null)
                statement.close();
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}