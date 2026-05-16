package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SelectTest {
    Connection connection;
    final String dbName = TestUtils.camelToSnake(SelectTest.class);
    final String tName = "t0";
    final String host = TestEnvUtil.getHost();

    @Before
    public void createDatabaseAndTable() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":0/";
        }
        connection = DriverManager.getConnection(url, properties);

        Statement stmt = connection.createStatement();
        stmt.execute("drop database if exists " + dbName);
        stmt.execute("create database if not exists " + dbName);
        stmt.execute("create table if not exists " + dbName + "." + tName + " (ts timestamp, k int, v int)");
        stmt.close();
    }

    @Test
    public void selectData() throws SQLException {
        long ts = 1496732686000L;

        try (Statement stmt = connection.createStatement()) {
            for (int i = 0; i < 50; i++) {
                ts++;
                int row = stmt.executeUpdate("insert into " + dbName + "." + tName + " values (" + ts + ", " + (100 + i) + ", " + i + ")");
                assertEquals(1, row);
            }

            String sql = "select * from " + dbName + "." + tName;
            ResultSet resSet = stmt.executeQuery(sql);

            int num = 0;
            while (resSet.next()) {
                num++;
            }
            resSet.close();
            assertEquals(num, 50);
        }

    }

    @After
    public void close() throws SQLException {
        if (connection != null) {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database " + dbName);
            stmt.close();
            connection.close();
        }
    }
}

