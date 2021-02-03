package com.taosdata.jdbc;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;

public class TSDBStatementTest {
    private static final String host = "127.0.0.1";
    private static Connection conn;
    private static Statement stmt;

    @Test
    public void executeQuery() {
        try {
            ResultSet rs = stmt.executeQuery("show databases");
            Assert.assertNotNull(rs);
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                }
                System.out.println();
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void executeUpdate() {
        String dbName = "test_" + UUID.randomUUID();
        dbName = dbName.replace("-", "_");
        try {
            int affectRows = stmt.executeUpdate("create database " + dbName);
            Assert.assertEquals(0, affectRows);
            affectRows = stmt.executeUpdate("drop database " + dbName);
            Assert.assertEquals(0, affectRows);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void close() {
    }

    @Test
    public void execute() {

    }

    @Test
    public void getResultSet() {
    }

    @Test
    public void getUpdateCount() {
    }

    @Test
    public void getResultSetConcurrency() {
    }

    @Test
    public void getResultSetType() {
    }

    @Test
    public void addBatch() {
    }

    @Test
    public void clearBatch() {
    }

    @Test
    public void executeBatch() {
    }

    @Test
    public void getConnection() {
    }

    @Test
    public void getMoreResults() {
    }

    @Test
    public void getResultSetHoldability() {
    }

    @Test
    public void isClosed() {
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata", properties);
            stmt = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (stmt != null)
                stmt.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
