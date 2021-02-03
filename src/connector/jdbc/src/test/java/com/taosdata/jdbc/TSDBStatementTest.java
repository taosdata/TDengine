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
        final String dbName = ("test_" + UUID.randomUUID()).replace("-", "_").substring(0, 32);
        try {
            int affectRows = stmt.executeUpdate("create database " + dbName);
            Assert.assertEquals(0, affectRows);
            affectRows = stmt.executeUpdate("create table " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
            Assert.assertEquals(0, affectRows);
            affectRows = stmt.executeUpdate("insert into " + dbName + ".t1 using " + dbName + ".weather tags('北京') values(now, 22.33)");
            Assert.assertEquals(1, affectRows);
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
        final String dbName = ("test_" + UUID.randomUUID()).replace("-", "_").substring(0, 32);
        try {
            boolean isSelect = stmt.execute("create database " + dbName);
            Assert.assertEquals(false, isSelect);
            int affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);

            isSelect = stmt.execute("create table " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
            Assert.assertEquals(false, isSelect);
            affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);

            isSelect = stmt.execute("insert into " + dbName + ".t1 using " + dbName + ".weather tags('北京') values(now, 22.33)");
            Assert.assertEquals(false, isSelect);
            affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(1, affectedRows);

            isSelect = stmt.execute("select * from " + dbName + ".weather");
            Assert.assertEquals(true, isSelect);

            isSelect = stmt.execute("drop database " + dbName);
            Assert.assertEquals(false, isSelect);
            affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getResultSet() {
        final String dbName = ("test_" + UUID.randomUUID()).replace("-", "_").substring(0, 32);
        try {
            boolean isSelect = stmt.execute("create database " + dbName);
            Assert.assertEquals(false, isSelect);
            int affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);

            isSelect = stmt.execute("create table " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
            Assert.assertEquals(false, isSelect);
            affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);

            isSelect = stmt.execute("insert into " + dbName + ".t1 using " + dbName + ".weather tags('北京') values(now, 22.33)");
            Assert.assertEquals(false, isSelect);
            affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(1, affectedRows);

            isSelect = stmt.execute("select * from " + dbName + ".weather");
            Assert.assertEquals(true, isSelect);
            ResultSet rs = stmt.getResultSet();
            Assert.assertNotNull(rs);
            ResultSetMetaData meta = rs.getMetaData();
            Assert.assertEquals(3, meta.getColumnCount());
            int count = 0;
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ": " + rs.getString(i) + "\t");
                }
                System.out.println();
                count++;
            }
            Assert.assertEquals(1, count);

            isSelect = stmt.execute("drop database " + dbName);
            Assert.assertEquals(false, isSelect);
            affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getUpdateCount() {
        execute();
    }

    @Test
    public void addBatch() {
        final String dbName = ("test_" + UUID.randomUUID()).replace("-", "_").substring(0, 32);
        try {
            stmt.addBatch("create database " + dbName);
            stmt.addBatch("create table " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
            stmt.addBatch("insert into " + dbName + ".t1 using " + dbName + ".weather tags('北京') values(now, 22.33)");
            stmt.addBatch("select * from " + dbName + ".weather");
            stmt.addBatch("drop database " + dbName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clearBatch() {
        final String dbName = ("test_" + UUID.randomUUID()).replace("-", "_").substring(0, 32);
        try {
            stmt.clearBatch();
            stmt.addBatch("create database " + dbName);
            stmt.addBatch("create table " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
            stmt.addBatch("insert into " + dbName + ".t1 using " + dbName + ".weather tags('北京') values(now, 22.33)");
            stmt.addBatch("select * from " + dbName + ".weather");
            stmt.addBatch("drop database " + dbName);
            stmt.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void executeBatch() {
        final String dbName = ("test_" + UUID.randomUUID()).replace("-", "_").substring(0, 32);
        try {
            stmt.addBatch("create database " + dbName);
            stmt.addBatch("create table " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
            stmt.addBatch("insert into " + dbName + ".t1 using " + dbName + ".weather tags('北京') values(now, 22.33)");
            stmt.addBatch("select * from " + dbName + ".weather");
            stmt.addBatch("drop database " + dbName);
            int[] results = stmt.executeBatch();
            Assert.assertEquals(0, results[0]);
            Assert.assertEquals(0, results[1]);
            Assert.assertEquals(1, results[2]);
            Assert.assertEquals(Statement.SUCCESS_NO_INFO, results[3]);
            Assert.assertEquals(0, results[4]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getConnection() {
        try {
            Connection connection = stmt.getConnection();
            Assert.assertNotNull(connection);
            Assert.assertTrue(this.conn == connection);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void isClosed() {
        try {
            Assert.assertEquals(false, stmt.isClosed());
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
