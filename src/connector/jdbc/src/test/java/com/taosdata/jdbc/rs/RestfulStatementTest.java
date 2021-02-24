package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.UUID;

public class RestfulStatementTest {
    private static final String host = "127.0.0.1";
    //        private static final String host = "master";
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
        // test in AfterClass method
    }


    @Test
    public void getMaxFieldSize() throws SQLException {
        Assert.assertEquals(16 * 1024, stmt.getMaxFieldSize());
    }

    @Test(expected = SQLException.class)
    public void setMaxFieldSize() throws SQLException {

        stmt.setMaxFieldSize(0);
        stmt.setMaxFieldSize(-1);
    }

    @Test
    public void getMaxRows() throws SQLException {
        Assert.assertEquals(0, stmt.getMaxRows());
    }

    @Test(expected = SQLException.class)
    public void setMaxRows() throws SQLException {
        stmt.setMaxRows(0);
        stmt.setMaxRows(-1);
    }

    @Test
    public void setEscapeProcessing() throws SQLException {
        stmt.setEscapeProcessing(true);
        stmt.setEscapeProcessing(false);
    }

    @Test
    public void getQueryTimeout() throws SQLException {
        Assert.assertEquals(0, stmt.getQueryTimeout());
    }

    @Test(expected = SQLException.class)
    public void setQueryTimeout() throws SQLException {
        stmt.setQueryTimeout(0);
        stmt.setQueryTimeout(-1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void cancel() throws SQLException {
        stmt.cancel();
    }

    @Test
    public void getWarnings() throws SQLException {
        Assert.assertNull(stmt.getWarnings());
    }

    @Test
    public void clearWarnings() throws SQLException {
        stmt.clearWarnings();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void setCursorName() throws SQLException {
        stmt.setCursorName("");
    }

    @Test
    public void execute() {
        final String dbName = ("test_" + UUID.randomUUID()).replace("-", "_").substring(0, 32);
        try {
            boolean isSelect = stmt.execute("create database if not exists " + dbName);
            Assert.assertEquals(false, isSelect);
            int affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);

            isSelect = stmt.execute("create table if not exists " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
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
            boolean isSelect = stmt.execute("create database if not exists " + dbName);
            Assert.assertEquals(false, isSelect);
            int affectedRows = stmt.getUpdateCount();
            Assert.assertEquals(0, affectedRows);

            isSelect = stmt.execute("create table if not exists " + dbName + ".weather(ts timestamp, temperature float) tags(loc nchar(64))");
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
        // already test in execute method
    }

    @Test
    public void getMoreResults() throws SQLException {
        Assert.assertEquals(false, stmt.getMoreResults());
    }

    @Test(expected = SQLException.class)
    public void setFetchDirection() throws SQLException {
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
        stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
        stmt.setFetchDirection(ResultSet.FETCH_UNKNOWN);
        stmt.setFetchDirection(-1);
    }

    @Test
    public void getFetchDirection() throws SQLException {
        Assert.assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection());
    }

    @Test(expected = SQLException.class)
    public void setFetchSize() throws SQLException {
        stmt.setFetchSize(0);
        stmt.setFetchSize(-1);
    }

    @Test
    public void getFetchSize() throws SQLException {
        stmt.setFetchSize(0);
        Assert.assertEquals(0, stmt.getFetchSize());
        stmt.setFetchSize(0);
    }

    @Test
    public void getResultSetConcurrency() throws SQLException {
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, stmt.getResultSetConcurrency());
    }

    @Test
    public void getResultSetType() throws SQLException {
        Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, stmt.getResultSetType());
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

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testGetMoreResults() throws SQLException {
        Assert.assertEquals(false, stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        stmt.getMoreResults(Statement.KEEP_CURRENT_RESULT);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getGeneratedKeys() throws SQLException {
        stmt.getGeneratedKeys();
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecuteUpdate() throws SQLException {
        stmt.executeUpdate("", 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecuteUpdate1() throws SQLException {
        stmt.executeUpdate("", new int[]{});
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecuteUpdate2() throws SQLException {
        stmt.executeUpdate("", new String[]{});
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecute() throws SQLException {
        stmt.execute("", 1);
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecute1() throws SQLException {
        stmt.execute("", new int[]{});
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void testExecute2() throws SQLException {
        stmt.execute("", new String[]{});
    }

    @Test
    public void getResultSetHoldability() throws SQLException {
        Assert.assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, stmt.getResultSetHoldability());
    }

    @Test
    public void isClosed() {
        try {
            Assert.assertEquals(false, stmt.isClosed());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void setPoolable() throws SQLException {
        stmt.setPoolable(true);
        stmt.setPoolable(false);
    }

    @Test
    public void isPoolable() throws SQLException {
        Assert.assertEquals(false, stmt.isPoolable());
    }

    @Test
    public void closeOnCompletion() throws SQLException {
        stmt.closeOnCompletion();
    }

    @Test
    public void isCloseOnCompletion() throws SQLException {
        Assert.assertFalse(stmt.isCloseOnCompletion());
    }

    @Test
    public void unwrap() throws SQLException {
        RestfulStatement unwrap = stmt.unwrap(RestfulStatement.class);
        Assert.assertNotNull(unwrap);
    }

    @Test
    public void isWrapperFor() throws SQLException {
        Assert.assertTrue(stmt.isWrapperFor(RestfulStatement.class));
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
            properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root&password=taosdata", properties);
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
