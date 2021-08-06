package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBErrorNumbers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

public class AuthenticationTest {

    private static final String host = "127.0.0.1";
    private static final String user = "root";
    private static final String password = "taos?data";
    private Connection conn;

    @Test
    public void connectWithoutUserByJni() {
        try {
            DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?");
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (2319): user is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutUserByRestful() {
        try {
            DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?");
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (2319): user is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutPasswordByJni() {
        try {
            DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root");
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (231a): password is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutPasswordByRestful() {
        try {
            DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/?user=root");
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (231a): password is required", e.getMessage());
        }
    }

    @Ignore
    @Test
    public void test() {
        // change password
        try {
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/restful_test?user=" + user + "&password=taosdata");
            Statement stmt = conn.createStatement();
            stmt.execute("alter user " + user + " pass '" + password + "'");
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // use new to login and execute query
        try {
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/restful_test?user=" + user + "&password=" + password);
            Statement stmt = conn.createStatement();
            stmt.execute("show databases");
            ResultSet rs = stmt.getResultSet();
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    System.out.print(meta.getColumnLabel(i) + ":" + rs.getString(i) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // change password back
        try {
            conn = DriverManager.getConnection("jdbc:TAOS-RS://" + host + ":6041/restful_test?user=" + user + "&password=" + password);
            Statement stmt = conn.createStatement();
            stmt.execute("alter user " + user + " pass 'taosdata'");
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Before
    public void before() {
        try {
            Class.forName("com.taosdata.jdbc.rs.RestfulDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

}
