package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBErrorNumbers;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;

public class AuthenticationTest {

    static final String HOST = TestEnvUtil.getHost();
    private static final String USER = TestEnvUtil.getUser();
    private static final String PASSWORD = "taos?data";
    @Test
    public void connectWithoutUserByJni() {
        try {
            String url = SpecifyAddress.getInstance().getJniWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + HOST + ":0/?";
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x2319): user is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutUserByRestful() {
        try {
            String url = SpecifyAddress.getInstance().getRestWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?";
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_USER_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x2319): user is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutPasswordByJni() {
        try {
            String url = SpecifyAddress.getInstance().getJniWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + HOST + ":0/?user=" + USER;
            } else {
                url += "?user=" + USER;
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x231a): password is required", e.getMessage());
        }
    }

    @Test
    public void connectWithoutPasswordByRestful() {
        try {
            String url = SpecifyAddress.getInstance().getRestWithoutUrl();
            if (url == null) {
                url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + USER;
            } else {
                url += "?user=" + USER;
            }
            DriverManager.getConnection(url);
        } catch (SQLException e) {
            Assert.assertEquals(TSDBErrorNumbers.ERROR_PASSWORD_IS_REQUIRED, e.getErrorCode());
            Assert.assertEquals("ERROR (0x231a): password is required", e.getMessage());
        }
    }

    @Ignore
    @Test
    public void test() throws SQLException {
        Connection conn;
        // change password
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + USER + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("alter user " + USER + " pass '" + PASSWORD + "'");
        stmt.close();
        conn.close();

        // use new to login and execute query
        url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort() + "/?user=" + USER + "&password=" + PASSWORD;
        } else {
            url += "?user=" + USER + "&password=" + PASSWORD;
        }
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        Assert.assertNotNull(stmt);
        stmt.execute("show databases");
        ResultSet rs = stmt.getResultSet();
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.print(meta.getColumnLabel(i) + ":" + rs.getString(i) + "\t");
            }
            System.out.println();
        }

        // change password back
        conn = DriverManager.getConnection(url);
        stmt = conn.createStatement();
        stmt.execute("alter user " + USER + " pass '" + TestEnvUtil.getPassword() + "'");
        stmt.close();
        conn.close();
    }

}

