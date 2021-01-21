package com.taosdata.jdbc.rs;

import org.junit.Assert;
import org.junit.Test;

import java.sql.*;

public class RestfulDriverTest {
    private static final String host = "master";

    @Test
    public void connect() {

    }

    @Test
    public void acceptsURL() throws SQLException {
        Driver driver = new RestfulDriver();
        boolean isAccept = driver.acceptsURL("jdbc:TAOS-RS://" + host + ":6041");
        Assert.assertTrue(isAccept);
        isAccept = driver.acceptsURL("jdbc:TAOS://" + host + ":6041");
        Assert.assertFalse(isAccept);
    }

    @Test
    public void getPropertyInfo() throws SQLException {
        Driver driver = new RestfulDriver();
        final String url = "";
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, null);
        for (DriverPropertyInfo prop : propertyInfo) {
            System.out.println(prop);
        }
    }

    @Test
    public void getMajorVersion() {
        Assert.assertEquals(2, new RestfulDriver().getMajorVersion());
    }

    @Test
    public void getMinorVersion() {
        Assert.assertEquals(0, new RestfulDriver().getMinorVersion());
    }

    @Test
    public void jdbcCompliant() {
        Assert.assertFalse(new RestfulDriver().jdbcCompliant());
    }

    @Test(expected = SQLFeatureNotSupportedException.class)
    public void getParentLogger() throws SQLFeatureNotSupportedException {
        new RestfulDriver().getParentLogger();
    }
}
