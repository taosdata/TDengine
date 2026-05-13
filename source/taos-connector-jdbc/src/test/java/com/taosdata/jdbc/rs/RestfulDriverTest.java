package com.taosdata.jdbc.rs;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class RestfulDriverTest {
    static final String HOST = TestEnvUtil.getHost();

    @Test
    public void acceptsURL() throws SQLException {
        Driver driver = new RestfulDriver();
        String url = SpecifyAddress.getInstance().getRestWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + HOST + ":" + TestEnvUtil.getRsPort();
        }
        boolean isAccept = driver.acceptsURL(url);
        Assert.assertTrue(isAccept);
        String specifyHost = SpecifyAddress.getInstance().getHost();
        if (specifyHost == null) {
            url = "jdbc:TAOS://" + HOST + ":6041";
        } else {
            url = "jdbc:TAOS://" + specifyHost + ":6041";
        }
        isAccept = driver.acceptsURL(url);
        Assert.assertFalse(isAccept);
    }

    @Test
    public void getPropertyInfo() throws SQLException {
        Driver driver = new RestfulDriver();

        final String url = "";
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, null);
        Assert.assertNotNull(propertyInfo);
    }

    @Test
    public void getMajorVersion() {
        Assert.assertEquals(3, new RestfulDriver().getMajorVersion());
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

