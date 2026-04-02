package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DriverAutoloadTest {

    private Properties properties;
    private final String host = TestEnvUtil.getHost();

    @Test
    public void testRestful() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Connection conn = DriverManager.getConnection(url, properties);
        Assert.assertNotNull(conn);
        conn.close();
    }

    @Test
    public void testJni() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Connection conn = DriverManager.getConnection(url, properties);
        Assert.assertNotNull(conn);
        conn.close();
    }

    @Before
    public void before() {
        properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
    }

}

