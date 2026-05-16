package com.taosdata.jdbc;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

import static org.junit.Assert.*;

public class TSDBDriverTest {

    private static String[] validURLs;
    private Connection conn;

    @Test
    public void connectWithJdbcURL() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://localhost:" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        conn = DriverManager.getConnection(url);
        assertNotNull("failure - connection should not be null", conn);
    }

    @Test
    public void connectWithProperties() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://localhost:" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        conn = DriverManager.getConnection(url, connProps);
        assertNotNull("failure - connection should not be null", conn);
    }

    @Test
    public void connectWithConfigFile() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://:/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");
        conn = DriverManager.getConnection(url, connProps);
        assertNotNull("failure - connection should not be null", conn);
    }

    @Test
    public void testParseURL() throws SQLException {
        TSDBDriver driver = new TSDBDriver();
        String endpoints = "127.0.0.1:0";

        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://127.0.0.1:0/db?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&charset=UTF-8";
        } else {
            url += "db?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&charset=UTF-8";
        }
        String host = SpecifyAddress.getInstance().getHost();
        if (host == null) {
            host = "127.0.0.1";
        }
        String port = SpecifyAddress.getInstance().getJniPort();
        if (port == null) {
            port = "0";
        }
        Properties config = new Properties();
        Properties actual = driver.parseURL(url, config);
        assertEquals("failure - host should be " + endpoints, endpoints, actual.get("endpoints"));
        assertEquals("failure - dbname should be db", "db", actual.get("dbname"));
        assertEquals("failure - user should be root", "root", actual.get("user"));
        assertEquals("failure - password should be taosdata", "taosdata", actual.get("password"));
        assertEquals("failure - charset should be UTF-8", "UTF-8", actual.get("charset"));

        url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://127.0.0.1:0";
        }
        host = SpecifyAddress.getInstance().getHost();
        if (host == null) {
            host = "127.0.0.1";
        }
        port = SpecifyAddress.getInstance().getJniPort();
        if (port == null) {
            port = "0";
        }
        config = new Properties();
        actual = driver.parseURL(url, config);
        assertEquals("failure - host should be " + endpoints, endpoints, actual.getProperty("endpoints"));
        assertNull("failure - dbname should be null", actual.get("dbname"));

        url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://127.0.0.1:0/db";
        } else {
            url += "db";
        }
        host = SpecifyAddress.getInstance().getHost();
        if (host == null) {
            host = "127.0.0.1";
        }
        port = SpecifyAddress.getInstance().getJniPort();
        if (port == null) {
            port = "0";
        }
        config = new Properties();
        actual = driver.parseURL(url, config);
        assertEquals("failure - host should be " + endpoints, endpoints, actual.getProperty("endpoints"));
        assertEquals("failure - dbname should be db", "db", actual.get("dbname"));

        url = "jdbc:TAOS://:/?";
        config = new Properties();
        config.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        config.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());
        actual = driver.parseURL(url, config);
        assertEquals("failure - user should be root", "root", actual.getProperty("user"));
        assertEquals("failure - password should be taosdata", "taosdata", actual.getProperty("password"));
        assertEquals("failure - password should be :", ":", actual.getProperty("endpoints"));
        assertNull("failure - dbname should be null", actual.getProperty("dbname"));
    }

    @Test
    public void parseURLWithIPv6Host() throws SQLException {
        TSDBDriver driver = new TSDBDriver();
        Properties defaults = new Properties();
        defaults.setProperty(TSDBDriver.PROPERTY_KEY_USER, TestEnvUtil.getUser());
        defaults.setProperty(TSDBDriver.PROPERTY_KEY_PASSWORD, TestEnvUtil.getPassword());

        Properties result = driver.parseURL("jdbc:TAOS://[2001:db8::1]:6030/db?charset=UTF-8", defaults);
        assertEquals("failure - host should be 2001:db8::1", "[2001:db8::1]:6030", result.getProperty(TSDBDriver.PROPERTY_KEY_ENDPOINTS));
        assertEquals("failure - dbname should be db", "db", result.getProperty(TSDBDriver.PROPERTY_KEY_DBNAME));
        assertEquals("failure - charset should be UTF-8", "UTF-8", result.getProperty(TSDBDriver.PROPERTY_KEY_CHARSET));
    }

    @Test(expected = SQLException.class)
    public void acceptsURL() throws SQLException {
        Driver driver = new TSDBDriver();
        for (String url : validURLs) {
            assertTrue("failure - acceptsURL(\" " + url + " \") should be true", driver.acceptsURL(url));
        }
        driver.acceptsURL(null);
        fail("acceptsURL throws exception when parameter is null");
    }

    @Test
    public void getPropertyInfo() throws SQLException {
        Driver driver = new TSDBDriver();
        String url = SpecifyAddress.getInstance().getJniWithoutUrl();
        if (url == null) {
            url = "jdbc:TAOS://localhost:6030/information_schema?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        } else {
            url += "information_schema?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        String host = SpecifyAddress.getInstance().getHost();
        if (host == null) {
            host = "localhost";
        }
        String port = SpecifyAddress.getInstance().getJniPort();
        if (port == null) {
            port = "6030";
        }
        Properties connProps = new Properties();
        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, connProps);
        for (DriverPropertyInfo info : propertyInfo) {
            if (info.name.equals(TSDBDriver.PROPERTY_KEY_ENDPOINTS))
                assertEquals("failure - endpoint should be " + "localhost:6030", "localhost:6030", info.value);
            if (info.name.equals(TSDBDriver.PROPERTY_KEY_DBNAME))
                assertEquals("failure - dbname should be information_schema", "information_schema", info.value);
            if (info.name.equals(TSDBDriver.PROPERTY_KEY_USER))
                assertEquals("failure - user should be root", "root", info.value);
            if (info.name.equals(TSDBDriver.PROPERTY_KEY_PASSWORD))
                assertEquals("failure - password should be root", "taosdata", info.value);
        }
    }

    @Test
    public void getMajorVersion() {
        assertEquals(3, new TSDBDriver().getMajorVersion());
    }

    @Test
    public void getMinorVersion() {
        assertEquals(0, new TSDBDriver().getMinorVersion());
    }

    @Test
    public void jdbcCompliant() {
        assertFalse(new TSDBDriver().jdbcCompliant());
    }

    @Test
    public void getParentLogger() {
        assertNull(new TSDBDriver().getParentLogger());
    }

    @BeforeClass
    public static void beforeClass() {
        String specifyHost = SpecifyAddress.getInstance().getHost();
        if (specifyHost == null) {
            specifyHost = "localhost";
        }
        String port = SpecifyAddress.getInstance().getJniPort();
        if (port == null) {
            port = "6030";
        }
        validURLs = new String[]{
                "jdbc:TAOS://" + specifyHost + ":0",
                "jdbc:TAOS://" + specifyHost,
                "jdbc:TAOS://" + specifyHost + ":" + port + "/test",
                "jdbc:TAOS://" + specifyHost + ":" + port,
                "jdbc:TAOS://" + specifyHost + ":" + port + "/",
                "jdbc:TSDB://" + specifyHost + ":" + port,
                "jdbc:TSDB://" + specifyHost + ":" + port + "/",
                "jdbc:TAOS://" + specifyHost + ":0/db?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword(),
                "jdbc:TAOS://:",
                "jdbc:TAOS://:/",
                "jdbc:TAOS://:/test",
                "jdbc:TAOS://" + specifyHost + ":0/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword()
        };
    }
}