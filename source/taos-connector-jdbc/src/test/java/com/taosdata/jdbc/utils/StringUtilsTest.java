package com.taosdata.jdbc.utils;

import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Properties;

public class StringUtilsTest {

    @Test
    public void isEmptyNull() {
        Assert.assertTrue(StringUtils.isEmpty(null));
    }

    @Test
    public void isEmptyEmpty() {
        Assert.assertTrue(StringUtils.isEmpty(""));
    }

    @Test
    public void isNumericNull() {
        Assert.assertFalse(StringUtils.isNumeric(null));
    }

    @Test
    public void isNumericEmpty() {
        Assert.assertFalse(StringUtils.isNumeric(""));
    }

    @Test
    public void isNumericStr() {
        Assert.assertFalse(StringUtils.isNumeric("abc"));
    }

    @Test
    public void isNumericNeg() {
        Assert.assertFalse(StringUtils.isNumeric("-21"));
    }

    @Test
    public void isNumericPoint() {
        Assert.assertFalse(StringUtils.isNumeric("2.15"));
    }

    @Test
    public void isNumeric() {
        Assert.assertTrue(StringUtils.isNumeric("61"));
    }

    @Test
    public void getBasicUrlTest() {
        Assert.assertEquals("jdbc:TAOS://localhost:6030/", StringUtils.getBasicUrl("jdbc:TAOS://localhost:6030/?user=root&password=taosdata"));
        Assert.assertEquals("jdbc:TAOS://localhost:6030/", StringUtils.getBasicUrl("jdbc:TAOS://localhost:6030/"));
    }

    @Test
    public void parseUrlHandlesEmptyUrl() throws SQLException {
        Properties defaults = new Properties();
        Properties result = StringUtils.parseUrl("", defaults);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void parseUrlHandlesNullUrl() throws SQLException {
        Properties defaults = new Properties();
        Properties result = StringUtils.parseUrl(null, defaults);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void parseUrlExtractsPropertiesFromValidUrl() throws SQLException {
        Properties defaults = new Properties();
        defaults.setProperty("user", "root");
        defaults.setProperty("password", "taosdata");

        Properties result = StringUtils.parseUrl("jdbc:TAOS://127.0.0.1:6030/db?charset=UTF-8", defaults);
        Assert.assertEquals("127.0.0.1:6030", result.getProperty("endpoints"));
        Assert.assertEquals("db", result.getProperty("dbname"));
        Assert.assertEquals("UTF-8", result.getProperty("charset"));
    }
    @Test
    public void parseUrlHandlesUrlWithoutParameters() throws SQLException {
        Properties defaults = new Properties();
        Properties result = StringUtils.parseUrl("jdbc:TAOS://127.0.0.1:6030/db", defaults);
        Assert.assertEquals("127.0.0.1:6030", result.getProperty("endpoints"));
        Assert.assertEquals("db", result.getProperty("dbname"));
        Assert.assertNull(result.getProperty("charset"));
    }

    @Test
    public void parseUrlIpv6Native() throws SQLException {
        Properties defaults = new Properties();
        Properties result = StringUtils.parseUrl("jdbc:TAOS://[fe80::1%eth0]:6030/db", defaults);
        Assert.assertEquals("[fe80::1%eth0]:6030", result.getProperty("endpoints"));
        Assert.assertEquals("db", result.getProperty("dbname"));
    }
    @Test
    public void parseUrlIpv6Ws() throws SQLException {
        Properties defaults = new Properties();
        Properties result = StringUtils.parseUrl("jdbc:TAOS-WS://[fe80::1%eth0]:6041/db", defaults);
        Assert.assertEquals("[fe80::1%eth0]:6041", result.getProperty("endpoints"));
        Assert.assertEquals("db", result.getProperty("dbname"));
    }
    @Test
    public void bytesToHexTest() {
        byte[] bytes = new byte[]{0x0A, 0x1B, 0x2C, 0x3D, (byte) 0xFE, (byte) 0xFF};
        String hexString = StringUtils.bytesToHex(bytes);
        Assert.assertEquals("0A1B2C3DFEFF", hexString);
    }

}