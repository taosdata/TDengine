package com.taosdata.jdbc.utils;

import com.taosdata.jdbc.AbstractStatement;
import com.taosdata.jdbc.TSDBDriver;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ReqIdTest {
    static final String host = TestEnvUtil.getHost();

    @Test
    public void testMurmurHash32() {
        long i = ReqId.murmurHash32("driver-go".getBytes(StandardCharsets.UTF_8), 0);

        Assert.assertEquals(3037880692L, i);
    }

    @Test
    public void testGetReqID() {
        ReqId.getReqID();
    }

    @Test
    public void testQueryWithReqId() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + host + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }

        try (Connection connection = DriverManager.getConnection(url);
                AbstractStatement statement = (AbstractStatement) connection.createStatement();
                ResultSet rs = statement.executeQuery("show databases", ReqId.getReqID())) {
            List<String> list = new ArrayList<>();
            while (rs.next()) {
                list.add(rs.getString("name"));
            }
            Assert.assertTrue(list.contains("information_schema"));
        }
    }

    @Test
    public void testQueryRestWithReqId() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }

        try (Connection connection = DriverManager.getConnection(url);
                AbstractStatement statement = (AbstractStatement) connection.createStatement();
                ResultSet rs = statement.executeQuery("show databases", ReqId.getReqID())) {
            List<String> list = new ArrayList<>();
            while (rs.next()) {
                list.add(rs.getString("name"));
            }
            Assert.assertTrue(list.contains("information_schema"));
        }
    }

    @Test
    public void testQueryWSWithReqId() throws SQLException {
        String url = SpecifyAddress.getInstance().getRestUrl();
        if (url == null) {
            url = "jdbc:TAOS-RS://" + host + ":" + TestEnvUtil.getRsPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties config = new Properties();
        config.setProperty(TSDBDriver.PROPERTY_KEY_BATCH_LOAD, "true");

        try (Connection connection = DriverManager.getConnection(url, config);
                AbstractStatement statement = (AbstractStatement) connection.createStatement();
                ResultSet rs = statement.executeQuery("show databases", ReqId.getReqID())) {
            List<String> list = new ArrayList<>();
            while (rs.next()) {
                list.add(rs.getString("name"));
            }
            Assert.assertTrue(list.contains("information_schema"));
        }
    }
}