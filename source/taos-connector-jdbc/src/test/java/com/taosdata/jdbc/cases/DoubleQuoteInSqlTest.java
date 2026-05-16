package com.taosdata.jdbc.cases;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.JsonUtil;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class DoubleQuoteInSqlTest {

                    static final String HOST = TestEnvUtil.getHost();
                    private static final String DB_NAME = TestUtils.camelToSnake(DoubleQuoteInSqlTest.class);

    private Connection conn;

    @Test
    public void test() {
        // given
        long ts = System.currentTimeMillis();
        ObjectNode value = JsonUtil.getObjectMapper().createObjectNode();
        value.put("name", "John Smith");
        value.put("age", 20);

        // when
        int ret = 0;
        try (PreparedStatement pstmt = conn.prepareStatement("insert into weather values(" + ts + ", ?)")) {
            pstmt.setString(1, JsonUtil.getObjectMapper().writeValueAsString(value));
            ret = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        // then
        Assert.assertEquals("{\"name\":\"John Smith\",\"age\":20}", value.toString());
        Assert.assertEquals(1, ret);
    }

    @Before
    public void before() throws SQLException {
        String url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        conn = DriverManager.getConnection(url, properties);
        Statement stmt = conn.createStatement();
        stmt.execute("drop database if exists " + DB_NAME);
        stmt.execute("create database if not exists " + DB_NAME);
        stmt.execute("use " + DB_NAME);
        stmt.execute("create table weather(ts timestamp, text binary(64))");
    }

    @After
    public void after() {
        if (null != conn) {
            try {
                Statement stmt = conn.createStatement();
                stmt.execute("drop database if exists " + DB_NAME);
                stmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}

