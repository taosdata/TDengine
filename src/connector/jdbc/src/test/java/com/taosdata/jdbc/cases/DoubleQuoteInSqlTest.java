package com.taosdata.jdbc.cases;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBDriver;
import org.junit.*;

import java.sql.*;
import java.util.Properties;

public class DoubleQuoteInSqlTest {
    private static final String host = "127.0.0.1";
    private static final String dbname = "td4174";

    private Connection conn;

    @Test
    public void test() {
        // given
        long ts = System.currentTimeMillis();
        JSONObject value = new JSONObject();
        value.put("name", "John Smith");
        value.put("age", 20);

        // when
        int ret = 0;
        try (PreparedStatement pstmt = conn.prepareStatement("insert into weather values(" + ts + ", ?)")) {
            pstmt.setString(1, value.toJSONString());
            ret = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // then
        Assert.assertEquals("{\"name\":\"John Smith\",\"age\":20}", value.toJSONString());
        Assert.assertEquals(1, ret);
    }

    @Before
    public void before() {
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        try {
            conn = DriverManager.getConnection(url, properties);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table weather(ts timestamp, text binary(64))");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
