package com.taosdata.jdbc.cases;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.jdbc.TSDBDriver;
import org.junit.*;

import java.sql.*;
import java.util.Properties;

public class TD4174Test {
    private Connection conn;
    private static final String host = "127.0.0.1";

    @Test
    public void test() {
        long ts = System.currentTimeMillis();
        try (PreparedStatement pstmt = conn.prepareStatement("insert into weather values(" + ts + ", ?)")) {
            JSONObject value = new JSONObject();
            value.put("name", "John Smith");
            value.put("age", 20);
            Assert.assertEquals("{\"name\":\"John Smith\",\"age\":20}",value.toJSONString());
            pstmt.setString(1, value.toJSONString());

            int ret = pstmt.executeUpdate();
            Assert.assertEquals(1, ret);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        JSONObject value = new JSONObject();
        value.put("name", "John Smith");
        value.put("age", 20);
        System.out.println(value.toJSONString());
    }

    @Before
    public void before() throws SQLException {
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, "en_US.UTF-8");
        properties.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        conn = DriverManager.getConnection(url, properties);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists td4174");
            stmt.execute("create database if not exists td4174");
            stmt.execute("use td4174");
            stmt.execute("create table weather(ts timestamp, text binary(64))");
        }
    }

    @After
    public void after() throws SQLException {
        if (conn != null)
            conn.close();

    }

}
