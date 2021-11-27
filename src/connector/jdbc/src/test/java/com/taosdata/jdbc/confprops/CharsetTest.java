package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

public class CharsetTest {
    private static final String host = "127.0.0.1";

    @Test
    public void test() throws SQLException {
        // given
        String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");

        try (Connection conn = DriverManager.getConnection(url, props);
             Statement stmt = conn.createStatement()) {

            // when
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, temperature nchar(10))");
            stmt.execute("insert into weather values(now, '北京')");

            // then
            ResultSet rs = stmt.executeQuery("select * from weather");
            while (rs.next()) {
                Object value = rs.getObject("temperature");
                Assert.assertTrue(value instanceof String);
                Assert.assertEquals("北京", value.toString());
            }
        }
    }

}
