package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.TSDBDriver;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

public class TimeZoneTest {

    private String url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";

    @Test
    public void javaTimeZone() {
        LocalDateTime localDateTime = LocalDateTime.of(1970, 1, 1, 0, 0, 0);

        Instant instant = localDateTime.atZone(ZoneId.of("UTC-8")).toInstant();
        System.out.println("UTC-8: " + instant.getEpochSecond() + "," + instant);

        instant = localDateTime.atZone(ZoneId.of("UT")).toInstant();
        System.out.println("UTC: " + instant.getEpochSecond() + "," + instant);


        instant = localDateTime.atZone(ZoneId.of("UTC+8")).toInstant();
        System.out.println("UTC+8: " + instant.getEpochSecond() + "," + instant);
    }

    @Test
    public void taosTimeZone() throws SQLException {
        // given
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-8");

        // when and then
        try (Connection connection = DriverManager.getConnection(url, props)) {
            Statement stmt = connection.createStatement();

            stmt.execute("drop database if exists timezone_test");
            stmt.execute("create database if not exists timezone_test keep 36500");
            stmt.execute("use timezone_test");
            stmt.execute("create table weather(ts timestamp, temperature float)");

            stmt.execute("insert into timezone_test.weather(ts, temperature) values('1970-01-01 00:00:00', 1.0)");

            ResultSet rs = stmt.executeQuery("select * from timezone_test.weather");
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp("ts");
                System.out.println("ts: " + ts.getTime() + "," + ts);
            }

            stmt.execute("insert into timezone_test.weather(ts, temperature) values('1970-01-02 00:00:00', 1.0)");

            rs = stmt.executeQuery("select * from timezone_test.weather");
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp("ts");
                System.out.println("ts: " + ts.getTime() + "," + ts);
            }


            stmt.execute("drop database if exists timezone_test");

            stmt.close();
        }
    }

}