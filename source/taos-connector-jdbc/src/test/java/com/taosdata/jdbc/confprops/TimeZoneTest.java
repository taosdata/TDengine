package com.taosdata.jdbc.confprops;

import com.taosdata.jdbc.TSDBDriver;
import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;

@Ignore
public class TimeZoneTest {

    private String url;

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
        String[] tzList= {
                "UTC-7",
//                "UTC+7",

//                "UTC-18",
//                "UTC+18",
//
//                "GMT-7",
//                "GMT+7",
//
//                "GMT-18",
//                "GMT+18",
//
//                "Asia/Shanghai",
//                "Africa/Blantyre",
//                "Pacific/Chuuk",
//                "Europe/Warsaw",
        };

        for (String s : tzList) {
            testTimeZone(s);
        }

    }

    private void testTimeZone(String timezone) throws SQLException {
        Properties props = new Properties();
        props.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, timezone);

        try (Connection connection = DriverManager.getConnection(url, props)) {
            Statement stmt = connection.createStatement();
            System.out.println(ZoneId.systemDefault());

            stmt.execute("drop database if exists timezone_test");
            stmt.execute("create database if not exists timezone_test keep 36500");
            stmt.execute("use timezone_test");
            stmt.execute("create table weather(ts timestamp, temperature float)");

            stmt.execute("insert into timezone_test.weather(ts, temperature) values('1970-01-01 00:00:00', 1.0)");

            ResultSet rs = stmt.executeQuery("select * from timezone_test.weather");
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp("ts");
                assert (ts.equals(Timestamp.valueOf("1970-01-01 00:00:00")));
            }
            stmt.execute("drop database if exists timezone_test");
            stmt.close();
        }
    }

    @Before
    public void before() {
        url = SpecifyAddress.getInstance().getJniUrl();
        if (url == null) {
            url = "jdbc:TAOS://" + TestEnvUtil.getHost() + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword() + "&batchfetch=true";
        }
    }
}