package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.TimestampUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

public class DatetimeBefore1970Test {

    private static Connection conn;

    @Test
    public void test() {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("insert into weather values('1969-12-31 23:59:59.999')");
            stmt.executeUpdate("insert into weather values('1970-01-01 00:00:00.000')");
            stmt.executeUpdate("insert into weather values('1970-01-01 08:00:00.000')");
            stmt.executeUpdate("insert into weather values('1970-01-01 07:59:59.999')");

            ResultSet rs = stmt.executeQuery("select * from weather");
            while (rs.next()) {
                Timestamp ts = rs.getTimestamp("ts");
                System.out.println("long: " + ts.getTime() + ", string: " + TimestampUtil.longToDatetime(ts.getTime()));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("timestamp: " + Long.MAX_VALUE + ", string: " + TimestampUtil.longToDatetime(Long.MAX_VALUE));
        System.out.println("timestamp: " + Long.MIN_VALUE + ", string: " + TimestampUtil.longToDatetime(Long.MIN_VALUE));
        System.out.println("timestamp: " + 0 + ", string: " + TimestampUtil.longToDatetime(0));
        System.out.println("timestamp: " + -1 + ", string: " + TimestampUtil.longToDatetime(-1));
        String datetime = "1970-01-01 00:00:00.000";
        System.out.println("timestamp: " + TimestampUtil.datetimeToLong(datetime) + ", string: " + datetime);
        datetime = "1969-12-31 23:59:59.999";
        System.out.println("timestamp: " + TimestampUtil.datetimeToLong(datetime) + ", string: " + datetime);
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            conn = DriverManager.getConnection("jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata");
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test_timestamp");
            stmt.execute("create database if not exists test_timestamp keep 36500");
            stmt.execute("use test_timestamp");
            stmt.execute("create table weather(ts timestamp)");
            stmt.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try {
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
