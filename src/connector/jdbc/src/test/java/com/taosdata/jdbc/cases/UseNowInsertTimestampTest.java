package com.taosdata.jdbc.cases;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UseNowInsertTimestampTest {
    String url = "jdbc:TAOS://127.0.0.1:6030/?user=root&password=taosdata";

    @Test
    public void millisec() {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test precision 'ms'");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            assertEquals(13, Long.toString(ts.getTime()).length());

            int nanos = ts.getNanos();
            assertEquals(0, nanos % 1000_000);

            stmt.execute("drop database if exists test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void microsec() {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test precision 'us'");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();
            Timestamp ts = rs.getTimestamp("ts");
            int nanos = ts.getNanos();

            assertEquals(0, nanos % 1000);

            stmt.execute("drop database if exists test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void nanosec() {
        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists test");
            stmt.execute("create database if not exists test precision 'ns'");
            stmt.execute("use test");
            stmt.execute("create table weather(ts timestamp, f1 int)");
            stmt.execute("insert into weather values(now, 1)");

            ResultSet rs = stmt.executeQuery("select * from weather");
            rs.next();

            Timestamp ts = rs.getTimestamp("ts");

            int nanos = ts.getNanos();
            assertTrue(nanos % 1000 != 0);

            stmt.execute("drop database if exists test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
