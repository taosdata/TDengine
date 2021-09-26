package com.taosdata.jdbc;

import org.junit.*;

import java.sql.*;
import java.util.Random;

public class OpenTSDBMigrateTest {
    private static final String host = "127.0.0.1";
    private static final String dbname = "opentsdb_migrate_test";
    private static final Random random = new Random(System.currentTimeMillis());
    private static TSDBConnection conn;

    @Test
    public void telnetPut() {
        System.out.println("test");
        try {
            // given
            String[] lines = new String[]{"stb0_0 1626006833639000000ns 4i8 host=\"host0\" interface=\"eth0\"",
                    "stb0_1 1626006833639000000ns 4i8 host=\"host0\" interface=\"eth0\"",
                    "stb0_2 1626006833639000000ns 4i8 host=\"host0\" interface=\"eth0\""};
//            // when
            conn.getConnector().insertTelnetLines(lines);
            // then
//            long actual = rs.getLong(1);
//            Assert.assertEquals(ms, actual);
//            actual = rs.getLong("ts");
//            Assert.assertEquals(ms, actual);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void before() {

    }

    @BeforeClass
    public static void beforeClass() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            conn = (TSDBConnection) DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname + " precision 'ns'");
            stmt.execute("use " + dbname);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @AfterClass
    public static void afterClass() {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("drop database if exists " + dbname);
            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
