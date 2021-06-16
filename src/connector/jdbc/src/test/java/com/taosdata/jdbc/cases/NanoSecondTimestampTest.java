package com.taosdata.jdbc.cases;

import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class NanoSecondTimestampTest {

    private static final String host = "127.0.0.1";
    private static final String dbname = "nano_sec_test";

    @BeforeClass
    public static void beforeClass() {
        final String url = "jdbc:TAOS://" + host + ":6030/?user=root&password=taosdata";
        try {
            Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname + " precision ns");


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
