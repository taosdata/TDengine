package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import static org.junit.Assert.*;


public class SetConfigurationInJNITest {

    private String host = "127.0.0.1";
    private String dbname = "test_jni";
    private long maxSQLLength = 1024000;
    private String debugFlagJSON = "{ \"debugFlag\": \"135\"}";
    private String maxSqlLengthJSON = "{ \"maxSQLLength\": " + maxSQLLength + "}";

    @Test
    public void testDebugFlag() {
        try {
            // when
            TSDBJNIConnector.setConfig(debugFlagJSON);

            // init
            TSDBJNIConnector.initImp(null);
            TSDBJNIConnector.setOptions(0, null);
            TSDBJNIConnector.setOptions(1, null);
            TSDBJNIConnector.setOptions(2, null);
            String tsCharset = TSDBJNIConnector.getTsCharset();
            assertEquals("", tsCharset);

            // connect
            TSDBJNIConnector jniConnector = new TSDBJNIConnector();
            boolean connected = jniConnector.connect(host, 0, null, "root", "taosdata");
            assertTrue(connected);

            String[] setupSqls = {
                    "drop database if exists " + dbname,
                    "create database if not exists " + dbname,
                    "use " + dbname,
                    "create table weather(ts timestamp, f1 int) tags(loc nchar(10))",
                    "insert into t1 using weather tags('beijing') values(now, 1)",
                    "drop database if exists " + dbname
            };

            Arrays.asList(setupSqls).forEach(sql -> {
                try {
                    long setupSql = jniConnector.executeQuery(sql);
                    if (jniConnector.isUpdateQuery(setupSql)) {
                        jniConnector.freeResultSet(setupSql);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testMaxSQLLength() {

    }

    @Test
    public void setConfigAfterConnect() {
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS:/" + host + ":0/?user=root&password");

            TSDBJNIConnector.setConfig(debugFlagJSON);

            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
//        try {
//            Connection conn = DriverManager.getConnection("jdbc:TAOS:/" + host + ":0/?user=root&password");
//            Statement stmt = conn.createStatement();
//
////            stmt.execute("drop database if exists " + dbname);
//
//            stmt.close();
//            conn.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }
}