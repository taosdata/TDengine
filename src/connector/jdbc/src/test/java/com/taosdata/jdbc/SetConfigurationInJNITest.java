package com.taosdata.jdbc;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


public class SetConfigurationInJNITest {

    private String host = "127.0.0.1";
    private String dbname = "test_jni";
    private String debugFlagJSON = "{ \"debugFlag\": \"135\"}";

    private long maxSQLLength = 1024000;
    private String maxSqlLengthJSON = "{ \"maxSQLLength\": " + maxSQLLength + "}";

    @Test
    public void setConfigBeforeConnectIsValid() {
        try {
            TSDBJNIConnector.setConfig(debugFlagJSON);

            Connection conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root&password=taosdata");
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("drop database if exists " + dbname);

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void setConfigAfterConnectIsInvalid() {
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root&password=taosdata");

            TSDBJNIConnector.setConfig(debugFlagJSON);
            Statement stmt = conn.createStatement();

            stmt.execute("drop database if exists " + dbname);
            stmt.execute("create database if not exists " + dbname);
            stmt.execute("use " + dbname);
            stmt.execute("create table weather(ts timestamp, f1 int) tags(loc nchar(10))");
            stmt.execute("drop database if exists " + dbname);

            stmt.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}