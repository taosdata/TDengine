package com.taosdata.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class SetConfigurationInJNITest {

    private String host = "127.0.0.1";
    private String dbname = "test_set_config";

    @Test
    public void setConfigInUrl() {
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root&password=taosdata&debugFlag=135");
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
    public void setConfigInProperties() {
        try {
            Properties props = new Properties();
            props.setProperty("debugFlag", "135");
            Connection conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root&password=taosdata", props);

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