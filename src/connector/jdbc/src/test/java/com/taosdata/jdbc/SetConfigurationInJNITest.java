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
            Connection conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root&password=taosdata&debugFlag=143&rpcTimer=500");
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
            props.setProperty("debugFlag", "143");
            props.setProperty("r pcTimer", "500");
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

    @Test
    //test case1:set debugFlag=135
    //expect:debugFlag:135 
    //result:pass
    public void setConfigfordebugFlag() {
        try {
            Properties props = new Properties();
            //set debugFlag=135
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
    @Test
    //test case2:set debugFlag=abc (wrong type)
    //expect:debugFlag:135 
    //result:pass
    public void setConfigforwrongtype() {
        try {
            Properties props = new Properties();
            //set debugFlag=135
            props.setProperty("debugFlag", "abc");
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
    @Test
    //test case3:set rpcTimer=0 (smaller than the boundary conditions)
    //expect:rpcTimer:300 
    //result:pass
    public void setConfigrpcTimer() {
        try {
            Properties props = new Properties();
            //set rpcTimer=0
            props.setProperty("rpcTimer", "0");
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
    @Test
    //test case4:set rpcMaxTime=10000 (bigger than the boundary conditions)
    //expect:rpcMaxTime:600 
    //result:pass
    public void setConfigforrpcMaxTime() {
        try {
            Properties props = new Properties();
            //set rpcMaxTime=10000
            props.setProperty("rpcMaxTime", "10000");
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
    @Test
    //test case5:set numOfThreadsPerCore=aaa (wrong type)
    //expect:numOfThreadsPerCore:1.0 
    //result:pass
    public void setConfigfornumOfThreadsPerCore() {
        try {
            Properties props = new Properties();
            //set numOfThreadsPerCore=aaa
            props.setProperty("numOfThreadsPerCore", "aaa");
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
    @Test
    //test case6:set numOfThreadsPerCore=100000 (bigger than the boundary conditions)
    //expect:numOfThreadsPerCore:1.0 
    //result:pass
    public void setConfignumOfThreadsPerCore() {
        try {
            Properties props = new Properties();
            //set numOfThreadsPerCore=100000
            props.setProperty("numOfThreadsPerCore", "100000");
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
    @Test
    // test case7:set both true and wrong config(debugFlag=0,rpcDebugFlag=143,cDebugFlag=143,rpcTimer=100000)
    // expect:rpcDebugFlag:143,cDebugFlag:143,rpcTimer:300
    // result:pass
    public void setConfigformaxTmrCtrl() {
        try {
            Properties props = new Properties();
            props.setProperty("debugFlag", "0");
            props.setProperty("rpcDebugFlag", "143");
            props.setProperty("cDebugFlag", "143");
            props.setProperty("rpcTimer", "100000");
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
    @Test
    //test case 8:use url to set with wrong type(debugFlag=abc,rpcTimer=abc)
    //expect:default value
    //result:pass
    public void setConfigInUrlwithwrongtype() {
        try {
            Connection conn = DriverManager.getConnection("jdbc:TAOS://" + host + ":0/?user=root&password=taosdata&debugFlag=abc&rpcTimer=abc");
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
