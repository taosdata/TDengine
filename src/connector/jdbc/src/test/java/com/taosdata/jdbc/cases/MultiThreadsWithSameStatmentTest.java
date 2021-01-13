package com.taosdata.jdbc.cases;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class MultiThreadsWithSameStatmentTest {


    private class Service {
        public Connection conn;
        public Statement stmt;

        public Service() {
            try {
                Class.forName("com.taosdata.jdbc.TSDBDriver");
                conn = DriverManager.getConnection("jdbc:TAOS://localhost:6030/?user=root&password=taosdata");
                stmt = conn.createStatement();
                stmt.execute("create database if not exists jdbctest");
                stmt.executeUpdate("create table if not exists jdbctest.weather (ts timestamp, f1 int)");
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Before
    public void before() {
    }

    @Test
    public void test() {
        Thread t1 = new Thread(() -> {
            try {
                Service service = new Service();
                ResultSet resultSet = service.stmt.executeQuery("select * from jdbctest.weather");
                while (resultSet.next()) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        System.out.print(metaData.getColumnLabel(i) + ": " + resultSet.getString(i));
                    }
                    System.out.println();
                }

                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                Service service = new Service();
                service.stmt.executeUpdate("insert into jdbctest.weather values(now,1)");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        t1.start();
        sleep(1000);
        t2.start();
    }

    private void sleep(long mills) {
        try {
            TimeUnit.MILLISECONDS.sleep(mills);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @After
    public void after() {
        try {
            if (stmt != null)
                stmt.close();
            if (conn != null)
                conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
