package com.taosdata.jdbc.cases;

import com.taosdata.jdbc.utils.SpecifyAddress;
import com.taosdata.jdbc.utils.TestEnvUtil;
import com.taosdata.jdbc.utils.TestUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.*;
import java.util.Properties;

@Ignore
public class InvalidResultSetPointerTest {

                    static final String HOST = TestEnvUtil.getHost();
                    private static final String DB_NAME = TestUtils.camelToSnake(InvalidResultSetPointerTest.class);
    private static final String STB_NAME = "stb";
    private static final String TB_NAME = "tb";
    private static Connection connection;
    private static final int NUM_OF_STB = 30000;
    private static final int NUM_OF_TB = 3;
    private static int numOfThreads = 100;

    @Test
    public void test() throws SQLException {
        execute("drop database if exists " + DB_NAME);
        execute("create database if not exists " + DB_NAME);
        execute("use " + DB_NAME);
        createSTable();
        createTable();
        insert();
        selectMultiThreading();
        close();
    }

    private void insert() {
        for (int i = 0; i < NUM_OF_STB; i++) {
            for (int j = 0; j < NUM_OF_TB; j++) {
                final String sql = "INSERT INTO " + DB_NAME + "." + TB_NAME + i + "_" + j + " (ts, temperature, humidity, name) values(now, 20.5, 34, \"" + i + "\")";
                System.out.println(sql);
                execute(sql);
            }
        }
    }

    private void createSTable() {
        for (int i = 0; i < NUM_OF_STB; i++) {
            final String sql = "create table if not exists " + DB_NAME + "." + STB_NAME + i + " (ts timestamp, temperature float, humidity int, name BINARY(" + (i % 73 + 10) + ")) TAGS (tag1 INT)";
            execute(sql);
        }
    }

    private void createTable() {
        for (int i = 0; i < NUM_OF_STB; i++) {
            for (int j = 0; j < NUM_OF_TB; j++) {
                final String sql = "create table if not exists " + DB_NAME + "." + TB_NAME + i + "_" + j + " USING " + STB_NAME + i + " TAGS(" + j + ")";
                execute(sql);
            }
        }
    }

    private void close() throws SQLException {
        if (connection != null) {
            this.connection.close();
            System.out.println("connection closed.");
        }
    }

    private void selectMultiThreading() {
        int a = NUM_OF_STB / numOfThreads;
        if (a < 1) {
            numOfThreads = NUM_OF_STB;
            a = 1;
        }

        int b = 0;
        if (numOfThreads != 0) {
            b = NUM_OF_STB % numOfThreads;
        }

        multiThreadingClass[] instance = new multiThreadingClass[numOfThreads];

        int last = 0;
        for (int i = 0; i < numOfThreads; i++) {
            instance[i] = new multiThreadingClass();
            instance[i].id = i;
            instance[i].from = last;
            if (i < b) {
                instance[i].to = last + a;
            } else {
                instance[i].to = last + a - 1;
            }

            last = instance[i].to + 1;
            instance[i].NUM_OF_TB = NUM_OF_TB;
            instance[i].connection = connection;
            instance[i].dbName = DB_NAME;
            instance[i].tbName = TB_NAME;
            instance[i].start();
        }

        for (int i = 0; i < numOfThreads; i++) {
            try {
                instance[i].join();
            } catch (InterruptedException ie) {
                ie.printStackTrace();
            }
        }
    }

    @BeforeClass
    public static void beforeClass() {
        try {
            String url = SpecifyAddress.getInstance().getJniUrl();
            if (url == null) {
                url = "jdbc:TAOS://" + HOST + ":" + TestEnvUtil.getJniPort() + "/?user=" + TestEnvUtil.getUser() + "&password=" + TestEnvUtil.getPassword();
            }
            Class.forName("com.taosdata.jdbc.TSDBDriver");
            Properties properties = new Properties();
            properties.setProperty("charset", "UTF-8");
            properties.setProperty("locale", "en_US.UTF-8");
            properties.setProperty("timezone", "UTC-8");
            System.out.println("get connection starting...");
            connection = DriverManager.getConnection(url, properties);
            if (connection != null)
                System.out.println("[ OK ] Connection established.");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void execute(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            statement.execute(sql);
            long end = System.currentTimeMillis();
            printSql(sql, (end - start));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void printSql(String sql, long cost) {
        System.out.println("time cost: " + cost + " ms, execute statement ====> " + sql);
    }

    private void executeQuery(String sql) {
        try (Statement statement = connection.createStatement()) {
            long start = System.currentTimeMillis();
            ResultSet resultSet = statement.executeQuery(sql);
            long end = System.currentTimeMillis();
            printSql(sql, (end - start));
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    class multiThreadingClass extends Thread {
        public int id;
        public int from, to;
        public int NUM_OF_TB;
        public Connection connection;
        public String dbName;
        public String tbName;

        public void run() {
            System.out.println("ID: " + id + " from: " + from + " to: " + to);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Thread " + id + " interrupted.");
            }

            for (int i = from; i < to; i++) {
                for (int j = 0; j < NUM_OF_TB; j++) {
                    if (j % 1000 == 0) {
                        try {
                            System.out.print(id + "s.");
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            System.out.println("Thread " + id + " interrupted.");
                        }
                    }
                    final String sql = "select last_row(humidity) from " + dbName + "." + tbName + i + "_" + j;
                    executeQuery(sql);
                }
            }
        }
    }

}

