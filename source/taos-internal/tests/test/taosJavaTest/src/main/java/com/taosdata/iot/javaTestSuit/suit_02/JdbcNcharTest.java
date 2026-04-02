package com.taosdata.iot.javaTestSuit.suit_02;

import com.taosdata.jdbc.TSDBDriver;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test case
 */
public class JdbcNcharTest {
    private static String db = "";
//    private String cfgDir = "D:\\vmshare\\release\\taos-1.5.0-windows-client-x64-20181113-102458\\cfg";
    private static String cfgDir = "";
    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String TSDB_URL = "jdbc:TAOS://192.168.1.106:0/?user=root&password=taosdata";

    public static void main(String[] args) throws SQLException {
        Connection connection = null;
        JdbcNcharTest jdbcConnectionTest = new JdbcNcharTest();
        jdbcConnectionTest.setDb("db");

        long count = 0l;
        try {

            Class.forName(TSDB_DRIVER);
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "UTF-8");
            connection = DriverManager.getConnection(TSDB_URL, properties);

            System.out.println("connected to server");
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists " + db);
            stmt.executeUpdate("create database " + db);
            stmt.executeUpdate("use " + db);
            String sql = "create table tb1 (ts timestamp, c1 binary(20), c2 nchar(10))";
            System.out.println("query: " + sql);
            stmt.executeUpdate(sql);
            sql = "insert into tb1 values (now-1s, 'imbinary20', '樱木花道')";
            System.out.println("query: " + sql);
            stmt.executeUpdate(sql);
            sql = "insert into tb1 values (now, 'imbinary20', 'さくらぎはなみち')";
            System.out.println("query: " + sql);
            stmt.executeUpdate(sql);
            sql = "select * from tb1";
            System.out.println("query: " + sql);
            long start = System.nanoTime();
            ResultSet res = stmt.executeQuery(sql);
            while (res.next()) {
                for (int col = 1; col <= res.getMetaData().getColumnCount(); col++) {
                    System.out.printf("%s  | ", res.getString(col));
                }
                System.out.println("");
                count++;
            }
            long end = System.nanoTime();
            end = end - start;
            BigDecimal time = BigDecimal.valueOf(end).divide(BigDecimal.valueOf(1e9)); // time used in seconds
            System.out.printf("Query completed.\nNumber of rows retrieved: %d\nTime used: %fs\n", count, time);
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.out.println("failed to connect");
        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, cfgDir);

        ExecutorService executorService = Executors.newFixedThreadPool(500);
        System.out.println("Finished.");

    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }
}
