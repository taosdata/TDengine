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
public class JdbcConnectionTest {
    private static String host = "192.168.0.1";
    private static String db = "";
    private static String user = "root";
    private static String password = "taosdata";
    private static String port = "0";
    private static String jdbcProtocal = "";
//    private String cfgDir = "D:\\vmshare\\release\\taos-1.5.0-windows-client-x64-20181113-102458\\cfg";
    private static String cfgDir = "";
    private static final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String TSDB_URL = "jdbc:TAOS://192.168.1.113:0/?user=root&password=taosdata";
    private Connection connection;



    public static void main(String[] args) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        JdbcConnectionTest jdbcConnectionTest = new JdbcConnectionTest();
        jdbcConnectionTest.setDb("dbcmp");
        jdbcConnectionTest.setHost("192.168.1.113");
        jdbcConnectionTest.setCfgDir("/home/jyhou/workspace/sim/dnode1/cfg");
//        jdbcConnectionTest.getConnection();

        try {
            Class.forName(TSDB_DRIVER);
        } catch (Exception e) {

        }
        Properties properties = new Properties();
        properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, cfgDir);
//        properties.setProperty(TSDBDriver.LOCALE_KEY, "en_US.UTF-8");
        String[] locales = new String[] {"en_US.UTF-8", "zh_CN.UTF-8", "GBK"};

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 3; i++) {
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, "zh_CN.UTF-8");
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    long count = 0l;
                    try {

                        Connection connection = (Connection) DriverManager.getConnection(TSDB_URL, properties);

                        System.out.println("connected to server");
                        Statement stmt = connection.createStatement();
                        stmt.executeUpdate("use " + db);
                        System.out.println("query: \"select count(*) from devices\"");
                        long start = System.nanoTime();
                        ResultSet res = stmt.executeQuery("select count(*) from devices");
                        while (res.next()) {
//                for (int col = 1; col <= res.getMetaData().getColumnCount(); col++) {
//                    res.getObject(col);
//                }
                            count++;
                        }
                        long end = System.nanoTime();
                        end = end - start;
                        BigDecimal time = BigDecimal.valueOf(end).divide(BigDecimal.valueOf(1e9)); // time used in seconds
                        System.out.printf("Query completed.\n Number of rows retrieved: %d\n Time used: %fs\n", count, time);
                        connection.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println(e.getMessage());
                        System.out.println("failed to connect");
                    }
                }
            });
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // wait
        }
        System.out.println("Finished.");

    }

    public void getConnection() {
        long count = 0l;
        try {
            Class.forName(TSDB_DRIVER);
            Properties properties = new Properties();
            properties.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, cfgDir);
            connection = (Connection) DriverManager.getConnection(TSDB_URL, properties);

            System.out.println("connected to server");
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("use " + db);
            System.out.println("query: \"select * from tb1\"");
            long start = System.nanoTime();
            ResultSet res = stmt.executeQuery("select count(*) from devices");
            while (res.next()) {
//                for (int col = 1; col <= res.getMetaData().getColumnCount(); col++) {
//                    res.getObject(col);
//                }
                count++;
            }
            long end = System.nanoTime();
            end = end - start;
            BigDecimal time = BigDecimal.valueOf(end).divide(BigDecimal.valueOf(1e9)); // time used in seconds
            System.out.printf("Query completed.\n Number of rows retrieved: %d\n Time used: %fs\n", count, time);
        } catch (Exception e) {
            connection = null;
            e.printStackTrace();
            System.out.println(e.getMessage());
            System.out.println("failed to connect");
        } finally {

        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getJdbcProtocal() {
        return jdbcProtocal;
    }

    public void setJdbcProtocal(String jdbcProtocal) {
        this.jdbcProtocal = jdbcProtocal;
    }

    public String getCfgDir() {
        return cfgDir;
    }

    public void setCfgDir(String cfgDir) {
        this.cfgDir = cfgDir;
    }
}
