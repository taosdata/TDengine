package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.Exceptions.TestFailureException;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Test case
 */
public class JdbcConnectionTester extends TaosTester {
    private String host = "localhost";
    private String db = "";
    private String user = "root";
    private String password = "taosdata";
    private String port = "0";
    private String jdbcProtocal = "";
    private String cfgDir = "/etc/taos";
    private final String TSDB_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private final String TSDB_URL = "jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata";
    private int threadNum = 1;

    public static void main(String[] args) throws SQLException {

        PropertyConfigurator.configure("/home/jyhou/workspace/taosdata/test/taosJavaTest/src/main/resources/log4j.properties");
        JdbcConnectionTester jdbcConnectionTester = new JdbcConnectionTester();

        System.out.println("============JDBC Connection Test============");
        // test cases
        jdbcConnectionTester.testGetConnectionByUrlLocale();
        jdbcConnectionTester.testGetConnectionByUrl();
        jdbcConnectionTester.testGetConnectionByUrlInfo();
        jdbcConnectionTester.testGetConnectionByUrlUsrPass();
//        jdbcConnectionTester.threadNum = Integer.valueOf(args[0]);
        jdbcConnectionTester.testJNIConnectorInit();
        jdbcConnectionTester.testDbSelection();
//        for (int i = 0; i < 100; i++) {
//            System.out.println("=====" + i);
//            jdbcConnectionTester.testDbSelection();
//        }
        jdbcConnectionTester.testGetConnectionByUrlLocale();

    }

    public void testGetConnectionByUrl() throws TestFailureException {
        String url1 = "jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata";
        String url2 = "jdbc:TAOS://127.0..1:0/";

        System.out.println("Test: connect by URL");
        Assert.assertTrue(testGetConnection(url1));
        Assert.assertFalse(testGetConnection(url2));
    }

    public void testGetConnectionByUrlInfo() throws TestFailureException {
        String url1 = "jdbc:TAOS://127.0.0.1:0/";
        Properties info1 = new Properties();
        String user1 = "root";
        String password1 = "taosdata";
        info1.put("user", user1);
        info1.put("password", password1);

        Properties info2 = new Properties();
        String user2 = "root1";
        String password2 = "taosdata";
        info2.put("user", user2);
        info2.put("password", password1);

        System.out.println("Test: connect by URL and DB properties");
        Assert.assertTrue(testGetConnection(url1, info1));
        Assert.assertFalse(testGetConnection(url1, info2));
    }

    public void testGetConnectionByUrlUsrPass() throws  TestFailureException {
        String url1 = "jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata";
        String user1 = "root";
        String password1 = "taosdata";
        String url2 = "jdbc:TAOS://127.0.0.1:0/";
        String user2 = "root1";

        System.out.println("Test: connect by URL, user and password");
        Assert.assertTrue(testGetConnection(url1, user1, password1));
        Assert.assertFalse(testGetConnection(url2, user2, password1));
        Assert.assertTrue(testGetConnection(url2, user1, password1));
    }

    public void testGetConnectionByUrlLocale() throws  TestFailureException {
        String url1 = "jdbc:TAOS://192.168.0.1:0/?user=root&password=taosdata&locale=en_US.UTF-8&timezone=UTC-7&cfgdir=/etc/taos";
        String user1 = "root";
        String password1 = "taosdata";
        String url2 = "jdbc:TAOS://127.0.0.1:0/";
        String user2 = "root1";

        System.out.println("Test: connect by URL with a specified locale");
        Assert.assertTrue(testGetConnection(url1));
//        Assert.assertFalse(testGetConnection(url2, user2, password1));
//        Assert.assertTrue(testGetConnection(url2, user1, password1));
    }

    public void testJNIConnectorInit() {
        String url = "jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata";
        Properties info0 = new Properties();
        info0.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
        info0.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-7");

        Properties info1 = new Properties();
        info1.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
        info1.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC+2");

        Properties info2 = new Properties();
        info2.setProperty(TSDBDriver.PROPERTY_KEY_CONFIG_DIR, "/etc/taos");
        info2.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, "UTC-4");

        Connection connection = getConnection(new Properties());
        try {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists db");
            stmt.executeUpdate("create database db ablocks 800 tblocks 200 ");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("\tFailed to create database\n");
            return;
        }

        System.out.printf("JNI connector initialization test, number of threads: %d\n", threadNum);
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            Properties info = null;
            switch (i % 3) {
                case 0:
                    info = info0;
                    System.out.printf("\tcreating the %dth thread: info0\n", i);
                    break;
                case 1:
                    info = info1;
                    System.out.printf("\tcreating the %dth thread: info1\n", i);
                    break;
                case 2:
                    info = info2;
                    System.out.printf("\tcreating the %dth thread: info2\n", i);
                    break;
            }
            executorService.execute(new JdbcConnectionTask(url, info));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {
            // wait till all threads complete their tasks
        }
        return;

    }

    public void testDbSelection() {
        String db = "db007";
        String url0 = "jdbc:TAOS://127.0.0.1:0/?user=root&password=taosdata";
        String url1 = "jdbc:TAOS://127.0.0.1:0/" + db + "?user=root&password=taosdata";
        Connection connection;
        int count = 0;
        int tbNum = 10;
        String tb = "tb";
        try {
            Class.forName(TSDB_DRIVER);
            connection = (Connection) DriverManager.getConnection(url0);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists " + db);
            stmt.executeUpdate("create database " + db);
            stmt.executeUpdate("use " + db);
            for (int i = 0; i < tbNum; i++) {
                stmt.executeUpdate("create table tb" + i + " (ts timestamp, c1 int)");
            }
            connection.close();
            connection = (Connection) DriverManager.getConnection(url1);
            stmt = connection.createStatement();
            ResultSet res = stmt.executeQuery("show tables");
            while (res.next()) {
                count++;
            }
            res.close();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("\tFailed to create database\n");
            return;
        }
        System.out.printf("Number of tables in db: %d\n", count);
        Assert.assertTrue(count == tbNum);
    }

//    public void testNull() {
//        String url = "jdbc:TAOS://127.0.0.1:0/lm_db0?user=root&password=taosdata";
//        Connection connection;
//        int tbNum = 10;
//        String db = "lm_db0";
//        String tb = "lm_tb0";
//        try {
//            Class.forName(TSDB_DRIVER);
//            connection = (Connection) DriverManager.getConnection(url);
//            Statement stmt = connection.createStatement();
//            stmt.executeUpdate("use " + db);
//            ResultSet res = stmt.executeQuery("select min(c2) from lm_stb0 where ts >= 1537146000000 and ts <= 1537151400000 and t1 > 1 and t1 < 8 group by t1 order by t1 asc limit 5 offset 0");
//            ResultSetMetaData metaData = res.getMetaData();
//            while (res.next()) {
//                count++;
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    private class JdbcConnectionTask implements Runnable {

        private String url;
        private Properties info;

        JdbcConnectionTask(String url, Properties info) {
            this.url = url;
            this.info = info;
        }
        @Override
        public void run() {
            try {
                Class.forName(TSDB_DRIVER);
                Connection connection = (Connection) DriverManager.getConnection(url, info);

                // create db for each thread
                /**
                String dbName = Thread.currentThread().getName().replaceAll("pool-1-thread-", "db");
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("drop database if exists " + dbName);
                stmt.executeUpdate("create database " + dbName + " ablocks 800 tblocks 200 ");
                stmt.executeUpdate("use " + dbName);
                System.out.printf("create table tb (ts timestamp, c1 int)\n");
                stmt.executeUpdate("create table tb (ts timestamp, c1 int)");
                Thread.sleep(3000);
                stmt.executeUpdate("insert into tb values ('2018-09-17 09:00:00.000', 1)");
                ResultSet resSet = stmt.executeQuery("select * from tb");
                 **/

                // all threads use the same db, but each thread will create tb for itself
                String tb = Thread.currentThread().getName().replaceAll("pool-1-thread-", "tb");
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("use db");
                System.out.printf("create table %s (ts timestamp, c1 int)\n", tb);
                stmt.executeUpdate("create table " + tb + " (ts timestamp, c1 int)");
                stmt.executeUpdate("insert into " + tb + " values ('2018-09-17 09:00:00.000', 1)");
                ResultSet resSet = stmt.executeQuery("select * from " + tb);

                ResultSetMetaData metaData = resSet.getMetaData();
                while (resSet.next()) {
                    StringBuffer strBuff = new StringBuffer();
                    for (int col = 1; col <= metaData.getColumnCount(); col++) {
                        strBuff.append(metaData.getColumnName(col) + "=" + resSet.getObject(col) + " ");
                    }
                    System.out.printf("\t%s; result: %s\n", Thread.currentThread().getName(), strBuff.toString());
                }

                resSet.close();
//                stmt.executeUpdate("drop database " + dbName);
//                stmt.executeUpdate("drop database db");
                stmt.close();
                connection.close();

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
    private boolean testGetConnection(String url) {

        boolean success = true;
        Connection connection;
        try {
            Class.forName(TSDB_DRIVER);
            connection = (Connection) DriverManager.getConnection(url);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("Show databases");
            stmt.close();
            connection.close();
        } catch (Exception e) {
            success = false;
        }
        try {
            System.out.printf("\t%s: Sleep for 5s after connecting...\n", Thread.currentThread().getName());
            Thread.currentThread().sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return success;
    }

    private boolean testGetConnection(String url, Properties info) {

        boolean success = true;
        Connection connection;
        try {
            Class.forName(TSDB_DRIVER);
            connection = (Connection) DriverManager.getConnection(url, info);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("Show databases");
            stmt.close();
            connection.close();
        } catch (Exception e) {
            success = false;
        }

        try {
            System.out.printf("\t%s: Sleep for 5s after connecting...\n", Thread.currentThread().getName());
            Thread.currentThread().sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return success;
    }

    private boolean testGetConnection(String url, String user, String password) {
        boolean success = true;
        Connection connection;
        try {
            Class.forName(TSDB_DRIVER);
            connection = (Connection) DriverManager.getConnection(url, user, password);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("Show databases");
            stmt.close();
            connection.close();
        } catch (Exception e) {
            success = false;
        }
        try {
            System.out.printf("\t%s: Sleep for 5s after connecting...\n", Thread.currentThread().getName());
            Thread.currentThread().sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return success;
    }
}
