package com.taosdata.iot.javaTestSuit.suit_02;

import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

/**
 * @author Jiangyi Hou
 * @since 19-4-24
 */
public class SessionsNotReadyTester {

    private static int rows = 10000;
    private static int loop = 10000;

    public static void main(String[] args) {
        if (args.length < 1) {
            // use default values
        } else if (args.length < 2){
            rows = Integer.valueOf(args[0]);
        } else {
            rows = Integer.valueOf(args[0]);
            loop = Integer.valueOf(args[1]);
        }

        SessionsNotReadyTester sessionsNotReadyTester = new SessionsNotReadyTester();
        sessionsNotReadyTester.runTest();

    }

    public void runTest() {
        Thread importThread = new Thread(new ImportTask(rows));
        Thread queryThread = new Thread(new QueryTask(loop));
        Connection connection = new ConnectionFactory().getConnection();
        try {
            // set up db
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists db00");
            stmt.executeUpdate("create database db00");
            stmt.executeUpdate("use db00");
            stmt.executeUpdate("create table tb (ts timestamp, c1 bigint)");
            importThread.start();
            queryThread.start();
            queryThread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        System.out.println("Importing and querying all completed!");
    }

    class ImportTask implements Runnable {
        private int rows = 10000;
        ImportTask(int rows) {
            this.rows = rows;
        }
        public void run() {
            Connection connection = new ConnectionFactory().getConnection();
            try {
                System.out.printf("Import-thread starts to import %d rows into tb\n", rows);
                Statement stmt = connection.createStatement();
                // create db and tables
                stmt.executeUpdate("use db00");
                long ts = 1537146000000L;
                String sql = "import into tb values (";
                for (int i = 0; i < rows; i++) {
                    ts -= 1000;
                    stmt.executeUpdate(sql + ts + ", " + i + ")");
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Failure during importing.");
            }
        }
    }

    class QueryTask implements Runnable {
        private int loop = 10000;
        QueryTask(int loop) {
            this.loop = loop;
        }
        public void run() {
            long sleep = 1000;
            Connection connection = new ConnectionFactory().getConnection();
            try {
                System.out.printf("Query-thread starts to query all data from tb for %d times, sleeping %d ms between two queries\n", loop, sleep);
                Statement stmt = connection.createStatement();
                stmt.executeUpdate("use db00");
                for (int i = 0; i < this.loop; i++) {
                    ResultSet resultSet = stmt.executeQuery("select * from tb");
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    resultSet.close();
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Failure during querying");
            }
        }
    }
}
