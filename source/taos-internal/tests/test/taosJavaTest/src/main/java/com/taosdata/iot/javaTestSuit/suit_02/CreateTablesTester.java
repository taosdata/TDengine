package com.taosdata.iot.javaTestSuit.suit_02;

import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Jiangyi Hou
 * @since 19-5-7
 */
public class CreateTablesTester {
    private static String db = "db";
    private static String stb = "stb";
    private static String tbPrefix = "tb";
    private static int threadNum = 1;
    private static int tbNum = 100000;
    private static String hostIP = "192.168.0.1";

    public static void main(String[] args) {
        if (args.length > 1) {
            hostIP = args[0];
            threadNum = Integer.valueOf(args[1]);
        } else if (args.length > 0){
            hostIP = args[0];
        }

        CreateTablesTester tester = new CreateTablesTester();
        tester.createDB();
        tester.runTask();

    }

    public void runTask() {
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i = 0; i < threadNum; i++) {
            executorService.execute(new CreateTableTask(i));
        }

        executorService.shutdown();
        while (!executorService.isTerminated()) {

            // wait till all threads complete their tasks

        }
        System.out.println("All thread tasks are completed!");
    }

    class CreateTableTask implements Runnable {

        private int threadId = 1;
        public CreateTableTask() {

        }

        public CreateTableTask(int threadId) {
            this.threadId = threadId;
        }

        public void run() {
            String sql = "use " + db;
            try {
                Connection connection = new ConnectionFactory().getConnection(hostIP, new Properties());
                Statement stmt = connection.createStatement();
                stmt.executeUpdate(sql.toString());
                for (int i = 0; i < tbNum; i++) {
                    if (i % threadNum == threadId) {
                        sql = "create table tb" + i + " using " + stb + " tags(" + i + ")";
                        stmt.executeUpdate(sql);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void createDB() {
        String sql = "create database " + db + " replica 2 cache 2048 ablocks 2.0 tblocks 10 tables 1000";
        try {
            Connection connection = new ConnectionFactory().getConnection(hostIP, new Properties());
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("drop database if exists " + db);
            stmt.executeUpdate(sql);
            stmt.executeUpdate("use " + db);
            sql  = "create table " + stb + " (ts timestamp, c1 int, c2 timestamp) tags(t1 int)";
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failure when executing %s\n", sql);
        }
    }

}
