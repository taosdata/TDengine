package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;
import com.taosdata.iot.javaTestSuit.utils.Timer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * @author Jiangyi Hou
 * @since 18-11-11
 */
public class ConcurrentlyWriteSameTableTester extends TaosTester {

    String db = "db";
    String tb = "tb";
//    long ts0 = System.currentTimeMillis();
    long ts0 = 1541927600000l;
    int threadNum = 100;
//    private static final String TSDBInsert = "insert";
//    private static final String TSDBImport = "import";
//    String writeMethod = TSDBImport;

    public static void main(String[] args) {
//        int threadNum = 100;
        int repeat = 10;
        ConcurrentlyWriteSameTableTester tester =  new ConcurrentlyWriteSameTableTester();
        if (args.length > 0) {
            tester.threadNum = Integer.valueOf(args[0]);
        } else {
            tester.threadNum = 2;
        }
//        tester.writeMethod = args[1];

        System.out.println("======ConcurrentlyWriteSameTableTest======");
        System.out.printf("Number of threads: %d\n", tester.threadNum);
        tester.setupSchema();
        tester.concurrentlyWriteSameTable(tester.threadNum);
        tester.selectCount(repeat);
        tester.selectAll(repeat);
    }

    private class WriteSameTableTask implements Callable<Integer> {
        String db;
        String tb;
        WriteSameTableTask(String db, String tb){
            this.db = db;
            this.tb = tb;
        }

        @Override
        public Integer call() throws Exception {
            Connection connection = getConnection(new Properties());
            Timer timer = new Timer();
            String sql = "use " + this.db;
            int rows = 10000; // number of records to insert
            int inserted = 0;
            String threadName = Thread.currentThread().getName().replaceAll("Thread-", "thread");
            int threadId = Integer.valueOf(threadName.replaceAll("thread", ""));
            try {
                Statement stmt = connection.createStatement();
                stmt.executeUpdate(sql);
                long ts = 0l;
                System.out.printf("\t%s started at %s\n", threadName, ts0);
                for (int i = 0; i < rows; i++) {
                    ts = ts0 + i * threadNum + threadId;
//                    sql = SqlGenerator.getSingleInsertSql(this.tb, new String[]{String.valueOf(ts), threadName});
                    sql = SqlGenerator.getSingleImportSql(this.tb, new String[]{String.valueOf(ts), threadName});
                    timer.start();
                    inserted += stmt.executeUpdate(sql);
                    timer.stop();
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("\t%s: Failed to execute sql: %s\n", threadName, sql);
            }
            System.out.printf("%s: Insert completed! Total inserted: %d! Total time used: %f\n", threadName,
                    inserted, timer.getTimeInSeconds());
            return inserted;
        }
    }

    private void setupSchema() {
        System.out.println("creating database and table...");
        Connection connection = getConnection(new Properties());
        String sql = "drop database if exists " + this.db;
        try {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql);
            sql = SqlGenerator.getCreateDbSql(this.db);
            stmt.executeUpdate(sql);
            stmt.executeUpdate("use " + this.db);
            sql = SqlGenerator.getCreateTableSql1(this.tb, "ts timestamp", "c1 binary(10)");
            stmt.executeUpdate(sql);
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s", sql);
        }
    }

    private void concurrentlyWriteSameTable(int threadNum) {
        System.out.println("writing to tb...");
        List<FutureTask<Integer>> tasks = new ArrayList<>(threadNum);
        for (int i = 0; i < threadNum; i++) {
            WriteSameTableTask task = new WriteSameTableTask(this.db, this.tb);
            tasks.add(new FutureTask<Integer>(task));
            new Thread(tasks.get(i)).start();
        }
        int totalInsert = 0;
        try {
            for (int i = 0; i < threadNum; i++) {
                totalInsert += tasks.get(i).get();
            }
        } catch (InterruptedException interruptedE) {
            interruptedE.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.printf("All threads completed inserting!\nTotal inserted: %d\n", totalInsert);
    }

    private void selectCount(int repeat) {
        Connection connection = getConnection(new Properties());
        Timer timer = new Timer();
        String sql = "use " + this.db;
        long counter = 0l;
        BigDecimal time = new BigDecimal(0);
        try {
            Statement stmt = connection.createStatement();

            // select db
            stmt.executeUpdate(sql);

            // select count(*)
            sql = "select count(*) from " + this.tb;
            timer.start();
            ResultSet resSet = stmt.executeQuery(sql);
            if (resSet.next()) {
                counter = resSet.getLong(1);
            }
            timer.stop();
            resSet.close();
            BigDecimal timeUsed = timer.getTimeInSeconds();
            BigDecimal speed = new BigDecimal(counter).divide(timeUsed, 0, RoundingMode.HALF_UP);
            System.out.printf("Execute sql: select count(*) from tb; counter = %d, time used for query: %fs, reading speed: %d rows/s\n", counter,
                    timeUsed, speed.longValue());

            // reset timer and counter
            timer.reset();
            counter = 0l;

            // repeat the test for multiple times to get average time cost
            for (int i = 0; i < repeat; i++) {
                sql = "select count(*) from " + this.tb;
                timer.start();
                resSet = stmt.executeQuery(sql);
                if (resSet.next()) {
                    counter += resSet.getLong(1);
                }
                timer.stop();
                resSet.close();
            }

            // calculate total rows counted, total time used and average reading speed
            timeUsed = timer.getTimeInSeconds();
            speed = new BigDecimal(counter).divide(timeUsed, 0, RoundingMode.HALF_UP);
            System.out.printf("Repeat for %d times: select count(*) from tb; rows counted = %d, time used for query: %f s, average reading speed: %d rows/s\n", repeat, counter,
                    timeUsed, speed.longValue());
            timer.reset();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        }
    }

    private void selectAll(int repeat) {
        Connection connection = getConnection(new Properties());
        Timer timer = new Timer();
        String sql = "use " + this.db;
        long counter = 0l;
        BigDecimal time = new BigDecimal(0);
        try {
            Statement stmt = connection.createStatement();
            // select db
            stmt.executeUpdate(sql);

            sql = "select * from " + this.tb;
            timer.start();
            ResultSet resSet = stmt.executeQuery(sql);
            ResultSetMetaData metaData = resSet.getMetaData();
            while (resSet.next()) {
//                resSet.getLong(1);
                counter++;
            }
            timer.stop();
            resSet.close();
            BigDecimal timeUsed = timer.getTimeInSeconds();
            BigDecimal speed = new BigDecimal(counter).divide(timeUsed, 0, RoundingMode.HALF_UP);
            System.out.printf("Execute sql: select * from tb; counter = %d, time used for query: %fs, reading speed: %d rows/s\n", counter,
                    timeUsed, speed.longValue());

            // reset timer and counter
            timer.reset();
            counter = 0l;

            // repeat the test for multiple times to get average time
            for (int i = 0; i < repeat; i++) {
                sql = "select * from " + this.tb;
                timer.start();
                resSet = stmt.executeQuery(sql);
                while (resSet.next()) {
//                    resSet.getLong(1);
                    counter++;
                }
                timer.stop();
                resSet.close();
            }
            timeUsed = timer.getTimeInSeconds();
            speed = new BigDecimal(counter).divide(timeUsed, 0, RoundingMode.HALF_UP);
            System.out.printf("Repeat for %d times: select * from tb; rows counted = %d, time used for query: %f s, average reading speed: %d rows/s\n", repeat, counter,
                    timeUsed, speed.longValue());
            timer.reset();
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        }
    }

}
