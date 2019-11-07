package com.taosdata.iot.javaTestSuit.suit_01;

import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test if the stream functionality works properly in condition of maximum table width.
 * The current size limit of a record in any table is set to be 2048 bytes.
 */
public class StreamTableWidthTester extends TaosTester {

    public static void main(String[] args) {
        int threadNum = 1;
        StreamTableWidthTester tester = new StreamTableWidthTester();
        tester.runTest(threadNum);
    }

    public Boolean runTest(int threadNum) {
        Boolean success = false;

        String db = createDb();
        StreamTestTask task = new StreamTestTask();
        task.setDb(db);
        task.run();
        return success;
    }

    private String createDb() {
        String db = "DB";
        Properties properties = new Properties();
        Connection connection = getConnection(properties);
        String sql = "";
        try {
            Statement stmt = connection.createStatement();
            sql = SqlGenerator.getDropDbSql(db);
            stmt.executeUpdate(sql);
            sql = SqlGenerator.getCreateDbSql(db);
            stmt.executeUpdate(sql);
            stmt.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("%s: Failed to execute sql: %s\n", Thread.currentThread().getName(), sql);
            return null;
        }
        return db;
    }

    private class StreamTestTask extends TestTask {

        private String db;
        private int columns = 255;

        public void setDb(String db) {
            this.db = db;
        }

        @Override
        public void run() {
            insert();
        }

        private void insert() {

            Connection connection = getConnection(new Properties());
            String tb = "tb_" + Thread.currentThread().getName().replaceAll("-","_");
            Statement stmt = null;
            String sql = "";
            int records = 30;
            try {
                stmt = connection.createStatement();
                // select db
                sql = "use " + db;
                stmt.executeUpdate(sql);

                // create table and stream
                // stream table has 1 column of type timestamp and 255 columns of type double, total size 2048 bytes
                StringBuilder createTbSql = new StringBuilder("create table ").append(tb).append(" (ts timestamp, ");
                StringBuilder createStrmSql = new StringBuilder("create table strm as select ");
                for (int i = 0; i < columns - 1; i++) {
                    createTbSql.append("c").append(i).append(" int, ");
                    createStrmSql.append("avg(c").append(i).append("), ");
                }
                createTbSql.append("c").append(columns - 1).append(" int)");
                createStrmSql.append("avg(c").append(columns - 1).append(") from ").append(tb)
                        .append(" interval(10s) sliding(5s)");
                sql = createTbSql.toString();
                stmt.executeUpdate(sql);
                sql = createStrmSql.toString();
                stmt.executeUpdate(sql);
                System.out.println("%s: Stream is created. Sleep for 20s...");
                Thread.currentThread().sleep(20000);

                // insert data into tb
                System.out.printf("%s: start to insert data into %s\n", Thread.currentThread().getName(), tb);
                for (int i = 0; i < records; i++) {
                    sql = "insert into " + tb + " (ts, c0) values (now, " + i + ")";
                    stmt.executeUpdate(sql);
                    System.out.printf("%s: %d records inserted, sleep for 1s\n", Thread.currentThread().getName(), i);
                    Thread.currentThread().sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("%s: Failed to execute sql: %s\n", Thread.currentThread().getName(), sql);
            }
        }
    }
}
