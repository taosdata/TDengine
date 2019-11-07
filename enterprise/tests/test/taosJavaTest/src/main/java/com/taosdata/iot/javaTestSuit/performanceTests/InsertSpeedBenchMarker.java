package com.taosdata.iot.javaTestSuit.performanceTests;

import com.taosdata.iot.javaTestSuit.suit_01.TestTask;
import com.taosdata.iot.javaTestSuit.utils.ConnectionFactory;
import com.taosdata.iot.javaTestSuit.utils.SqlGenerator;
import com.taosdata.iot.javaTestSuit.utils.Timer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Jiangyi Hou
 * @since 18-11-15
 */
public class InsertSpeedBenchMarker {

    ConnectionFactory connectionFactory = new ConnectionFactory();
    String db = "InsertBenchMarkerDB";
    String mt = "mt";
    String tbPrefix = "tb";
    String[] cols = new String[] {"ts timestamp", "speed bigint", "str binary(128)"};
    String[] tags = new String[] {"t1 int"};
    int tbNum = 1;
    long rows = 100;
    int threadNum = 1;

    public static void main(String[] args) {

        InsertSpeedBenchMarker benchMarker = new InsertSpeedBenchMarker();

        benchMarker.threadNum = Integer.valueOf(args[0]);
        benchMarker.rows = Long.valueOf(args[1]);

        System.out.printf("Num of threads: %d; rows to insert: %d\n", benchMarker.threadNum, benchMarker.rows);
        benchMarker.createSchema();
        benchMarker.insert();
    }

    private class SingleThreadInsertTask extends TestTask {

        public SingleThreadInsertTask() {
        }


        public void run() {
            System.out.printf("\t%s started SimpleInsertTestTask...\n", Thread.currentThread().getName());
            Connection connection = getConnection(new Properties());
            String threadName = Thread.currentThread().getName().replace("pool-1-thread-", "thread");
            int tbId = Integer.valueOf(threadName.replace("thread",""));
            String tbName = tbPrefix + tbId;
            Timer timer = new Timer();

            Statement stmt;
            StringBuilder sql = new StringBuilder("insert into ");
            try {

                long ts0 = 1430000000000l;
                int inserted = 0; // counter for imported records
                System.out.printf("\t%s: Start to insert %d records...\n", threadName, rows);

                stmt = connection.createStatement();
                stmt.executeUpdate("use " + db);
                stmt.executeUpdate(SqlGenerator.getCreateTableUsingMetricSql(tbName, mt, new String[]{String.valueOf(tbId)}));
                timer.start();
                for (int i = 0; i < rows; i++) {
                    sql.append(tbName).append(" values (").append(ts0 + i).append(", ").append(i).append(", 'dem0')");
                    inserted += stmt.executeUpdate(sql.toString());
                    sql.delete(12, sql.length());
                }
                timer.stop();
                System.out.printf("\t%s: Insert completed!\n", threadName);
                System.out.printf("\t%s: Total records inserted: %d\n", threadName, inserted);
                System.out.printf("\t%s: Total time used: %fs\n", threadName, timer.getTimeInSeconds());
                timer.reset();

                // query table
                System.out.printf("\t%s: Fetch records from table %s\n", threadName, tbName);
                int fetched = 0;
                sql = new StringBuilder("select * from ").append(tbName);
                ResultSet resultSet = stmt.executeQuery(sql.toString());
                while (resultSet.next()) {
                    fetched++;
                }
                resultSet.close();
                System.out.printf("\t%s: Total records fetched: %d\n", threadName, fetched);
                if (fetched != rows) {
                    System.out.printf("%s: Failed: Total rows inserted: %d; Number of rows fetched: %d\n", threadName, inserted, fetched);
                }
                stmt.close();
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("Failed to execute sql: %s\n", sql.toString());
            }
        }
    }

    private void createSchema() {
        System.out.println("Creating schema...");
        Connection connection = this.connectionFactory.getConnection();
        String sql = "";
        try {

            Statement stmt = connection.createStatement();

            // create db
            sql = SqlGenerator.getDropDbSql(db);
            stmt.executeUpdate(sql);
            sql = SqlGenerator.getCreateDbSql(db);
            stmt.executeUpdate(sql);
            sql = "use " + db;
            stmt.executeUpdate(sql);

            // create mt
            sql = SqlGenerator.getCreateMetricSql(mt, cols, tags);
            stmt.executeUpdate(sql);

            stmt.close();
            System.out.println("Schema is created!");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Failed to execute sql: %s\n", sql);
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.printf("Failed to close connection.\n");
            }
        }
    }

    private void insert() {
        ExecutorService executorService = Executors.newFixedThreadPool(this.threadNum);
        for (int i = 0; i < threadNum; i++) {
            executorService.execute(new SingleThreadInsertTask());
        }
        executorService.shutdown();
        while(!executorService.isTerminated()) {
            // wait for termination
        }
    }
}
